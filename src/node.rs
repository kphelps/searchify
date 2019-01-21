use crate::clock::Clock;
use crate::config::Config;
use crate::gossip::GossipServer;
use crate::key_value_state_machine::{KeyValueStateMachine, MetaStateEvent};
use crate::network::{start_rpc_server, InternalServer};
use crate::node_router::{NodeRouter, NodeRouterHandle};
use crate::proto::{RaftGroupMetaState, RaftGroupType};
use crate::raft::{RaftClient, RaftEvent};
use crate::raft_router::RaftRouter;
use crate::raft_storage::{
    init_raft_group, RaftStorage, LOCAL_PREFIX, RAFT_GROUP_META_PREFIX, RAFT_GROUP_META_PREFIX_KEY,
};
use crate::search::IndexCoordinator;
use crate::storage_engine::StorageEngine;
use crate::web::{start_web, HttpServer};
use failure::Error;
use futures::{future, prelude::*, sync::oneshot};
use grpcio::Server;
use log::*;
use protobuf::parse_from_bytes;
use std::path::Path;
use tokio_signal::unix::{Signal, SIGINT, SIGTERM};

struct Inner {
    _server: Server,
    _http_server: HttpServer,
    _index_coordinator: IndexCoordinator,
}

pub fn run(config: &Config) -> Result<(), Error> {
    let config = config.clone();
    let (sender, receiver) = oneshot::channel();
    let mut rt = tokio::runtime::Runtime::new()?;

    let signal_handler = Signal::new(SIGINT)
        .flatten_stream()
        .select(Signal::new(SIGTERM).flatten_stream())
        .into_future()
        .map_err(|_| ())
        .and_then(move |_| sender.send(()))
        .map_err(|_| ());

    rt.spawn(signal_handler);
    rt.block_on_all(future::lazy(move || run_system(&config, receiver)))
}

fn run_system(
    config: &Config,
    shutdown_signal: oneshot::Receiver<()>,
) -> impl Future<Item = (), Error = Error> {
    future::result(build_system(config))
        .and_then(move |inner| shutdown_signal.map(|_| inner).from_err())
        .map(|_| info!("System shutdown."))
}

fn build_system(config: &Config) -> Result<Inner, Error> {
    let storage_root = Path::new(&config.storage_root);
    let storage_engine = StorageEngine::new(&storage_root.join("cluster"))?;
    init_node(&config.master_ids, &storage_engine)?;

    let clock = Clock::new();
    let kv_raft_router = RaftRouter::new();
    let search_raft_router = RaftRouter::new();

    let gossip_server = GossipServer::new(
        config.node_id,
        &config.seeds,
        &format!("{}:{}", config.advertised_host, config.port),
        clock.clone(),
    );
    let node_router = NodeRouter::start(&config, gossip_server.state())?;
    let group_states = get_raft_groups(&storage_engine)?;
    let index_coordinator = IndexCoordinator::start(
        &config,
        node_router.clone(),
        storage_engine.clone(),
        &group_states,
        &search_raft_router,
    )?;
    let internal_service = InternalServer::build_service(
        config.node_id,
        clock.clone(),
        &kv_raft_router,
        &search_raft_router,
    );
    let server = start_rpc_server(
        vec![internal_service, gossip_server.build_service()],
        config.node_id,
        config.port,
    )?;
    let http_server = start_web(config, node_router.clone())?;
    start_master_process(
        &config,
        &group_states,
        &storage_root,
        &storage_engine,
        gossip_server,
        &node_router,
        &kv_raft_router,
        &clock,
    )?;
    Ok(Inner {
        _server: server,
        _http_server: http_server,
        _index_coordinator: index_coordinator,
    })
}

fn init_node(master_ids: &[u64], engine: &StorageEngine) -> Result<(), Error> {
    init_raft_group(engine, 0, master_ids, RaftGroupType::RAFT_GROUP_META)
}

fn get_raft_groups(engine: &StorageEngine) -> Result<Vec<RaftGroupMetaState>, Error> {
    let end_key: Vec<u8> = vec![LOCAL_PREFIX, RAFT_GROUP_META_PREFIX + 1];
    let mut out = Vec::new();
    engine.scan(RAFT_GROUP_META_PREFIX_KEY, end_key, |_, value| {
        out.push(parse_from_bytes(value)?);
        Ok(true)
    })?;

    Ok(out)
}

fn start_master_process(
    config: &Config,
    group_states: &[RaftGroupMetaState],
    storage_root: &Path,
    storage_engine: &StorageEngine,
    gossip_server: GossipServer,
    node_router: &NodeRouterHandle,
    kv_raft_router: &RaftRouter<KeyValueStateMachine>,
    clock: &Clock,
) -> Result<(), Error> {
    if !config.master_ids.contains(&config.node_id) {
        return Ok(());
    }

    let group_state = group_states[0].clone();
    let storage = RaftStorage::new(group_state, storage_engine.clone())?;
    let kv_engine = StorageEngine::new(&storage_root.join("kv"))?;
    let mut kv_state_machine = KeyValueStateMachine::new(kv_engine, clock.clone())?;
    let liveness_gossip = gossip_server.clone();
    let liveness_update_task = kv_state_machine
        .subscribe()
        .filter_map(|event| match event {
            MetaStateEvent::PeerUpdate(peer) => Some(peer),
        })
        .for_each(move |peer| liveness_gossip.update_node_liveness(peer));
    tokio::spawn(liveness_update_task);
    let meta_raft = RaftClient::new(
        config.node_id,
        storage,
        kv_state_machine,
        node_router.clone(),
        &kv_raft_router,
        None,
        None,
    )?;
    let leader_update_task = meta_raft
        .subscribe()
        .filter_map(|event| match event {
            RaftEvent::LeaderChanged(id) => Some(id),
        })
        .for_each(move |leader_id| gossip_server.update_meta_leader(leader_id));
    tokio::spawn(leader_update_task);
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::mappings::Mappings;
    use crate::rpc_client::RpcClient;
    use futures::prelude::*;
    use log::error;
    use rand::{thread_rng, Rng};
    use tempfile;

    fn config_for_node(node_id: u64) -> Config {
        std::env::set_var("RUST_LOG", "searchify=info,raft=debug");
        let _ = env_logger::try_init();
        let mut config = Config::default().unwrap();
        let dir = tempfile::tempdir().unwrap();
        config.node_id = node_id;
        config.storage_root = dir.path().to_str().unwrap().to_string();
        config.master_ids = vec![config.node_id];
        config.port = thread_rng().gen_range(10000, 65534);
        config.web.port = config.port + 1;
        config
    }

    fn config() -> Config {
        config_for_node(1)
    }

    fn run_in_system<F, I, E>(system: &mut SystemRunner, f: F) -> Result<I, Error>
    where
        F: IntoFuture<Item = I, Error = E>,
        E: Into<Error>,
    {
        system.block_on(f.into_future()).map_err(|e| {
            let err = e.into();
            error!("Error in execution: {:?}", err);
            err
        })
    }

    fn run_node<F, I, E>(config: &Config, f: F) -> Result<I, Error>
    where
        F: IntoFuture<Item = I, Error = E>,
        E: Into<Error>,
    {
        let mut system = build_system(config)?;
        run_in_system(&mut system, f)
    }

    fn run_node_fn<F, N, I, E>(config: &Config, f: F) -> Result<I, Error>
    where
        F: FnOnce() -> N,
        N: IntoFuture<Item = I, Error = E>,
        E: Into<Error>,
    {
        run_node(config, f())
    }

    fn rpc_client(config: &Config) -> RpcClient {
        let address = format!("127.0.0.1:{}", config.port);
        RpcClient::new(config.node_id, &address)
    }

    #[test]
    fn test_set_then_get() {
        let config = config();
        let mut system = build_system(&config).unwrap();
        let client = rpc_client(&config);
        let key = vec![0];
        let value = vec![0];
        let f = client.set(&key, &value);
        let _ = run_in_system(&mut system, f).unwrap();

        let f = client.get(&key);
        let got = run_in_system(&mut system, f).unwrap();
        assert_eq!(got.key, key);
        assert_eq!(got.value, value);
    }

    #[test]
    fn test_set_restart_get() {
        let mut config = config();
        let client = rpc_client(&config);
        let key = vec![0];
        let value = vec![0];
        let f = client.set(&key, &value);
        let _ = run_node(&config, f).unwrap();
        config.web.port += 1; // TODO :sigh:

        let f = client.get(&key);
        let got = run_node(&config, f).unwrap();
        assert_eq!(got.key, key);
        assert_eq!(got.value, value);
    }

    #[test]
    fn test_create_index_with_live_nodes() {
        let config = config();
        let mut system = build_system(&config).unwrap();
        let client = rpc_client(&config);
        let _ = run_in_system(&mut system, client.heartbeat());
        let index_name = "hello-world";
        let f = client.create_index(index_name, 2, 3, Mappings::default());
        let _ = run_in_system(&mut system, f).unwrap();

        let f = client.get_index(index_name);
        let response = run_in_system(&mut system, f).unwrap();
        assert_eq!(response.shard_count, 2);
        assert_eq!(response.replica_count, 3);

        let f = client.list_shards(config.node_id);
        let shards = run_in_system(&mut system, f).unwrap();
        // No live nodes
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn test_create_index_without_live_nodes() {
        let config = config();
        let mut system = build_system(&config).unwrap();
        let client = rpc_client(&config);
        let index_name = "hello-world";
        let f = client.create_index(index_name, 2, 3, Mappings::default());
        let _ = run_in_system(&mut system, f).unwrap();

        let f = client.get_index(index_name);
        let response = run_in_system(&mut system, f).unwrap();
        assert_eq!(response.shard_count, 2);
        assert_eq!(response.replica_count, 3);

        let f = client.list_shards(config.node_id);
        let shards = run_in_system(&mut system, f).unwrap();
        // No live nodes
        assert_eq!(shards.len(), 0);
    }

    #[test]
    fn test_node_liveness() {
        let config = config();
        let mut system = build_system(&config).unwrap();
        let client = rpc_client(&config);
        let f = client.list_nodes();
        let nodes = run_in_system(&mut system, f).unwrap();
        assert_eq!(nodes.len(), 0);
        let f = client.heartbeat().and_then(|_| client.list_nodes());
        let nodes = run_in_system(&mut system, f).unwrap();
        assert_eq!(nodes.len(), 1);
    }
}
