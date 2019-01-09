use crate::config::Config;
use crate::key_value_state_machine::KeyValueStateMachine;
use crate::network::{start_rpc_server, NetworkActor, ServerStarted};
use crate::node_router::NodeRouter;
use crate::proto::{RaftGroupMetaState, RaftGroupType};
use crate::raft::RaftClient;
use crate::raft_router::RaftRouter;
use crate::raft_storage::{
    init_raft_group, RaftStorage, LOCAL_PREFIX, RAFT_GROUP_META_PREFIX, RAFT_GROUP_META_PREFIX_KEY,
};
use crate::search::IndexCoordinator;
use crate::search_state_machine::SearchStateMachine;
use crate::storage_engine::StorageEngine;
use crate::web::start_web;
use actix::prelude::*;
use actix::SystemRunner;
use failure::Error;
use futures::{future, sync::oneshot};
use protobuf::parse_from_bytes;
use std::path::Path;

pub struct System {
    shutdown_signal: oneshot::Sender<()>,
}

pub fn run(config: &Config) -> Result<System, Error> {
    let config = config.clone();
    let (sender, receiver) = oneshot::channel();
    tokio::run(future::lazy(move || build_system(&config).map_err(|_| ())));
    Ok(System {
        shutdown_signal: sender,
    })
}

fn build_system(config: &Config) -> Result<(), Error> {
    let storage_root = Path::new(&config.storage_root);
    let storage_engine = StorageEngine::new(&storage_root.join("cluster"))?;
    init_node(&config.master_ids, &storage_engine)?;

    let kv_raft_router = RaftRouter::<KeyValueStateMachine>::new(config.node_id);
    let search_raft_router = RaftRouter::<SearchStateMachine>::new(config.node_id);
    let network = NetworkActor::new(config.node_id).start();
    let server = start_rpc_server(
        network.clone(),
        &kv_raft_router,
        &search_raft_router,
        config.node_id,
        config.port,
    )?;
    network.try_send(ServerStarted { server });

    let node_router = NodeRouter::start(&config)?;
    let group_states = get_raft_groups(&storage_engine)?;
    let index_coordinator = IndexCoordinator::new(
        &config,
        node_router.clone(),
        storage_engine.clone(),
        &group_states,
        &network,
        &search_raft_router,
    )?
    .start();
    let group_state = group_states[0].clone();
    let storage = RaftStorage::new(group_state, storage_engine)?;
    let kv_engine = StorageEngine::new(&storage_root.join("kv"))?;
    let kv_state_machine = KeyValueStateMachine::new(kv_engine)?;
    let raft = RaftClient::new(
        config.node_id,
        storage,
        kv_state_machine,
        node_router.clone(),
        &network,
        &kv_raft_router,
    )?
    .start();
    start_web(config, node_router);
    Ok(())
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
        std::env::set_var("RUST_LOG", "searchify=info,actix_web=info,raft=debug");
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
