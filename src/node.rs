use crate::action_executor::ActionExecutor;
use crate::clock::Clock;
use crate::cluster_state::{ClusterState, ClusterStateUpdater};
use crate::config::Config;
use crate::gossip::GossipServer;
use crate::index_coordinator::IndexCoordinator;
use crate::key_value_state_machine::{KeyValueStateMachine, MetaStateEvent};
use crate::network::{start_rpc_server, InternalServer};
use crate::node_router::{NodeRouter, NodeRouterHandle};
use crate::proto::{RaftGroupMetaState, RaftGroupType};
use crate::raft::{RaftClient, RaftEvent};
use crate::raft_router::RaftRouter;
use crate::raft_storage::{
    init_raft_group, RaftStorage, LOCAL_PREFIX, RAFT_GROUP_META_PREFIX, RAFT_GROUP_META_PREFIX_KEY,
};
use crate::storage_engine::StorageEngine;
use crate::web::start_web;
use failure::Error;
use futures::{future, prelude::*, sync::oneshot};
use grpcio::Server;
use log::*;
use protobuf::parse_from_bytes;
use std::path::Path;
use tokio_signal::unix::{Signal, SIGINT, SIGTERM};

struct Inner {
    _server: Server,
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
    init_node(config.node_id, &config.master_ids, &storage_engine)?;

    let cluster_state = ClusterState::new(config.node_id);
    let clock = Clock::new();
    let kv_raft_router = RaftRouter::new();
    let search_raft_router = RaftRouter::new();

    let gossip_server = GossipServer::new(
        config.node_id,
        &config.seeds,
        &format!("{}:{}", config.advertised_host, config.port),
        clock.clone(),
    );
    let node_router = NodeRouter::start(&config, gossip_server.state(), cluster_state.clone())?;
    let group_states = get_raft_groups(&storage_engine)?;
    let index_coordinator = IndexCoordinator::start(
        &config,
        cluster_state.clone(),
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
    let action_executor = ActionExecutor::new(node_router.clone());
    start_web(config, action_executor.clone())?;
    start_cluster_service(
        &config,
        &group_states,
        &storage_root,
        &storage_engine,
        gossip_server,
        &node_router,
        &kv_raft_router,
        &clock,
        &cluster_state,
    )?;
    Ok(Inner {
        _server: server,
        _index_coordinator: index_coordinator,
    })
}

fn init_node(peer_id: u64, master_ids: &[u64], engine: &StorageEngine) -> Result<(), Error> {
    init_raft_group(
        engine,
        0,
        peer_id,
        master_ids,
        RaftGroupType::RAFT_GROUP_META,
    )
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

fn start_cluster_service(
    config: &Config,
    group_states: &[RaftGroupMetaState],
    storage_root: &Path,
    storage_engine: &StorageEngine,
    gossip_server: GossipServer,
    node_router: &NodeRouterHandle,
    kv_raft_router: &RaftRouter<KeyValueStateMachine>,
    clock: &Clock,
    cluster_state: &ClusterState,
) -> Result<(), Error> {
    let group_state = group_states[0].clone();
    let storage = RaftStorage::new(group_state, storage_engine.clone())?;
    let kv_engine = StorageEngine::new(&storage_root.join("kv"))?;
    let mut kv_state_machine = KeyValueStateMachine::new(kv_engine, clock.clone())?;
    let liveness_gossip = gossip_server.clone();

    let liveness_update_task = kv_state_machine
        .subscribe()
        .filter_map(|event| match event {
            MetaStateEvent::PeerUpdate(peer) => Some(peer),
            _ => None,
        })
        .for_each(move |peer| liveness_gossip.update_node_liveness(peer));
    tokio::spawn(liveness_update_task);

    let cluster_state_updater = ClusterStateUpdater::new(cluster_state.clone());
    let cluster_state_update_task = kv_state_machine
        .subscribe()
        .for_each(move |event| Ok(cluster_state_updater.handle_event(event)));
    tokio::spawn(cluster_state_update_task);
    cluster_state.initialize(&kv_state_machine);

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
