use actix::prelude::*;
use crate::cluster::Cluster;
use crate::config::Config;
use crate::web::start_web;
use crate::network::NetworkActor;
use crate::raft::{InitNetwork, RaftClient};
use crate::raft_group::RaftGroup;
use crate::raft_storage::RaftStorage;
use crate::storage_engine::StorageEngine;
use failure::Error;

pub fn run(config: &Config) -> Result<(), Error> {
    let sys = System::new("searchify");
    let storage_engine = StorageEngine::new(&config.storage_root)?;
    let raft_group = RaftGroup {
        id: 1,
        peers: vec![],
    };
    let storage = RaftStorage::new(raft_group, storage_engine)?;
    let raft = RaftClient::new(config.node_id, storage)?.start();
    let network = NetworkActor::start(config.node_id, config.port, &config.seeds, raft.clone())?;
    raft.try_send(InitNetwork(network))?;
    let _cluster = Cluster::new(raft);
    start_web();
    let _ = sys.run();
    Ok(())
}
