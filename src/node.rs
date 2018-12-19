use actix::prelude::*;
use crate::cluster::Cluster;
use crate::web::start_web;
use crate::network::NetworkActor;
use crate::raft::{InitNetwork, RaftClient};
use failure::Error;

pub fn run(seed_id: u64, port: u16, seeds: &[String]) -> Result<(), Error> {
    let sys = System::new("searchify");
    let raft = RaftClient::new(seed_id)?.start();
    let network = NetworkActor::start(seed_id, port, seeds, raft.clone())?;
    raft.try_send(InitNetwork(network))?;
    let _cluster = Cluster::new(raft);
    start_web();
    let _ = sys.run();
    Ok(())
}
