use crate::proto::gossip::*;
use crate::proto::gossip_grpc::*;
use failure::{format_err, Error};
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use grpcio::{
    EnvBuilder, RpcContext, RpcStatus, RpcStatusCode, Server, ServerBuilder, Service, UnarySink,
};
use log::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

#[derive(Clone)]
pub struct GossipServer {
    state: Arc<RwLock<GossipState>>,
}

impl Gossip for GossipServer {
    fn exchange(&mut self, ctx: RpcContext, req: GossipData, sink: UnarySink<GossipData>) {
        let out = self.state.read().unwrap().current.clone();
        ctx.spawn(
            sink.success(out)
                .map_err(|err| error!("Error exhanging gossip: {:?}", err)),
        );
    }
}

impl GossipServer {
    pub fn build_service(node_id: u64, bootstrap: &[String]) -> Service {
        let state = Arc::new(RwLock::new(GossipState::new(node_id, bootstrap)));
        let server = GossipServer { state };
        create_gossip(server)
    }
}

struct GossipState {
    bootstrap: Vec<String>,
    current: GossipData,
    connections: HashMap<String, mpsc::Sender<()>>,
    peers: HashMap<u64, GossipData>,
}

impl GossipState {
    fn new(node_id: u64, bootstrap: &[String]) -> Self {
        let mut current = GossipData::new();
        current.set_node_id(node_id);
        Self {
            current,
            bootstrap: bootstrap.to_vec(),
            connections: HashMap::new(),
            peers: HashMap::new(),
        }
    }

    fn remove_connection(&mut self, addr: &str) {
        self.connections.remove(addr);
    }

    fn add_connection(&mut self, addr: &str, sender: mpsc::Sender<()>) {
        self.connections.insert(addr.to_string(), sender);
    }
}
