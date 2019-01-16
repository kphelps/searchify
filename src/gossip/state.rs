use super::proto::GossipData;
use super::server::GossipRpcClient;
use std::collections::HashMap;
use futures::sync::mpsc;

pub struct GossipState {
    pub bootstrap: Vec<String>,
    pub current: GossipData,
    pub connections: HashMap<String, mpsc::Sender<()>>,
    pub peers: HashMap<u64, GossipData>,
}

impl GossipState {
    pub fn new(
        node_id: u64,
        bootstrap: &[String],
    ) -> Self {
        Self {
            bootstrap: bootstrap.to_vec(),
            current: GossipData::new(node_id),
            connections: HashMap::new(),
            peers: HashMap::new(),
        }
    }

    pub fn remove_connection(&mut self, addr: &str) {
        self.connections.remove(addr);
    }

    pub fn add_connection(&mut self, addr: &str, sender: mpsc::Sender<()>) {
        self.connections.insert(addr.to_string(), sender);
    }
}
