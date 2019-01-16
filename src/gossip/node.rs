use super::state::GossipState;
use super::server::{GossipServer, GossipServerHandle};
use failure::Error;
use std::sync::{Arc, RwLock};

pub struct GossipNode {
    state: Arc<RwLock<GossipState>>,
    server_handle: GossipServerHandle,
}

impl GossipNode {
    pub fn new(
        node_id: u64,
        listen_host: &str,
        listen_port: u16,
        bootstrap: &[String],
    ) -> Result<Self, Error> {
        let state = Arc::new(RwLock::new(GossipState::new(node_id, bootstrap)));
        let listen_address = format!("{}:{}", listen_host, listen_port);
        let server_handle = GossipServer::run(&listen_address, state.clone())?;
        Ok(GossipNode { state, server_handle })
    }
}

