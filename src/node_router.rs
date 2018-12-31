use actix::Arbiter;
use crate::config::Config;
use crate::rpc_client::{RpcClient, RpcFuture};
use futures::{prelude::*, future};
use failure::{Error, err_msg, format_err};
use log::*;
use std::collections::HashMap;
use std::sync::{
    Arc,
    RwLock,
    atomic::{
        AtomicUsize,
        Ordering
    },
};
use std::time::Duration;
use tokio_retry::{
    Retry,
    strategy::{
        ExponentialBackoff,
        jitter
    }
};
use tokio_timer::Interval;

pub struct NodeRouter {
    node_id: u64,
    peers: Arc<RwLock<HashMap<u64, RpcClient>>>,
    leader_id: AtomicUsize,
}

#[derive(Clone)]
pub struct NodeRouterHandle {
    handle: Arc<NodeRouter>,
}

impl NodeRouter {
    fn new(config: &Config) -> Self {
        let mut clients = HashMap::new();
        let self_address = format!("127.0.0.1:{}", config.port);
        clients.insert(config.node_id, RpcClient::new(config.node_id, &self_address));
        Self {
            node_id: config.node_id,
            peers: Arc::new(RwLock::new(clients)),
            leader_id: AtomicUsize::new(0),
        }
    }

    pub fn start(config: &Config) -> Result<NodeRouterHandle, Error> {
        let router = NodeRouter::new(config);
        let handle = NodeRouterHandle{
            handle: Arc::new(router)
        };

        let heartbeat_handle = handle.clone();
        let heartbeat_task = Interval::new_interval(Duration::from_secs(5))
            .from_err::<Error>()
            .for_each(move |_| {
                heartbeat_handle.send_heartbeat().or_else(|err| {
                    warn!("Error sending heartbeat: {:?}", err);
                    Ok(())
                })
            })
            .map_err(|_| error!("Heartbeat task failed"));
        Arbiter::spawn(heartbeat_task);

        config.seeds.iter().for_each(|seed| {
            let peer_task = handle.connect_to_peer(seed).map_err(|_| ());
            Arbiter::spawn(peer_task);
        });

        Ok(handle)
    }

    // TODO: Retry indefinitely
    pub fn connect_to_peer(&self, peer_address: &str) -> impl Future<Item=(), Error=Error> {
        let peers = self.peers.clone();
        let client = RpcClient::new(self.node_id, peer_address);
        let f = move || {
            let peers = peers.clone();
            let client = client.clone();
            client.hello().map(move |response| {
                peers.write().unwrap().insert(response.peer_id, client);
                info!("Connected to peer id '{}'", response.peer_id);
            })
        };
        let retry_strategy = ExponentialBackoff::from_millis(300)
            .max_delay(Duration::from_secs(10))
            .map(jitter);
        Retry::spawn(retry_strategy, f)
            .map(|_| ())
            .map_err(|err| format_err!("Connect retry failed: {:?}", err))
    }

    pub fn route_raft_message(
        &self,
        message: raft::eraftpb::Message,
        raft_group_id: u64,
    ) -> impl RpcFuture<()> {
        let peers = self.peers.read().unwrap();
        future::result(peers.get(&message.to).cloned().ok_or(err_msg("peer not found")))
            .and_then(move |peer| peer.raft_message(&message, raft_group_id))
    }

    pub fn create_index(&self, name: String) -> impl RpcFuture<()> {
        future::result(self.leader_client())
            .and_then(move |client| client.create_index(&name))
    }

    pub fn send_heartbeat(&self) -> impl RpcFuture<()> {
        future::result(self.leader_client())
            .and_then(|client| client.heartbeat())
    }

    pub fn set_leader_id(&self, id: u64) {
        self.leader_id.store(id as usize, Ordering::Relaxed);
    }

    fn leader_id(&self) -> u64 {
        self.leader_id.load(Ordering::Relaxed) as u64
    }

    fn leader_client(&self) -> Result<RpcClient, Error> {
        let id = self.leader_id();
        let peers = self.peers.read().unwrap();
        peers.get(&id).cloned().ok_or(err_msg("no leader available"))
    }
}

impl std::ops::Deref for NodeRouterHandle {
    type Target = NodeRouter;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
