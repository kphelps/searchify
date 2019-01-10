use crate::config::Config;
use crate::mappings::Mappings;
use crate::proto::*;
use crate::rpc_client::RpcClient;
use failure::{err_msg, format_err, Error};
use futures::{future, prelude::*};
use log::*;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
};
use std::time::Duration;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
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
        clients.insert(
            config.node_id,
            RpcClient::new(config.node_id, &self_address),
        );
        Self {
            node_id: config.node_id,
            peers: Arc::new(RwLock::new(clients)),
            leader_id: AtomicUsize::new(0),
        }
    }

    pub fn start(config: &Config) -> Result<NodeRouterHandle, Error> {
        let router = NodeRouter::new(config);
        let handle = NodeRouterHandle {
            handle: Arc::new(router),
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
        tokio::spawn(heartbeat_task);

        config.seeds.iter().for_each(|seed| {
            let peer_task = handle.connect_to_peer(seed).map_err(|_| ());
            tokio::spawn(peer_task);
        });

        Ok(handle)
    }

    // TODO: Retry indefinitely
    pub fn connect_to_peer(&self, peer_address: &str) -> impl Future<Item = (), Error = Error> {
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
    ) -> impl Future<Item = (), Error = Error> {
        debug!(
            "[group-{}] Routing message to {}",
            raft_group_id, message.to
        );
        future::result(self.peer(message.to))
            .and_then(move |peer| peer.raft_message(&message, raft_group_id))
    }

    pub fn create_index(
        &self,
        name: String,
        shard_count: u64,
        replica_count: u64,
        mappings: Mappings,
    ) -> impl Future<Item = (), Error = Error> {
        self.with_leader_client(move |client| {
            client.create_index(&name, shard_count, replica_count, mappings)
        })
    }

    pub fn delete_index(&self, name: String) -> impl Future<Item = (), Error = Error> {
        self.with_leader_client(move |client| client.delete_index(&name))
    }

    pub fn get_index(&self, name: String) -> impl Future<Item = IndexState, Error = Error> {
        self.with_leader_client(move |client| client.get_index(&name))
    }

    pub fn list_indices(&self) -> impl Future<Item = ListIndicesResponse, Error = Error> {
        self.with_leader_client(move |client| client.list_indices())
    }

    pub fn list_shards(&self, node_id: u64) -> impl Future<Item = Vec<ShardState>, Error = Error> {
        self.with_leader_client(move |client| client.list_shards(node_id))
    }

    pub fn send_heartbeat(&self) -> impl Future<Item = (), Error = Error> {
        self.with_leader_client(|client| client.heartbeat())
    }

    pub fn set_leader_id(&self, id: u64) {
        self.leader_id.store(id as usize, Ordering::Relaxed);
    }

    pub fn index_document(
        &self,
        index_name: String,
        document_id: u64,
        payload: serde_json::Value,
    ) -> impl Future<Item = (), Error = Error> {
        // TODO: should get handle, not clone
        let peers = self.peers.clone();
        self.get_shard_for_document(&index_name, document_id)
            .and_then(move |shard| {
                let replica_id = shard.replicas.first().unwrap().id;
                let client = peers.read().unwrap().get(&replica_id).cloned().unwrap();
                client.index_document(&index_name, shard.id, payload)
            })
    }

    pub fn search(
        &self,
        index_name: String,
        query: Vec<u8>,
    ) -> impl Future<Item = (), Error = Error> {
        let peers = self.peers.clone();
        self.get_index(index_name.to_string())
            .and_then(move |index| {
                let futures = index.shards.into_iter().map(move |shard| {
                    let replica_id = shard.replicas.first().unwrap().id;
                    let client = peers.read().unwrap().get(&replica_id).cloned().unwrap();
                    // lift the future error up into the response so we can join all
                    client
                        .search(&index_name, shard.id, query.clone())
                        .then(future::ok)
                });
                future::join_all(futures).map(|results| {
                    info!("Search response: {:?}", results);
                })
            })
    }

    fn get_shard_for_document(
        &self,
        index_name: &str,
        document_id: u64,
    ) -> impl Future<Item = ShardState, Error = Error> {
        // TODO: obviously need a shard routing algorithm
        self.get_index(index_name.to_string()).map(move |index| {
            index
                .shards
                .into_iter()
                .find(|shard| {
                    let low_id = shard.get_range().low;
                    let high_id = shard.get_range().high;
                    low_id <= document_id && document_id <= high_id
                })
                .expect("Invalid range")
        })
    }

    fn peer(&self, id: u64) -> Result<RpcClient, Error> {
        let peers = self.peers.read().unwrap();
        peers
            .get(&id)
            .cloned()
            .ok_or_else(|| format_err!("peer '{}' not found", id))
    }

    fn leader_id(&self) -> u64 {
        self.leader_id.load(Ordering::Relaxed) as u64
    }

    fn leader_client(&self) -> Result<RpcClient, Error> {
        let id = self.leader_id();
        self.peer(id).map_err(|_| err_msg("no leader available"))
    }

    fn with_leader_client<F, X, R>(&self, f: F) -> impl Future<Item = R, Error = Error>
    where
        F: FnOnce(RpcClient) -> X,
        X: Future<Item = R, Error = Error>,
    {
        future::result(self.leader_client()).and_then(f)
    }
}

impl std::ops::Deref for NodeRouterHandle {
    type Target = NodeRouter;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
