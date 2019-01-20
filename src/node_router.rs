use crate::config::Config;
use crate::gossip::GossipState;
use crate::mappings::Mappings;
use crate::proto::*;
use crate::rpc_client::RpcClient;
use failure::{err_msg, Error};
use futures::{future, prelude::*, sync::oneshot};
use log::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio_timer::Interval;

pub struct NodeRouter {
    gossip_state: GossipState,
    tasks: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
    index_cache: Arc<RwLock<HashMap<String, IndexState>>>,
}

#[derive(Clone)]
pub struct NodeRouterHandle {
    handle: Arc<NodeRouter>,
}

impl NodeRouter {
    fn new(
        config: &Config,
        gossip_state: GossipState,
    ) -> Self {
        let mut clients = HashMap::new();
        let self_address = format!("127.0.0.1:{}", config.port);
        clients.insert(
            config.node_id,
            RpcClient::new(config.node_id, &self_address),
        );
        Self {
            gossip_state,
            tasks: Arc::new(Mutex::new(Vec::new())),
            index_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn start(
        config: &Config,
        gossip_state: GossipState,
    ) -> Result<NodeRouterHandle, Error> {
        let router = NodeRouter::new(config, gossip_state);
        let handle = NodeRouterHandle {
            handle: Arc::new(router),
        };

        let heartbeat_handle = Arc::downgrade(&handle.handle);
        let heartbeat_task = Interval::new_interval(Duration::from_secs(5))
            .map_err(|_| ())
            .and_then(move |_| heartbeat_handle.upgrade().ok_or(()))
            .for_each(move |handle| {
                handle.send_heartbeat()
                    .map_err(|err| warn!("Error sending heartbeat: {:?}", err))
                    .then(|_| Ok(()))
            });
        let t = heartbeat_task.select(handle.handle.task().map_err(|_| ()));
        tokio::spawn(t.then(|_| Ok(())));

        Ok(handle)
    }

    fn task(&self) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        self.tasks.lock().unwrap().push(sender);
        receiver
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

    pub fn index_document(
        &self,
        index_name: String,
        document_id: u64,
        payload: serde_json::Value,
    ) -> impl Future<Item = (), Error = Error> {
        let resolver = self.gossip_state.clone();
        self.get_shard_for_document(&index_name, document_id)
            .and_then(move |shard| {
                let replica_id = shard.replicas.first().unwrap().id;
                resolver.get_client(replica_id)
                    .into_future()
                    .and_then(move |client| client.index_document(&index_name, shard.id, payload))
            })
    }

    pub fn search(
        &self,
        index_name: String,
        query: Vec<u8>,
    ) -> impl Future<Item = (), Error = Error> {
        let resolver = self.gossip_state.clone();
        self.get_index(index_name.to_string())
            .and_then(move |index| {
                let futures = index.shards.into_iter().map(move |shard| {
                    let query = query.clone();
                    let index_name = index_name.clone();
                    let replica_id = shard.replicas.first().unwrap().id;
                    // lift the future error up into the response so we can join all
                    resolver.get_client(replica_id)
                        .into_future()
                        .and_then(move |client| client.search(&index_name, shard.id, query.clone()))
                        .then(future::ok)
                });
                future::join_all(futures).map(|results| {
                    info!("Search response: {:?}", results);
                })
            })
    }

    pub fn refresh_index(&self, index_name: &str) -> impl Future<Item = (), Error = Error> {
        let resolver = self.gossip_state.clone();
        self.get_cached_index(index_name)
            .and_then(move |mut index| {
                let shards = index.take_shards().into_iter().map(move |shard| {
                    let replica_id = shard.replicas.first().unwrap().id;
                    resolver.get_client(replica_id)
                        .into_future()
                        .and_then(move |client| client.refresh_shard(shard.id))
                });
                future::join_all(shards)
            })
            .map(|_| ())
    }

    pub fn refresh_shard(&self, shard_id: u64) -> impl Future<Item = (), Error = Error> {
        let resolver = self.gossip_state.clone();
        self.get_shard_by_id(shard_id).and_then(move |shard| {
            let replica_id = shard.replicas.first().unwrap().id;
            resolver.get_client(replica_id)
                .into_future()
                .and_then(move |client| client.refresh_shard(shard.id))
        })
    }

    fn get_shard_by_id(&self, shard_id: u64) -> impl Future<Item = ShardState, Error = Error> {
        self.list_indices().and_then(move |response| {
            response
                .get_indices()
                .into_iter()
                .map(|index| index.get_shards())
                .flatten()
                .find(|shard| shard.get_id() == shard_id)
                .cloned()
                .ok_or_else(|| err_msg("Shard not found"))
        })
    }

    fn get_shard_for_document(
        &self,
        index_name: &str,
        document_id: u64,
    ) -> impl Future<Item = ShardState, Error = Error> {
        self.get_cached_index(index_name).map(move |index| {
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

    fn get_cached_index(&self, index_name: &str) -> impl Future<Item = IndexState, Error = Error> {
        let cache = self.index_cache.read().unwrap();
        let f: Box<Future<Item = IndexState, Error = Error> + Send> =
            if cache.contains_key(index_name) {
                Box::new(futures::future::ok(cache.get(index_name).cloned().unwrap()))
            } else {
                drop(cache);
                Box::new(self.refresh_index_cache(index_name))
            };
        f
    }

    // TODO: Need to refresh this when routing is out of date
    fn refresh_index_cache(
        &self,
        index_name: &str,
    ) -> impl Future<Item = IndexState, Error = Error> {
        let cache = self.index_cache.clone();
        self.get_index(index_name.to_string()).map(move |index| {
            let mut locked = cache.write().unwrap();
            locked.insert(index.get_name().to_string(), index.clone());
            index
        })
    }

    fn peer(&self, id: u64) -> Result<RpcClient, Error> {
        self.gossip_state.get_client(id)
    }

    fn leader_client(&self) -> Result<RpcClient, Error> {
        self.gossip_state.get_meta_leader_client()
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
