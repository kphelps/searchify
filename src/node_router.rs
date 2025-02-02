use crate::cluster_state::{ClusterState, ClusterStateRef};
use crate::config::Config;
use crate::document::DocumentId;
use crate::gossip::GossipState;
use crate::mappings::Mappings;
use crate::proto::*;
use crate::rpc_client::RpcClient;
use failure::{Error, Fail};
use futures::{future, prelude::*, sync::oneshot};
use log::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_timer::Interval;

pub struct NodeRouter {
    gossip_state: GossipState,
    tasks: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
    cluster_state: ClusterStateRef,
}

#[derive(Clone)]
pub struct NodeRouterHandle {
    handle: Arc<NodeRouter>,
}

#[derive(Debug, Fail)]
pub enum SearchError {
    #[fail(display = "index not found")]
    IndexNotFound,
    #[fail(display = "shard not found")]
    ShardNotFound,
    #[fail(display = "cluster state unavailable")]
    ClusterStateUnavailable,
    #[fail(display = "leader unavailable")]
    LeaderUnavailable,
    #[fail(display = "Unknown error: {}", 0)]
    Error(String),
}

impl From<Error> for SearchError {
    fn from(error: Error) -> Self {
        SearchError::Error(format!("{}", error))
    }
}

impl NodeRouter {
    fn new(config: &Config, gossip_state: GossipState, cluster_state: ClusterState) -> Self {
        let mut clients = HashMap::new();
        let self_address = format!("127.0.0.1:{}", config.port);
        clients.insert(
            config.node_id,
            RpcClient::new(config.node_id, &self_address),
        );
        Self {
            gossip_state,
            cluster_state: cluster_state.handle(),
            tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn start(
        config: &Config,
        gossip_state: GossipState,
        cluster_state: ClusterState,
    ) -> Result<NodeRouterHandle, Error> {
        let router = NodeRouter::new(config, gossip_state, cluster_state);
        let handle = NodeRouterHandle {
            handle: Arc::new(router),
        };

        let heartbeat_handle = Arc::downgrade(&handle.handle);
        let heartbeat_task = Interval::new_interval(Duration::from_secs(5))
            .map_err(|_| ())
            .and_then(move |_| heartbeat_handle.upgrade().ok_or(()))
            .for_each(move |handle| {
                handle
                    .send_heartbeat()
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
    ) -> Result<(), Error> {
        self.peer(message.to)
            .and_then(move |peer| peer.raft_message(&message, raft_group_id))
    }

    pub fn create_index(
        &self,
        name: String,
        shard_count: u64,
        replica_count: u64,
        mappings: Mappings,
    ) -> impl Future<Item = (), Error = SearchError> {
        self.with_leader_client(move |client| {
            client.create_index(&name, shard_count, replica_count, mappings)
        })
    }

    pub fn delete_index(&self, name: String) -> impl Future<Item = (), Error = SearchError> {
        self.with_leader_client(move |client| client.delete_index(&name))
    }

    pub fn get_index(&self, name: String) -> impl Future<Item = IndexState, Error = SearchError> {
        self.with_leader_client(move |client| client.get_index(&name))
    }

    pub fn list_indices(&self) -> impl Future<Item = ListIndicesResponse, Error = SearchError> {
        self.with_leader_client(move |client| client.list_indices())
    }

    pub fn send_heartbeat(&self) -> impl Future<Item = (), Error = SearchError> {
        self.with_leader_client(|client| client.heartbeat())
    }

    pub fn index_document(
        &self,
        index_name: String,
        document_id: DocumentId,
        payload: serde_json::Value,
    ) -> impl Future<Item = (), Error = SearchError> {
        self.get_shard_client_for_document(&index_name, &document_id)
            .into_future()
            .and_then(move |shard_client| {
                shard_client
                    .client
                    .index_document(&index_name, shard_client.shard.id, document_id, payload)
                    .from_err()
            })
    }

    pub fn get_document(
        &self,
        index_name: String,
        document_id: DocumentId,
    ) -> impl Future<Item = GetDocumentResponse, Error = SearchError> {
        self.get_shard_client_for_document(&index_name, &document_id)
            .into_future()
            .and_then(|client| {
                client
                    .client
                    .get_document(client.shard.id, document_id)
                    .from_err()
            })
    }

    pub fn search(
        &self,
        index_name: String,
        query: Vec<u8>,
    ) -> impl Future<Item = MergedSearchResponse, Error = SearchError> {
        let resolver = self.gossip_state.clone();
        let limit = 10;
        self.get_cached_index(&index_name)
            .into_future()
            .and_then(move |index| {
                let futures = index.shards.into_iter().map(move |shard| {
                    let query = query.clone();
                    let index_name = index_name.clone();
                    let replica_id = shard.replicas.first().unwrap().id;
                    // lift the future error up into the response so we can join all
                    resolver
                        .get_client(replica_id)
                        .into_future()
                        .and_then(move |client| client.search(&index_name, shard.id, query.clone()))
                        .then(future::ok)
                });
                future::join_all(futures)
            })
            .map(move |results| {
                let mut merged = MergedSearchResponse::new();
                let mut hit_list = Vec::new();
                let mut successes = 0;
                let mut total = 0;
                merged.set_shard_count(results.len() as u64);
                results.into_iter().for_each(|result| {
                    if let Ok(mut response) = result {
                        successes += 1;
                        total += response.total;
                        hit_list.extend(response.take_hits().into_iter());
                    }
                });
                hit_list.sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
                let top_hits: Vec<SearchHit> = hit_list.into_iter().take(limit).collect();
                merged.set_success_count(successes);
                merged.set_hit_total(total);
                merged.set_hits(top_hits.into());
                merged
            })
    }

    pub fn delete_document(
        &self,
        index_name: String,
        id: DocumentId,
    ) -> impl Future<Item = DeleteDocumentResponse, Error = SearchError> {
        self.get_shard_client_for_document(&index_name, &id)
            .into_future()
            .and_then(move |client| {
                client
                    .client
                    .delete_document(client.shard.id, id)
                    .from_err()
            })
    }

    pub fn refresh_index(&self, index_name: &str) -> impl Future<Item = (), Error = SearchError> {
        let resolver = self.gossip_state.clone();
        let index_res = self.get_cached_index(index_name);
        future::result(index_res)
            .and_then(move |mut index| {
                let shards = index.take_shards().into_iter().map(move |shard| {
                    let replica_id = shard.replicas.first().unwrap().id;
                    resolver
                        .get_client(replica_id)
                        .into_future()
                        .and_then(move |client| client.refresh_shard(shard.id))
                });
                future::join_all(shards).from_err()
            })
            .map(|_| ())
    }

    pub fn refresh_shard(&self, shard_id: u64) -> impl Future<Item = (), Error = SearchError> {
        let resolver = self.gossip_state.clone();
        self.get_cached_shard(shard_id)
            .into_future()
            .and_then(move |shard| {
                let replica_id = shard.replicas.first().unwrap().id;
                resolver
                    .get_client(replica_id)
                    .into_future()
                    .and_then(move |client| client.refresh_shard(shard.id))
                    .from_err()
            })
    }

    pub fn get_shard_for_document(
        &self,
        index_name: &str,
        document_id: &DocumentId,
    ) -> Result<ShardState, SearchError> {
        let id = document_id.routing_id();
        self.get_cached_index(index_name).map(move |index| {
            index
                .shards
                .into_iter()
                .find(|shard| {
                    let low_id = shard.get_range().low;
                    let high_id = shard.get_range().high;
                    low_id <= id && id <= high_id
                })
                .expect("Invalid range")
        })
    }

    pub fn get_shard_client_for_document(
        &self,
        index_name: &str,
        document_id: &DocumentId,
    ) -> Result<ShardClient, SearchError> {
        let resolver = self.gossip_state.clone();
        self.get_shard_for_document(index_name, document_id)
            .and_then(move |shard| {
                let replica_id = shard.replicas.first().unwrap().id;
                let client = resolver.get_client(replica_id)?;
                Ok(ShardClient { client, shard })
            })
    }

    pub fn get_cached_index(&self, index_name: &str) -> Result<IndexState, SearchError> {
        self.cluster_state
            .upgrade()
            .ok_or(SearchError::ClusterStateUnavailable)
            .and_then(|cs| cs.get_index(index_name).ok_or(SearchError::IndexNotFound))
    }

    pub fn get_cached_shard(&self, id: u64) -> Result<ShardState, SearchError> {
        self.cluster_state
            .upgrade()
            .ok_or(SearchError::ClusterStateUnavailable)
            .and_then(|cs| cs.get_shard(id).ok_or(SearchError::ShardNotFound))
    }

    fn peer(&self, id: u64) -> Result<RpcClient, Error> {
        self.gossip_state.get_client(id)
    }

    pub fn leader_client(&self) -> Result<RpcClient, SearchError> {
        self.gossip_state
            .get_meta_leader_client()
            .map_err(|_| SearchError::LeaderUnavailable)
    }

    fn with_leader_client<F, X, R, E>(&self, f: F) -> impl Future<Item = R, Error = SearchError>
    where
        F: FnOnce(RpcClient) -> X,
        X: Future<Item = R, Error = E>,
        SearchError: From<E>,
    {
        future::result(self.leader_client()).and_then(|c| f(c).from_err())
    }
}

impl std::ops::Deref for NodeRouterHandle {
    type Target = NodeRouter;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

pub struct ShardClient {
    pub client: RpcClient,
    pub shard: ShardState,
}
