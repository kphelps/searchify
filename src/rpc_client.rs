use crate::document::DocumentId;
use crate::mappings::Mappings;
use crate::metrics::{GRPC_CLIENT_HISTOGRAM, GRPC_CLIENT_ERROR_COUNTER};
use crate::proto::gossip::*;
use crate::proto::gossip_grpc::*;
use crate::proto::*;
use failure::Error;
use futures::prelude::*;
use grpcio::{CallOption, ChannelBuilder, ClientUnaryReceiver, EnvBuilder};
use protobuf::Message;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct RpcClient {
    node_id: u64,
    pub client: InternalClient,
    gossip_client: GossipClient,
}

pub fn futurize<T>(
    name: &str,
    input: Result<ClientUnaryReceiver<T>, grpcio::Error>,
) -> impl Future<Item = T, Error = Error> {
    let timer = GRPC_CLIENT_HISTOGRAM.with_label_values(&[name]).start_timer();
    let error_counter = GRPC_CLIENT_ERROR_COUNTER.with_label_values(&[name]);
    futures::future::result(input)
        .and_then(|r| r)
        .inspect(|_| timer.observe_duration())
        .map_err(move |err| {
            error_counter.inc();
            err.into()
        })
}

fn futurize_unit<T>(
    name: &str,
    input: Result<ClientUnaryReceiver<T>, grpcio::Error>,
) -> impl Future<Item = (), Error = Error> {
    let timer = GRPC_CLIENT_HISTOGRAM.with_label_values(&[name]).start_timer();
    let error_counter = GRPC_CLIENT_ERROR_COUNTER.with_label_values(&[name]);
    futures::future::result(input)
        .and_then(|r| r)
        .map_err(move |err| {
            error_counter.inc();
            err.into()
        })
        .inspect(|_| timer.observe_duration())
        .map(|_| ())
}

impl RpcClient {
    pub fn new(node_id: u64, address: &str) -> Self {
        let env = Arc::new(EnvBuilder::new().build());
        let channel = ChannelBuilder::new(env).connect(address);

        Self {
            node_id,
            client: InternalClient::new(channel.clone()),
            gossip_client: GossipClient::new(channel),
        }
    }

    pub fn gossip(&self, data: &GossipData) -> impl Future<Item = GossipData, Error = Error> {
        futurize("gossip", self.gossip_client.exchange_async_opt(data, self.options()))
    }

    pub fn raft_message(
        &self,
        message: &raft::eraftpb::Message,
        raft_group_id: u64,
    ) -> impl Future<Item = (), Error = Error> {
        let mut request = SearchifyRaftMessage::new();
        request.wrapped_message = message.write_to_bytes().unwrap();
        request.raft_group_id = raft_group_id;
        futurize_unit("raft", self.client.raft_message_async_opt(&request, self.options()))
    }

    pub fn create_index(
        &self,
        name: &str,
        shard_count: u64,
        replica_count: u64,
        mappings: Mappings,
    ) -> impl Future<Item = (), Error = Error> {
        let mut request = CreateIndexRequest::new();
        request.set_name(name.to_string());
        request.set_shard_count(shard_count);
        request.set_replica_count(replica_count);
        request.set_mappings(serde_json::to_string(&mappings).unwrap());
        futurize_unit("create_index", self.client.create_index_async_opt(&request, self.options()))
    }

    pub fn delete_index(&self, name: &str) -> impl Future<Item = (), Error = Error> {
        let mut request = DeleteIndexRequest::new();
        request.set_name(name.to_string());
        futurize_unit("delete_index", self.client.delete_index_async_opt(&request, self.options()))
    }

    pub fn get_index(&self, name: &str) -> impl Future<Item = IndexState, Error = Error> {
        let mut request = GetIndexRequest::new();
        request.set_name(name.to_string());
        futurize("get_index", self.client.get_index_async_opt(&request, self.options()))
    }

    pub fn list_indices(&self) -> impl Future<Item = ListIndicesResponse, Error = Error> {
        let request = ListIndicesRequest::new();
        futurize("list_indices", self.client.list_indices_async_opt(&request, self.options()))
    }

    pub fn list_shards(&self, node: u64) -> impl Future<Item = Vec<ShardState>, Error = Error> {
        let mut request = ListShardsRequest::new();
        let mut peer = Peer::new();
        peer.set_id(node);
        request.set_peer(peer);
        futurize("list_shards", self.client.list_shards_async_opt(&request, self.options()))
            .map(|mut resp| resp.take_shards().into_vec())
    }

    #[allow(dead_code)]
    pub fn list_nodes(&self) -> impl Future<Item = Vec<NodeState>, Error = Error> {
        let request = ListNodesRequest::new();
        futurize("list_nodes", self.client.list_nodes_async_opt(&request, self.options()))
            .map(|mut resp| resp.take_nodes().into_vec())
    }

    pub fn heartbeat(&self) -> impl Future<Item = (), Error = Error> {
        let mut request = HeartbeatRequest::new();
        let mut peer = Peer::new();
        peer.id = self.node_id;
        request.set_peer(peer);
        futurize_unit("heartbeat", self.client.heartbeat_async_opt(&request, self.options()))
    }

    pub fn index_document(
        &self,
        _index_name: &str,
        shard_id: u64,
        document_id: DocumentId,
        payload: serde_json::Value,
    ) -> impl Future<Item = (), Error = Error> {
        let mut request = IndexDocumentRequest::new();
        request.shard_id = shard_id;
        request.payload = serde_json::to_string(&payload).unwrap();
        request.document_id = document_id.into();
        futurize_unit(
            "index_document",
            self.client
                .index_document_async_opt(&request, self.options()),
        )
    }

    pub fn delete_document(
        &self,
        shard_id: u64,
        document_id: DocumentId,
    ) -> impl Future<Item = DeleteDocumentResponse, Error = Error> {
        let mut request = DeleteDocumentRequest::new();
        request.shard_id = shard_id;
        request.document_id = document_id.into();
        futurize(
            "delete_document",
            self.client
                .delete_document_async_opt(&request, self.options()),
        )
    }

    pub fn get_document(
        &self,
        shard_id: u64,
        document_id: DocumentId,
    ) -> impl Future<Item = GetDocumentResponse, Error = Error> {
        let mut request = GetDocumentRequest::new();
        request.shard_id = shard_id;
        request.document_id = document_id.into();
        futurize("get_document", self.client.get_document_async_opt(&request, self.options()))
    }

    pub fn search(
        &self,
        _index_name: &str,
        shard_id: u64,
        payload: Vec<u8>,
    ) -> impl Future<Item = SearchResponse, Error = Error> {
        let mut request = SearchRequest::new();
        request.set_shard_id(shard_id);
        request.set_query(payload);
        futurize("search", self.client.search_async_opt(&request, self.options()))
    }

    pub fn refresh_shard(&self, shard_id: u64) -> impl Future<Item = (), Error = Error> {
        let mut request = RefreshRequest::new();
        request.set_shard_id(shard_id);
        futurize_unit("refresh_shard", self.client.refresh_async_opt(&request, self.options()))
    }

    pub fn options(&self) -> CallOption {
        CallOption::default()
            .wait_for_ready(true)
            .timeout(Duration::from_secs(30))
    }
}
