use crate::mappings::Mappings;
use crate::proto::*;
use failure::Error;
use futures::prelude::*;
use grpcio::{CallOption, ClientUnaryReceiver, EnvBuilder, ChannelBuilder};
use protobuf::Message;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct RpcClient {
    node_id: u64,
    client: InternalClient,
}

pub trait RpcFuture<T> = Future<Item=T, Error=Error>;

fn futurize<T>(input: Result<ClientUnaryReceiver<T>, grpcio::Error>)
    -> impl RpcFuture<T>
{
    futures::future::result(input)
        .and_then(|r| r)
        .map_err(|err| err.into())
}

fn futurize_unit<T>(input: Result<ClientUnaryReceiver<T>, grpcio::Error>)
    -> impl RpcFuture<()>
{
    futures::future::result(input)
        .and_then(|r| r)
        .map_err(|err| err.into())
        .map(|_| ())
}


impl RpcClient {
    pub fn new(node_id: u64, address: &str) -> Self {
        let env = Arc::new(EnvBuilder::new().build());
        let channel = ChannelBuilder::new(env).connect(address);

        Self {
            node_id,
            client: InternalClient::new(channel),
        }
    }

    pub fn hello(&self) -> impl RpcFuture<HelloResponse> {
        let mut request = HelloRequest::new();
        request.peer_id = self.node_id;
        futurize(self.client.hello_async_opt(&request, self.options()))
    }

    pub fn raft_message(
        &self,
        message: &raft::eraftpb::Message,
        raft_group_id: u64,
    ) -> impl RpcFuture<()> {
        let mut request = SearchifyRaftMessage::new();
        request.wrapped_message = message.write_to_bytes().unwrap();
        request.raft_group_id = raft_group_id;
        futurize_unit(self.client.raft_message_async_opt(&request, self.options()))
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> impl RpcFuture<()> {
        let mut kv = KeyValue::new();
        kv.key = key.to_vec();
        kv.value = value.to_vec();
        futurize_unit(self.client.set_async_opt(&kv, self.options()))
    }

    pub fn get(&self, key: &[u8]) -> impl RpcFuture<KeyValue> {
        let mut request = Key::new();
        request.key = key.to_vec();
        futurize(self.client.get_async_opt(&request, self.options()))
    }

    pub fn create_index(
        &self,
        name: &str,
        shard_count: u64,
        replica_count: u64,
        mappings: Mappings,
    ) -> impl RpcFuture<()> {
        let mut request = CreateIndexRequest::new();
        request.set_name(name.to_string());
        request.set_shard_count(shard_count);
        request.set_replica_count(replica_count);
        request.set_mappings(serde_json::to_string(&mappings).unwrap());
        futurize_unit(self.client.create_index_async_opt(&request, self.options()))
    }

    pub fn delete_index(&self, name: &str) -> impl RpcFuture<()> {
        let mut request = DeleteIndexRequest::new();
        request.set_name(name.to_string());
        futurize_unit(self.client.delete_index_async_opt(&request, self.options()))
    }

    pub fn get_index(&self, name: &str) -> impl RpcFuture<IndexState> {
        let mut request = GetIndexRequest::new();
        request.set_name(name.to_string());
        futurize(self.client.get_index_async_opt(&request, self.options()))
    }

    pub fn list_indices(&self) -> impl RpcFuture<ListIndicesResponse> {
        let request = ListIndicesRequest::new();
        futurize(self.client.list_indices_async_opt(&request, self.options()))
    }

    pub fn list_shards(&self, node: u64) -> impl RpcFuture<Vec<ShardState>> {
        let mut request = ListShardsRequest::new();
        let mut peer = Peer::new();
        peer.set_id(node);
        request.set_peer(peer);
        futurize(self.client.list_shards_async_opt(&request, self.options()))
            .map(|mut resp| resp.take_shards().into_vec())
    }

    pub fn list_nodes(&self) -> impl RpcFuture<Vec<NodeState>> {
        let request = ListNodesRequest::new();
        futurize(self.client.list_nodes_async_opt(&request, self.options()))
            .map(|mut resp| resp.take_nodes().into_vec())
    }

    pub fn heartbeat(&self) -> impl RpcFuture<()> {
        let mut request = HeartbeatRequest::new();
        let mut peer = Peer::new();
        peer.id = self.node_id;
        request.set_peer(peer);
        futurize_unit(self.client.heartbeat_async_opt(&request, self.options()))
    }

    pub fn index_document(
        &self,
        index_name: &str,
        shard_id: u64,
        payload: serde_json::Value,
    ) -> impl RpcFuture<()> {
        let mut request = IndexDocumentRequest::new();
        request.shard_id = shard_id;
        request.payload = serde_json::to_string(&payload).unwrap();
        futurize_unit(self.client.index_document_async_opt(&request, self.options()))
    }

    pub fn search(
        &self,
        index_name: &str,
        shard_id: u64,
        payload: Vec<u8>,
    ) -> impl RpcFuture<SearchResponse> {
        let mut request = SearchRequest::new();
        request.set_shard_id(shard_id);
        request.set_query(payload);
        futurize(self.client.search_async_opt(&request, self.options()))
    }

    fn options(&self) -> CallOption {
        CallOption::default()
            .wait_for_ready(true)
            .timeout(Duration::from_secs(3))
    }
}
