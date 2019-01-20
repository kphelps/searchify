use crate::clock::Clock;
use crate::key_value_state_machine::KeyValueStateMachine;
use crate::mappings::Mappings;
use crate::proto::*;
use crate::raft::{RaftMessageReceived, RaftPropose, RaftStateMachine};
use crate::raft_router::RaftRouter;
use crate::search_state_machine::SearchStateMachine;
use failure::{err_msg, Error};
use futures::{
    prelude::*,
    sync::oneshot::{channel, Receiver},
};
use grpcio::{
    EnvBuilder, RpcContext, RpcStatus, RpcStatusCode, Server, ServerBuilder, Service, UnarySink,
};
use log::*;
use protobuf::parse_from_bytes;
use raft::eraftpb;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct InternalServer {
    clock: Clock,
    peer_id: u64,
    kv_raft_router: RaftRouter<KeyValueStateMachine>,
    search_raft_router: RaftRouter<SearchStateMachine>,
}

impl Internal for InternalServer {
    fn heartbeat(
        &mut self,
        ctx: RpcContext,
        req: HeartbeatRequest,
        sink: UnarySink<EmptyResponse>,
    ) {
        debug!("Heartbeat from '{}'", req.get_peer().get_id());
        let (sender, receiver) = channel();
        let expires_at = self.clock.for_expiration_in(Duration::from_secs(15));
        let proposal = KeyValueStateMachine::propose_heartbeat(req, expires_at, sender);
        propose_api(&self.kv_raft_router, proposal, receiver, ctx, sink);
    }

    fn raft_message(
        &mut self,
        ctx: RpcContext,
        req: SearchifyRaftMessage,
        sink: UnarySink<EmptyResponse>,
    ) {
        let raft_message = parse_from_bytes::<eraftpb::Message>(&req.wrapped_message).unwrap();
        let message = RaftMessageReceived {
            raft_group_id: req.raft_group_id,
            message: raft_message,
        };
        if self.kv_raft_router.has_group(req.raft_group_id) {
            let f = self
                .kv_raft_router
                .handle_raft_message(message)
                .map(|_| EmptyResponse::new());
            future_to_sink(f, ctx, sink);
        } else {
            let f = self
                .search_raft_router
                .handle_raft_message(message)
                .map(|_| EmptyResponse::new());
            future_to_sink(f, ctx, sink);
        }
    }

    fn create_index(
        &mut self,
        ctx: RpcContext,
        req: CreateIndexRequest,
        sink: UnarySink<CreateIndexResponse>,
    ) {
        if let Err(err) = serde_json::from_str::<Mappings>(&req.mappings) {
            let status = RpcStatus::new(RpcStatusCode::InvalidArgument, Some(format!("{}", err)));
            ctx.spawn(sink.fail(status).map(|_| ()).map_err(|_| ()));
        } else {
            let (sender, receiver) = channel();
            let proposal = KeyValueStateMachine::propose_create_index(req, sender);
            propose_api(&self.kv_raft_router, proposal, receiver, ctx, sink);
        }
    }

    fn delete_index(
        &mut self,
        ctx: RpcContext,
        req: DeleteIndexRequest,
        sink: UnarySink<EmptyResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::propose_delete_index(req, sender);
        propose_api(&self.kv_raft_router, proposal, receiver, ctx, sink);
    }

    fn get_index(&mut self, ctx: RpcContext, req: GetIndexRequest, sink: UnarySink<IndexState>) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            sm.index(&req.name)
                .and_then(|option| option.ok_or_else(|| err_msg("Not found")))
        });
        propose_api_result(&self.kv_raft_router, proposal, receiver, ctx, sink);
    }

    fn list_indices(
        &mut self,
        ctx: RpcContext,
        _req: ListIndicesRequest,
        sink: UnarySink<ListIndicesResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, |sm| {
            let indices = sm.list_indices();
            let mut response = ListIndicesResponse::new();
            response.set_indices(indices.into());
            response
        });
        propose_api(&self.kv_raft_router, proposal, receiver, ctx, sink);
    }

    fn list_nodes(
        &mut self,
        ctx: RpcContext,
        _req: ListNodesRequest,
        sink: UnarySink<ListNodesResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            let mut response = ListNodesResponse::new();
            let peer_states = sm.live_nodes();
            let nodes = peer_states
                .into_iter()
                .map(|peer_state| {
                    let mut node_state = NodeState::new();
                    node_state.set_peer_state(peer_state);
                    node_state
                })
                .collect();
            response.set_nodes(nodes);
            Ok(response)
        });
        propose_api_result(&self.kv_raft_router, proposal, receiver, ctx, sink);
    }

    fn health(&mut self, ctx: RpcContext, _req: HealthRequest, sink: UnarySink<HealthResponse>) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |_| {
            // TODO
            let mut response = HealthResponse::new();
            response.available = true;
            response.fully_replicated = true;
            response
        });
        propose_api(&self.kv_raft_router, proposal, receiver, ctx, sink);
    }

    fn list_shards(
        &mut self,
        ctx: RpcContext,
        req: ListShardsRequest,
        sink: UnarySink<ListShardsResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            let mut response = ListShardsResponse::new();
            response.set_shards(sm.shards_for_node(req.get_peer().get_id())?.into());
            Ok(response)
        });
        propose_api_result(&self.kv_raft_router, proposal, receiver, ctx, sink);
    }

    fn index_document(
        &mut self,
        ctx: RpcContext,
        req: IndexDocumentRequest,
        sink: UnarySink<IndexDocumentResponse>,
    ) {
        info!("Index()");
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::propose_add_document(req, sender);
        propose_api(&self.search_raft_router, proposal, receiver, ctx, sink);
    }

    fn search(&mut self, ctx: RpcContext, req: SearchRequest, sink: UnarySink<SearchResponse>) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::search(req, sender);
        propose_api_result(&self.search_raft_router, proposal, receiver, ctx, sink);
    }

    fn refresh(&mut self, ctx: RpcContext, req: RefreshRequest, sink: UnarySink<RefreshResponse>) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::propose_refresh(req, sender);
        propose_api(&self.search_raft_router, proposal, receiver, ctx, sink);
    }
}

impl InternalServer {
    pub fn build_service(
        node_id: u64,
        clock: Clock,
        kv_raft_router: &RaftRouter<KeyValueStateMachine>,
        search_raft_router: &RaftRouter<SearchStateMachine>,
    ) -> Service {
        create_internal(Self {
            clock: clock,
            peer_id: node_id,
            kv_raft_router: kv_raft_router.clone(),
            search_raft_router: search_raft_router.clone(),
        })
    }
}

fn propose_api<T, K>(
    router: &RaftRouter<K>,
    proposal: RaftPropose<K>,
    receiver: Receiver<T>,
    ctx: RpcContext,
    sink: UnarySink<T>,
) where
    T: Default + Debug + Send + 'static,
    K: RaftStateMachine + Send + 'static,
{
    let f = router.propose(proposal).and_then(|_| receiver.from_err());
    future_to_sink(f, ctx, sink);
}

fn propose_api_result<T, K>(
    router: &RaftRouter<K>,
    proposal: RaftPropose<K>,
    receiver: Receiver<Result<T, Error>>,
    ctx: RpcContext,
    sink: UnarySink<T>,
) where
    T: Default + Debug + Send + 'static,
    K: RaftStateMachine + Send + 'static,
{
    let f = router
        .propose(proposal)
        .and_then(|_| receiver.from_err())
        .flatten();
    future_to_sink(f, ctx, sink);
}

fn future_to_sink<F, I, E>(f: F, ctx: RpcContext, sink: UnarySink<I>)
where
    F: Future<Item = I, Error = E> + Send + 'static,
    I: Send + 'static,
    E: Into<Error> + Send + Sync,
{
    let f = f.map_err(|e| e.into()).then(|out| match out {
        Ok(value) => sink.success(value).map_err(Error::from),
        Err(e) => {
            let status = RpcStatus::new(RpcStatusCode::Internal, Some(format!("{}", e)));
            sink.fail(status).map_err(Error::from)
        }
    });
    ctx.spawn(
        f.map(|_| ())
            .map_err(|err| error!("Failed to handle RPC: {:?}", err)),
    );
}

pub fn start_rpc_server(services: Vec<Service>, _node_id: u64, port: u16) -> Result<Server, Error> {
    let env = Arc::new(EnvBuilder::new().cq_count(32).build());
    let mut builder = ServerBuilder::new(env);
    builder = services.into_iter().fold(builder, |builder, service| {
        builder.register_service(service)
    });
    let mut server = builder.bind("127.0.0.1", port).build()?;
    server.start();
    info!("RPC Server started");
    Ok(server)
}
