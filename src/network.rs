use crate::clock::Clock;
use crate::key_value_state_machine::KeyValueStateMachine;
use crate::mappings::Mappings;
use crate::metrics::{GRPC_SERVER_ERROR_COUNTER, GRPC_SERVER_HISTOGRAM};
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
    ChannelBuilder, EnvBuilder, RpcContext, RpcStatus, RpcStatusCode, Server, ServerBuilder,
    Service, UnarySink,
};
use log::*;
use prost::Message;
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
        let (sender, receiver) = channel();
        let expires_at = self.clock.for_expiration_in(Duration::from_secs(15));
        let proposal = KeyValueStateMachine::propose_heartbeat(req, expires_at, sender);
        propose_api(
            "heartbeat",
            &self.kv_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn raft_message(
        &mut self,
        ctx: RpcContext,
        req: SearchifyRaftMessage,
        sink: UnarySink<EmptyResponse>,
    ) {
        let raft_message = eraftpb::Message::decode(&req.wrapped_message).unwrap();
        let message = RaftMessageReceived {
            raft_group_id: req.raft_group_id,
            message: raft_message,
        };
        if self.kv_raft_router.has_group(req.raft_group_id) {
            let f = self
                .kv_raft_router
                .handle_raft_message(message)
                .map(|_| EmptyResponse::new());
            future_to_sink("meta_raft", f, ctx, sink);
        } else {
            let f = self
                .search_raft_router
                .handle_raft_message(message)
                .map(|_| EmptyResponse::new());
            future_to_sink("search_raft", f, ctx, sink);
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
            propose_api(
                "create_index",
                &self.kv_raft_router,
                proposal,
                receiver,
                ctx,
                sink,
            );
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
        propose_api(
            "delete_index",
            &self.kv_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn get_index(&mut self, ctx: RpcContext, req: GetIndexRequest, sink: UnarySink<IndexState>) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            sm.index(&req.name)
                .and_then(|option| option.ok_or_else(|| err_msg("Not found")))
        });
        propose_api_result(
            "get_index",
            &self.kv_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
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
        propose_api(
            "list_indices",
            &self.kv_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn health(&mut self, ctx: RpcContext, _req: HealthRequest, sink: UnarySink<HealthResponse>) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| sm.health());
        propose_api(
            "health",
            &self.kv_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn index_document(
        &mut self,
        ctx: RpcContext,
        req: IndexDocumentRequest,
        sink: UnarySink<IndexDocumentResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::propose_add_document(req, sender);
        propose_api(
            "index_document",
            &self.search_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn get_document(
        &mut self,
        ctx: RpcContext,
        req: GetDocumentRequest,
        sink: UnarySink<GetDocumentResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::get_document(req, sender);
        propose_api_result(
            "get_document",
            &self.search_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn delete_document(
        &mut self,
        ctx: RpcContext,
        req: DeleteDocumentRequest,
        sink: UnarySink<DeleteDocumentResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::propose_delete_document(req, sender);
        propose_api(
            "delete_document",
            &self.search_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn search(&mut self, ctx: RpcContext, req: SearchRequest, sink: UnarySink<SearchResponse>) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::search(req, sender);
        propose_api_result(
            "search",
            &self.search_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn refresh(&mut self, ctx: RpcContext, req: RefreshRequest, sink: UnarySink<RefreshResponse>) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::propose_refresh(req, sender);
        propose_api(
            "refresh",
            &self.search_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
    }

    fn bulk(&mut self, ctx: RpcContext, req: BulkRequest, sink: UnarySink<BulkResponse>) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::propose_bulk(req, sender);
        propose_api(
            "bulk",
            &self.search_raft_router,
            proposal,
            receiver,
            ctx,
            sink,
        );
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
    name: &str,
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
    future_to_sink(name, f, ctx, sink);
}

fn propose_api_result<T, K>(
    name: &str,
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
    future_to_sink(name, f, ctx, sink);
}

fn future_to_sink<F, I, E>(name: &str, f: F, ctx: RpcContext, sink: UnarySink<I>)
where
    F: Future<Item = I, Error = E> + Send + 'static,
    I: Send + 'static,
    E: Into<Error> + Send + Sync,
{
    let timer = GRPC_SERVER_HISTOGRAM
        .with_label_values(&[name])
        .start_timer();
    let error_counter = GRPC_SERVER_ERROR_COUNTER.with_label_values(&[name]);
    let f = f.map_err(|e| e.into()).then(|out| match out {
        Ok(value) => sink.success(value).map_err(Error::from),
        Err(e) => {
            let status = RpcStatus::new(RpcStatusCode::Internal, Some(format!("{}", e)));
            sink.fail(status).map_err(Error::from)
        }
    });
    ctx.spawn(f.map(|_| timer.observe_duration()).map_err(move |err| {
        error_counter.inc();
        error!("Failed to handle RPC: {:?}", err);
    }));
}

pub fn start_rpc_server(services: Vec<Service>, _node_id: u64, port: u16) -> Result<Server, Error> {
    let env = Arc::new(EnvBuilder::new().cq_count(32).build());
    let channel_args = ChannelBuilder::new(Arc::clone(&env))
        .stream_initial_window_size(2 * 1024 * 1024)
        .max_concurrent_stream(1024)
        .max_receive_message_len(100 * 1024 * 1024)
        .max_send_message_len(-1)
        .build_args();
    let mut builder = ServerBuilder::new(env).channel_args(channel_args);
    builder = services.into_iter().fold(builder, |builder, service| {
        builder.register_service(service)
    });
    let mut server = builder.bind("127.0.0.1", port).build()?;
    server.start();
    info!("RPC Server started");
    Ok(server)
}
