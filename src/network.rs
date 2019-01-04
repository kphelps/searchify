use actix::prelude::*;
use crate::key_value_state_machine::KeyValueStateMachine;
use crate::search_state_machine::SearchStateMachine;
use crate::proto::*;
use crate::raft::{
    RaftClient,
    RaftMessageReceived,
    RaftPropose,
    RaftStateMachine,
    StateMachineObserver,
};
use failure::{err_msg, Error};
use futures::{
    future,
    prelude::*,
    sync::oneshot::{channel, Receiver},
};
use grpcio::{
    EnvBuilder,
    RpcContext,
    RpcStatus,
    RpcStatusCode,
    Server,
    ServerBuilder,
    UnarySink,
};
use log::*;
use protobuf::parse_from_bytes;
use raft::eraftpb;
use std::boxed::Box;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc};

#[derive(Clone)]
struct InternalServer {
    peer_id: u64,
    network: Addr<NetworkActor>,
}

impl Internal for InternalServer {
    fn hello(
        &mut self,
        ctx: RpcContext,
        req: HelloRequest,
        sink: UnarySink<HelloResponse>,
    ) {
        info!("Connection from peer '{}'", req.peer_id);
        let mut resp = HelloResponse::new();
        resp.peer_id = self.peer_id;
        ctx.spawn(sink.success(resp).map(|_| ()).map_err(|_| ()));
    }

    fn heartbeat(
        &mut self,
        ctx: RpcContext,
        req: HeartbeatRequest,
        sink: UnarySink<EmptyResponse>,
    ) {
        debug!("Heartbeat from '{}'", req.get_peer().get_id());
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::propose_heartbeat(req, sender);
        propose_api(&self.network, proposal, receiver, ctx, sink);
    }

    fn raft_message(
        &mut self,
        ctx: RpcContext,
        req: SearchifyRaftMessage,
        sink: UnarySink<EmptyResponse>,
    ) {
        let network = self.network.clone();
        let raft_message = parse_from_bytes::<eraftpb::Message>(&req.wrapped_message).unwrap();
        let f = network.send(RaftMessageReceived{
            raft_group_id: req.raft_group_id,
            message: raft_message,
        }).flatten().map(|_| EmptyResponse::new());
        future_to_sink(f, ctx, sink);
    }

    fn set(
        &mut self,
        ctx: RpcContext,
        req: KeyValue,
        sink: UnarySink<EmptyResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::propose_set(req, sender);
        propose_api(&self.network, proposal, receiver, ctx, sink);
    }

    fn get(
        &mut self,
        ctx: RpcContext,
        req: Key,
        sink: UnarySink<KeyValue>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::propose_get(req, sender);
        propose_api(&self.network, proposal, receiver, ctx, sink);
    }

    fn create_index(
        &mut self,
        ctx: RpcContext,
        req: CreateIndexRequest,
        sink: UnarySink<CreateIndexResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::propose_create_index(req, sender);
        propose_api(&self.network, proposal, receiver, ctx, sink);
    }

    fn delete_index(
        &mut self,
        ctx: RpcContext,
        req: DeleteIndexRequest,
        sink: UnarySink<EmptyResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::propose_delete_index(req, sender);
        propose_api(&self.network, proposal, receiver, ctx, sink);
    }

    fn get_index(
        &mut self,
        ctx: RpcContext,
        req: GetIndexRequest,
        sink: UnarySink<IndexState>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            sm.index(&req.name).and_then(|option| option.ok_or(err_msg("Not found")))
        });
        propose_api_result(&self.network, proposal, receiver, ctx, sink);
    }

    fn list_indices(
        &mut self,
        ctx: RpcContext,
        req: ListIndicesRequest,
        sink: UnarySink<ListIndicesResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(
            sender,
            |sm| {
                let indices = sm.list_indices();
                let mut response = ListIndicesResponse::new();
                response.set_indices(indices.into());
                response
            }
        );
        propose_api(&self.network, proposal, receiver, ctx, sink);
    }

    fn list_nodes(
        &mut self,
        ctx: RpcContext,
        req: ListNodesRequest,
        sink: UnarySink<ListNodesResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            let mut response = ListNodesResponse::new();
            let peer_states = sm.live_nodes();
            let nodes = peer_states.into_iter()
                .map(|peer_state| {
                    let mut node_state = NodeState::new();
                    node_state.set_peer_state(peer_state);
                    node_state
                })
                .collect();
            response.set_nodes(nodes);
            Ok(response)
        });
        propose_api_result(&self.network, proposal, receiver, ctx, sink);
    }

    fn health(
        &mut self,
        ctx: RpcContext,
        req: HealthRequest,
        sink: UnarySink<HealthResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            let mut response = HealthResponse::new();
            response.available = true;
            response.fully_replicated = true;
            response
        });
        propose_api(&self.network, proposal, receiver, ctx, sink);
    }

    fn list_shards(
        &mut self,
        ctx: RpcContext,
        req: ListShardsRequest,
        sink: UnarySink<ListShardsResponse>
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            let mut response = ListShardsResponse::new();
            response.set_shards(sm.shards_for_node(req.get_peer().get_id())?.into());
            Ok(response)
        });
        propose_api_result(&self.network, proposal, receiver, ctx, sink);
    }

    fn index_document(
        &mut self,
        ctx: RpcContext,
        req: IndexDocumentRequest,
        sink: UnarySink<IndexDocumentResponse>,
    ) {
        let (sender, receiver) = channel();
        let proposal = SearchStateMachine::propose_add_document(req, sender);
        propose_api(&self.network, proposal, receiver, ctx, sink);
    }
}


fn propose_api<T, O, K>(
    network: &Addr<NetworkActor>,
    proposal: RaftPropose<O, K>,
    receiver: Receiver<T>,
    ctx: RpcContext,
    sink: UnarySink<T>,
)
where T: Default + Debug + Send + 'static,
      O: StateMachineObserver<K> + Send + 'static,
      K: RaftStateMachine + 'static,
      NetworkActor: Handler<RaftPropose<O, K>>
{
    let f = network.send(proposal).flatten().and_then(|_| receiver.from_err());
    future_to_sink(f, ctx, sink);
}

fn propose_api_result<T, O>(
    network: &Addr<NetworkActor>,
    proposal: RaftPropose<O, KeyValueStateMachine>,
    receiver: Receiver<Result<T, Error>>,
    ctx: RpcContext,
    sink: UnarySink<T>,
)
where T: Default + Debug + Send + 'static,
      O: StateMachineObserver<KeyValueStateMachine> + Send + 'static
{
    let f = network.send(proposal).flatten().and_then(|_| receiver.from_err()).flatten();
    future_to_sink(f, ctx, sink);
}

fn future_to_sink<F, I, E>(f: F, ctx: RpcContext, sink: UnarySink<I>)
    where F: Future<Item=I, Error=E> + Send + 'static,
          I: Send + 'static,
          E: Into<Error> + Send + Sync
{
    let f = f.map_err(|e| e.into())
        .then(|out| match out {
            Ok(value) => sink.success(value).map_err(Error::from),
            Err(e) => {
                let status = RpcStatus::new(
                    RpcStatusCode::Internal,
                    Some(format!("{}", e))
                );
                sink.fail(status).map_err(Error::from)
            },
        });
    ctx.spawn(f.map(|_| ()).map_err(|err| error!("Failed to handle RPC: {:?}", err)));
}

pub struct NetworkActor {
    peer_id: u64,
    server: Option<Server>,
    kv_raft_groups: HashMap<u64, Addr<RaftClient<KeyValueStateMachine>>>,
    search_raft_groups: HashMap<u64, Addr<RaftClient<SearchStateMachine>>>,
}

#[derive(Message)]
#[rtype(result="Result<(), Error>")]
struct ConnectToPeer {
    address: String,
}

#[derive(Message)]
struct ServerStarted {
    server: Server,
}

#[derive(Message)]
pub struct RaftGroupStarted<T>
    where T: RaftStateMachine + 'static,
          NetworkActor: Handler<RaftGroupStarted<T>>
{
    pub raft_group_id: u64,
    pub raft_group: Addr<RaftClient<T>>
}

#[derive(Message)]
#[rtype(result="Result<(), Error>")]
pub struct CreateIndex {
    pub index_name: String,
}

const GLOBAL_RAFT_ID: u64 = 0;

impl NetworkActor {
    fn new(peer_id: u64) -> Self {
        Self{
            peer_id: peer_id,
            server: None,
            kv_raft_groups: HashMap::new(),
            search_raft_groups: HashMap::new(),
        }
    }

    pub fn start(peer_id: u64, port: u16) -> Result<Addr<Self>, Error> {
        let addr = Self::new(peer_id).start();
        let service = create_internal(InternalServer{
            peer_id: peer_id,
            network: addr.clone(),
        });
        let env = Arc::new(EnvBuilder::new().build());
        let mut server = ServerBuilder::new(env)
            .register_service(service)
            .bind("127.0.0.1", port)
            .build()?;
        server.start();
        addr.try_send(ServerStarted{server}).expect("init");
        info!("RPC Server started");
        Ok(addr)
    }

    fn global_raft(&self) -> Option<&Addr<RaftClient<KeyValueStateMachine>>> {
        self.kv_raft_groups.get(&GLOBAL_RAFT_ID)
    }
}

impl Actor for NetworkActor {
    type Context = Context<Self>;
}

impl Handler<ServerStarted> for NetworkActor {
    type Result = ();

    fn handle(&mut self, message: ServerStarted, ctx: &mut Context<Self>) {
        self.server = Some(message.server);
    }
}

impl Handler<RaftMessageReceived> for NetworkActor {
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, message: RaftMessageReceived, _ctx: &mut Context<Self>) -> Self::Result {
        debug!("[group-{}] Received message for {}", message.raft_group_id, message.message.to);
        let kv_raft = self.kv_raft_groups.get(&message.raft_group_id)
            .cloned()
            .map(|r| r.recipient());
        let search_raft = self.search_raft_groups.get(&message.raft_group_id)
            .cloned()
            .map(|r| r.recipient());
        let raft = kv_raft.or(search_raft);
        if raft.is_none() {
            let error = err_msg(format!("Unknown raft group: {}", message.raft_group_id));
            return Box::new(future::err(error));
        }
        let f = raft.unwrap().send(message).flatten();
        Box::new(f)
    }
}

impl<O> Handler<RaftPropose<O, KeyValueStateMachine>> for NetworkActor
    where O: StateMachineObserver<KeyValueStateMachine> + Send + 'static
{
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, message: RaftPropose<O, KeyValueStateMachine>, _ctx: &mut Context<Self>)
        -> Self::Result
    {
        let maybe_global = self.global_raft();
        if maybe_global.is_none() {
            return Box::new(future::err(err_msg("Not a member of meta group")))
        }
        let global = maybe_global.unwrap();
        let f = global.send(message).flatten();
        Box::new(f)
    }
}

impl<O> Handler<RaftPropose<O, SearchStateMachine>> for NetworkActor
where O: StateMachineObserver<SearchStateMachine> + Send + 'static
{
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, message: RaftPropose<O, SearchStateMachine>, _ctx: &mut Context<Self>)
              -> Self::Result
    {
        let maybe_group = self.search_raft_groups.get(&message.raft_group_id);
        if maybe_group.is_none() {
            return Box::new(future::err(err_msg("Not a member of shard group")))
        }
        let group = maybe_group.unwrap();
        debug!("[group-{}] Proposal!", message.raft_group_id);
        let f = group.send(message).flatten();
        Box::new(f)
    }
}

impl Handler<RaftGroupStarted<KeyValueStateMachine>> for NetworkActor {
    type Result = ();

    fn handle(&mut self, message: RaftGroupStarted<KeyValueStateMachine>, _ctx: &mut Context<Self>) {
        self.kv_raft_groups.insert(message.raft_group_id, message.raft_group);
    }
}

impl Handler<RaftGroupStarted<SearchStateMachine>> for NetworkActor {
    type Result = ();

    fn handle(&mut self, message: RaftGroupStarted<SearchStateMachine>, _ctx: &mut Context<Self>) {
        self.search_raft_groups.insert(message.raft_group_id, message.raft_group);
    }
}
