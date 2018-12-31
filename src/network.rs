use actix::prelude::*;
use crate::key_value_state_machine::KeyValueStateMachine;
use crate::search_state_machine::SearchStateMachine;
use crate::proto::*;
use crate::raft::{
    GetLeaderId,
    RaftClient,
    RaftMessageReceived,
    RaftPropose,
    StateMachineObserver,
};
use crate::rpc_client::RpcClient;
use failure::{err_msg, Error};
use futures::{
    future,
    prelude::*,
    sync::oneshot::{channel, Receiver},
};
use grpcio::{
    CallOption,
    ChannelBuilder,
    EnvBuilder,
    RpcContext,
    RpcStatus,
    RpcStatusCode,
    Server,
    ServerBuilder,
    UnarySink,
};
use log::*;
use protobuf::{Message, parse_from_bytes};
use raft::eraftpb;
use std::boxed::Box;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

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
        info!("Heartbeat from '{}'", req.get_peer().get_id());
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

    fn show_index(
        &mut self,
        ctx: RpcContext,
        req: ShowIndexRequest,
        sink: UnarySink<IndexState>,
    ) {
        let (sender, receiver) = channel();
        let proposal = KeyValueStateMachine::read_operation(sender, move |sm| {
            sm.index(&req.name).and_then(|option| option.ok_or(err_msg("Not found")))
        });
        propose_api_result(&self.network, proposal, receiver, ctx, sink);
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
}


fn propose_api<T, O>(
    network: &Addr<NetworkActor>,
    proposal: RaftPropose<O, KeyValueStateMachine>,
    receiver: Receiver<T>,
    ctx: RpcContext,
    sink: UnarySink<T>,
)
where T: Default + Debug + Send + 'static,
      O: StateMachineObserver<KeyValueStateMachine> + Send + 'static
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
    ctx.spawn(f.map(|_| ()).map_err(|_| error!("Failed to handle RPC")));
}

pub struct NetworkActor {
    peer_id: u64,
    seeds: Vec<String>,
    // TODO: Probably can find a way to get rid of this Arc...
    peers: Arc<RwLock<HashMap<u64, RpcClient>>>,
    server: Option<Server>,
    kv_raft_groups: HashMap<u64, Addr<RaftClient<KeyValueStateMachine>>>,
    search_raft_groups: HashMap<u64, Addr<RaftClient<SearchStateMachine>>>,
}

#[derive(Message)]
#[rtype(result="Result<(), Error>")]
pub struct SendRaftMessage {
    pub raft_group_id: u64,
    pub message: eraftpb::Message,
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
#[rtype(result="Result<(), Error>")]
pub struct CreateIndex {
    pub index_name: String,
}

const GLOBAL_RAFT_ID: u64 = 0;

impl NetworkActor {
    fn new(
        peer_id: u64,
        peers: &[String],
        port: u16,
        raft: Addr<RaftClient<KeyValueStateMachine>>,
    ) -> Self {
        let mut kv_raft_groups = HashMap::new();
        kv_raft_groups.insert(GLOBAL_RAFT_ID, raft);
        let mut clients = HashMap::new();
        let self_address = format!("127.0.0.1:{}", port);
        clients.insert(peer_id, RpcClient::new(peer_id, &self_address));
        Self{
            peer_id: peer_id,
            seeds: peers.to_vec(),
            peers: Arc::new(RwLock::new(clients)),
            server: None,
            kv_raft_groups,
            search_raft_groups: HashMap::new(),
        }
    }

    pub fn start(
        peer_id: u64,
        port: u16,
        peers: &[String],
        raft: Addr<RaftClient<KeyValueStateMachine>>,
    ) -> Result<Addr<Self>, Error> {
        let addr = Self::new(peer_id, peers, port, raft).start();
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

    fn connect_to_peer(
        &mut self,
        peer_address: &str,
    ) -> impl Future<Item=(), Error=Error> {
        let peers = self.peers.clone();
        let client = RpcClient::new(self.peer_id, peer_address);
        client.hello().map(move |response| {
            peers.write().unwrap().insert(response.peer_id, client);
            info!("Connected to peer id '{}'", response.peer_id);
        })
    }

    fn global_raft(&self) -> Option<&Addr<RaftClient<KeyValueStateMachine>>> {
        self.kv_raft_groups.get(&GLOBAL_RAFT_ID)
    }

    fn send_heartbeat(&mut self, ctx: &mut Context<Self>) {
        let f = self.leader_client()
            .and_then(|client| client.heartbeat())
            .map_err(|e| warn!("Error sending heartbeat: {:?}", e))
            .into_actor(self);
        ctx.spawn(f);
    }

    fn leader_id(&self) -> impl Future<Item=u64, Error=Error> {
        self.get_leader_id_from_raft()
    }

    fn get_leader_id_from_raft(&self) -> impl Future<Item=u64, Error=Error> {
        future::result(self.global_raft().cloned().ok_or(err_msg("No leader state locally")))
            .and_then(|raft| raft.send(GetLeaderId).from_err())
    }

    fn leader_client(&self) -> impl Future<Item=RpcClient, Error=Error> {
        let peers = self.peers.clone();
        self.leader_id().and_then(move |id| {
            info!("Leader: {}", id);
            peers.read().unwrap().get(&id).cloned()
                .ok_or(err_msg("no leader available"))
        })
    }
}

impl Actor for NetworkActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(5), Self::send_heartbeat);
    }
}

impl Handler<ConnectToPeer> for NetworkActor {
    type Result = ResponseActFuture<Self, (), Error>;

    fn handle(&mut self, message: ConnectToPeer, _ctx: &mut Context<Self>) -> Self::Result {
        Box::new(
            self.connect_to_peer(&message.address)
                .into_actor(self)
                .map_err(|e, _, ctx| {
                    error!("Failed: {}", e);
                    ctx.notify_later(message, Duration::from_millis(1000));
                    e
                })
        )
    }
}

impl Handler<ServerStarted> for NetworkActor {
    type Result = ();

    fn handle(&mut self, message: ServerStarted, ctx: &mut Context<Self>) {
        self.server = Some(message.server);
        self.seeds.iter().for_each(|seed| ctx.notify(ConnectToPeer{address: seed.clone()}));
    }
}

impl Handler<RaftMessageReceived> for NetworkActor {
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, message: RaftMessageReceived, _ctx: &mut Context<Self>) -> Self::Result {
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

impl Handler<SendRaftMessage> for NetworkActor {
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, message: SendRaftMessage, ctx: &mut Context<Self>) -> Self::Result {
        if message.message.to == self.peer_id {
            self.handle(RaftMessageReceived{
                raft_group_id: message.raft_group_id,
                message: message.message,
            }, ctx)
        } else {
            let peers = self.peers.read().unwrap();
            let maybe_peer = peers.get(&message.message.to);
            if maybe_peer.is_none() {
                return Box::new(future::ok(()));
            }
            let peer = maybe_peer.unwrap();
            Box::new(peer.raft_message(&message.message))
        }
    }
}

impl Handler<CreateIndex> for NetworkActor {
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, message: CreateIndex, ctx: &mut Context<Self>) -> Self::Result {
        let f = self.leader_client().and_then(move |client| {
            client.create_index(&message.index_name)
        });
        Box::new(f)
    }
}
