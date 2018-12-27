use actix::prelude::*;
use crate::key_value_state_machine::KeyValueStateMachine;
use crate::search_state_machine::SearchStateMachine;
use crate::proto::*;
use crate::raft::{
    PeerConnected,
    RaftClient,
    RaftMessageReceived,
    RaftPropose,
    StateMachineObserver,
};
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
use log::{error, info};
use protobuf::{Message, parse_from_bytes};
use raft::eraftpb;
use std::boxed::Box;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
        let request = self.network.send(PeerConnected{
            is_master: req.is_master,
            peer_id: req.peer_id,
        }).from_err().and_then(|f| f).map(|_| resp);
        future_to_sink(request, ctx, sink);
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
        }).from_err().and_then(|f| f).map(|_| EmptyResponse::new());
        future_to_sink(f, ctx, sink);
    }

    fn set(
        &mut self,
        ctx: RpcContext,
        req: KeyValue,
        sink: UnarySink<EmptyResponse>,
    ) {
        info!("Set()");
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
    let f = network.send(proposal).from_err().and_then(|r| r).and_then(|_| receiver.from_err());
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
    peers: Arc<RwLock<HashMap<u64, InternalClient>>>,
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

fn grpc_timeout(n: u64) -> CallOption {
    CallOption::default().timeout(Duration::from_secs(n))
}

const GLOBAL_RAFT_ID: u64 = 0;

impl NetworkActor {
    fn new(
        peer_id: u64,
        peers: &[String],
        raft: Addr<RaftClient<KeyValueStateMachine>>,
    ) -> Self {
        let mut kv_raft_groups = HashMap::new();
        kv_raft_groups.insert(GLOBAL_RAFT_ID, raft);
        Self{
            peer_id: peer_id,
            seeds: peers.to_vec(),
            peers: Arc::new(RwLock::new(HashMap::new())),
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
        let addr = Self::new(peer_id, peers, raft).start();
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
        let env = Arc::new(EnvBuilder::new().build());
        let channel = ChannelBuilder::new(env).connect(peer_address);
        let client = InternalClient::new(channel);
        let mut request = HelloRequest::new();
        request.peer_id = self.peer_id;
        future::result(client.hello_async_opt(&request, grpc_timeout(3)))
            .and_then(|f| f)
            .map(move |response| {
                peers.write().unwrap().insert(response.peer_id, client);
                info!("Connected to peer id '{}'", response.peer_id);
            })
            .map_err(|e| e.into())
    }

    fn global_raft(&self) -> Option<&Addr<RaftClient<KeyValueStateMachine>>> {
        self.kv_raft_groups.get(&GLOBAL_RAFT_ID)
    }
}

impl Actor for NetworkActor {
    type Context = Context<Self>;
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
        let f = raft.unwrap()
            .send(message)
            .map_err(|e| e.into())
            .and_then(|r| r);
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
            return Box::new(future::ok(()))
        }
        let global = maybe_global.unwrap();
        let f = global.send(message).from_err().and_then(|r| r);
        Box::new(f)
    }
}

impl Handler<PeerConnected> for NetworkActor {
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, message: PeerConnected, _ctx: &mut Context<Self>) -> Self::Result {
        let maybe_global = self.global_raft();
        if maybe_global.is_none() {
            return Box::new(future::ok(()))
        }
        let f = maybe_global.unwrap()
            .send(message)
            .from_err()
            .and_then(|r| r);
        Box::new(f)
    }
}

impl Handler<SendRaftMessage> for NetworkActor {
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, message: SendRaftMessage, ctx: &mut Context<Self>) -> Self::Result {
        let mut out = SearchifyRaftMessage::new();
        out.wrapped_message = message.message.write_to_bytes().unwrap();
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
            let f = peer.raft_message_async_opt(&out, grpc_timeout(3))
                .unwrap()
                .from_err()
                .map(|_| ());
            Box::new(f)
        }
    }
}
