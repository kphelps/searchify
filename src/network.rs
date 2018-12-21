use actix::prelude::*;
use crate::proto::*;
use crate::raft::{
    PeerConnected,
    RaftClient,
    RaftMessageReceived,
};
use failure::Error;
use futures::{
    future,
    prelude::*,
};
use grpcio::{
    CallOption,
    ChannelBuilder,
    EnvBuilder,
    Environment,
    RpcContext,
    Server,
    ServerBuilder,
    UnarySink,
};
use log::{error, info};
use protobuf::{Message, parse_from_bytes};
use raft::eraftpb;
use std::boxed::Box;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct InternalServer{
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
        let request = self.network.send(PeerConnected{
            is_master: req.is_master,
            peer_id: req.peer_id,
        });
        Arbiter::spawn(request.map(|_| ()).map_err(|err| error!("{}", err)));
        let mut resp = HelloResponse::new();
        resp.peer_id = self.peer_id;
        let f = sink.success(resp).map_err(|_| ());
        ctx.spawn(f);
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
        });
        Arbiter::spawn(f.map(|_| ()).map_err(|e| error!("{}", e)));
        ctx.spawn(sink.success(EmptyResponse::new()).map_err(|_| ()));
    }
}

pub struct NetworkActor {
    peer_id: u64,
    seeds: Vec<String>,
    peers: Rc<RefCell<HashMap<u64, InternalClient>>>,
    server: Option<Server>,
    raft_groups: HashMap<u64, Addr<RaftClient>>,
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
        raft: Addr<RaftClient>,
    ) -> Self {
        let mut raft_groups = HashMap::new();
        raft_groups.insert(GLOBAL_RAFT_ID, raft);
        Self{
            peer_id: peer_id,
            seeds: peers.to_vec(),
            peers: Rc::new(RefCell::new(HashMap::new())),
            server: None,
            raft_groups,
        }
    }

    pub fn start(
        peer_id: u64,
        port: u16,
        peers: &[String],
        raft: Addr<RaftClient>,
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
                peers.borrow_mut().insert(response.peer_id, client);
                info!("Connected to peer id '{}'", response.peer_id);
            })
            .map_err(|e| e.into())
    }

    fn global_raft(&self) -> Option<&Addr<RaftClient>> {
        self.raft_groups.get(&GLOBAL_RAFT_ID)
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
        let raft = self.raft_groups.get(&message.raft_group_id);
        if raft.is_none() {
            error!("Unknown raft group: {}", message.raft_group_id);
            return Box::new(future::ok(()))
        }
        let f = raft.unwrap()
            .send(message)
            .map_err(|e| e.into())
            .and_then(|r| r);
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
            .map_err(|e| e.into())
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
            let peers = self.peers.borrow();
            let maybe_peer = peers.get(&message.message.to);
            if maybe_peer.is_none() {
                return Box::new(future::ok(()));
            }
            let peer = maybe_peer.unwrap();
            let f = peer.raft_message_async_opt(&out, grpc_timeout(3))
                .unwrap()
                .map_err(|e| e.into())
                .map(|_| ());
            Box::new(f)
        }
    }
}
