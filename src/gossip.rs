use crate::proto::gossip::*;
use crate::proto::gossip_grpc::*;
use crate::rpc_client::RpcClient;
use failure::{format_err, Error};
use futures::prelude::*;
use futures::stream;
use futures::sync::{mpsc, oneshot};
use grpcio::{RpcContext, Service, UnarySink};
use log::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};
use std::time::{Duration, Instant};
use tokio::timer::Interval;

#[derive(Clone)]
pub struct GossipServer {
    state: Arc<RwLock<GossipState>>,
    sender: mpsc::Sender<GossipEvent>,
}

impl Gossip for GossipServer {
    fn exchange(&mut self, ctx: RpcContext, req: GossipData, sink: UnarySink<GossipData>) {
        ctx.spawn(
            self.sender
                .clone()
                .send(GossipEvent::GossipReceived(req))
                .map(|_| ())
                .map_err(|_| ()),
        );
        let state = self.state.read().unwrap();
        let out = state.current.clone();
        ctx.spawn(
            sink.success(out)
                .map_err(|err| error!("Error exhanging gossip: {:?}", err)),
        );
    }
}

#[derive(Eq, PartialEq)]
enum ClientEvent {
    GossipTick,
    Done,
}

impl GossipServer {
    pub fn build_service(node_id: u64, bootstrap: &[String], self_address: &str) -> Service {
        let (sender, receiver) = mpsc::channel(32);
        let state = Arc::new(RwLock::new(GossipState::new(
            node_id,
            self_address,
            bootstrap,
            sender.clone(),
        )));
        run_gossip_event_handler(receiver, Arc::downgrade(&state), node_id);
        let server = GossipServer { state, sender };
        create_gossip(server)
    }

    fn node_id(&self) -> u64 {
        self.state.read().unwrap().node_id()
    }
}

fn run_gossip_event_handler(
    receiver: mpsc::Receiver<GossipEvent>,
    state: Weak<RwLock<GossipState>>,
    self_id: u64,
) {
    let f = receiver.for_each(move |event| {
        match event {
            GossipEvent::NewPeerDiscovered(address) => {
                info!("Discovered: {}", address);
                connect_to_client(state.upgrade().unwrap(), self_id, &address);
            }
            GossipEvent::GossipReceived(data) => {
                let upgraded = state.upgrade().unwrap();
                let mut locked = upgraded.write().unwrap();
                locked.merge_gossip(data);
            }
        };
        Ok(())
    });
    tokio::spawn(f);
}

struct GossipState {
    bootstrap: Vec<String>,
    current: GossipData,
    connections: HashMap<String, oneshot::Sender<()>>,
    clients: HashMap<u64, RpcClient>,
    peers: HashMap<u64, GossipData>,
    event_publisher: mpsc::Sender<GossipEvent>,
}

enum GossipEvent {
    GossipReceived(GossipData),
    NewPeerDiscovered(String),
}

impl GossipState {
    fn new(
        node_id: u64,
        self_address: &str,
        bootstrap: &[String],
        event_publisher: mpsc::Sender<GossipEvent>,
    ) -> Self {
        let mut current = GossipData::new();
        current.set_node_id(node_id);
        current.set_address(self_address.to_string());
        let state = Self {
            current,
            event_publisher,
            bootstrap: bootstrap.to_vec(),
            connections: HashMap::new(),
            clients: HashMap::new(),
            peers: HashMap::new(),
        };
        bootstrap
            .iter()
            .for_each(|address| state.publish_peer_discovered(address));
        state
    }

    fn node_id(&self) -> u64 {
        self.current.get_node_id()
    }

    fn remove_connection(&mut self, addr: &str) {
        self.connections.remove(addr);
    }

    fn add_connection(&mut self, addr: &str, sender: oneshot::Sender<()>) {
        self.connections.insert(addr.to_string(), sender);
    }

    fn merge_gossip(&mut self, gossip: GossipData) {
        let peer_id = gossip.get_node_id();
        self.current
            .mut_peer_addresses()
            .entry(peer_id)
            .or_insert(gossip.get_address().to_string());
        gossip
            .get_peer_addresses()
            .iter()
            .filter(|(id, _)| !self.peers.contains_key(id) && **id != self.node_id())
            .for_each(|(_, address)| self.publish_peer_discovered(address));

        self.peers.insert(peer_id, gossip);
    }

    fn publish_event(&self, event: GossipEvent) {
        let f = self.event_publisher.clone().send(event);
        tokio::spawn(f.map(|_| ()).map_err(|_| ()));
    }

    fn publish_peer_discovered(&self, address: &str) {
        self.publish_event(GossipEvent::NewPeerDiscovered(address.to_string()));
    }
}

struct ClientContext {
    state: Weak<RwLock<GossipState>>,
    client: RpcClient,
}

fn connect_to_client(state: Arc<RwLock<GossipState>>, self_id: u64, address: &str) {
    let client = RpcClient::new(self_id, address);
    let (sender, receiver) = oneshot::channel();
    state.write().unwrap().add_connection(address, sender);
    let gossip_stream = Interval::new(Instant::now(), Duration::from_secs(5))
        .map(|_| ClientEvent::GossipTick)
        .map_err(|err| error!("Error in gossip tick: {:?}", err));
    let close_stream = receiver
        .into_stream()
        .map(|_: ()| ClientEvent::Done)
        .map_err(|_| ());
    let (sender, receiver) = mpsc::channel(64);
    let producer = gossip_stream
        .select(close_stream)
        .take_while(|item| Ok(*item != ClientEvent::Done))
        .for_each(move |event| sender.clone().send(event).map_err(|_| ()).map(|_| ()));
    let consumer = ClientContext::new(Arc::downgrade(&state), client).run(receiver);
    tokio::spawn(consumer);
    tokio::spawn(producer);
}

impl ClientContext {
    pub fn new(state: Weak<RwLock<GossipState>>, client: RpcClient) -> Self {
        Self { state, client }
    }

    pub fn run(self, receiver: mpsc::Receiver<ClientEvent>) -> impl Future<Item = (), Error = ()> {
        receiver.for_each(move |_event| {
            let state = self.state.upgrade().unwrap();
            let current_gossip = {
                let locked_state = state.read().unwrap();
                locked_state.current.clone()
            };
            self.client
                .gossip(&current_gossip)
                .map(move |gossip| {
                    state.write().unwrap().merge_gossip(gossip);
                })
                .then(|_| Ok(()))
        })
    }
}
