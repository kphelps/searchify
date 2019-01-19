use crate::proto::gossip::*;
use crate::proto::gossip_grpc::*;
use crate::rpc_client::RpcClient;
use failure::{format_err, err_msg, Error};
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
    state: GossipState,
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
        let out = self.state.get_current();
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
    pub fn new(node_id: u64, bootstrap: &[String], self_address: &str) -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let state = GossipState::new(
            node_id,
            self_address,
            bootstrap,
            sender.clone(),
        );
        run_gossip_event_handler(receiver, state.new_ref(), node_id);
        GossipServer { state, sender }
    }

    pub fn build_service(&self) -> Service {
        create_gossip(self.clone())
    }

    pub fn state(&self) -> GossipState {
        self.state.clone()
    }

    fn node_id(&self) -> u64 {
        self.state.node_id()
    }

    pub fn update_meta_leader(&self, id: u64) -> impl Future<Item = (), Error = ()> {
        self.sender.clone()
            .send(GossipEvent::MetaLeaderChanged(id))
            .map(|_| ())
            .map_err(|_| ())
    }
}

fn run_gossip_event_handler(
    receiver: mpsc::Receiver<GossipEvent>,
    state: GossipStateRef,
    self_id: u64,
) {
    let f = receiver.for_each(move |event| {
        match event {
            GossipEvent::NewPeerDiscovered(address) => {
                info!("Discovered: {}", address);
                connect_to_client(state.upgrade(), self_id, &address);
            },
            GossipEvent::GossipReceived(data) => {
                state.upgrade().merge_gossip(data);
            },
            GossipEvent::MetaLeaderChanged(id) => {
                state.upgrade().update_meta_leader(id);
            },
        };
        Ok(())
    });
    tokio::spawn(f);
}

#[derive(Clone)]
pub struct GossipState {
    inner: Arc<RwLock<InnerGossipState>>,
}

#[derive(Clone)]
pub struct GossipStateRef {
    inner: Weak<RwLock<InnerGossipState>>,
}

struct InnerGossipState {
    bootstrap: Vec<String>,
    current: GossipData,
    connections: HashMap<String, oneshot::Sender<()>>,
    clients: HashMap<String, RpcClient>,
    peers: HashMap<u64, GossipData>,
    event_publisher: mpsc::Sender<GossipEvent>,
}

enum GossipEvent {
    GossipReceived(GossipData),
    NewPeerDiscovered(String),
    MetaLeaderChanged(u64),
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
        let inner = InnerGossipState {
            current,
            event_publisher,
            bootstrap: bootstrap.to_vec(),
            connections: HashMap::new(),
            clients: HashMap::new(),
            peers: HashMap::new(),
        };
        inner.publish_peer_discovered(self_address);
        bootstrap
            .iter()
            .for_each(|address| inner.publish_peer_discovered(address));
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    fn get_current(&self) -> GossipData {
        self.inner.read().unwrap().current.clone()
    }

    fn node_id(&self) -> u64 {
        self.inner.read().unwrap().node_id()
    }

    fn remove_connection(&self, addr: &str) {
        self.inner.write().unwrap().remove_connection(addr)
    }

    pub fn get_client(&self, node_id: u64) -> Result<RpcClient, Error> {
        self.inner.read().unwrap().get_client(node_id)
    }

    pub fn get_meta_leader_client(&self) -> Result<RpcClient, Error> {
        let locked = self.inner.read().unwrap();
        locked.meta_leader_id()
            .ok_or_else(|| err_msg("Leader not available"))
            .and_then(|node_id| self.get_client(node_id))
    }

    fn add_connection(
        &self,
        addr: &str,
        sender: oneshot::Sender<()>,
        rpc_client: RpcClient,
    ) {
        self.inner.write().unwrap().add_connection(addr, sender, rpc_client)
    }

    fn merge_gossip(&self, gossip: GossipData) {
        self.inner.write().unwrap().merge_gossip(gossip)
    }

    pub fn update_meta_leader(&self, node_id: u64) {
        self.inner.write().unwrap().update_meta_leader(node_id)
    }

    fn new_ref(&self) -> GossipStateRef {
        GossipStateRef {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl GossipStateRef {
    fn upgrade(&self) -> GossipState {
        GossipState {
            inner: self.inner.upgrade().unwrap(),
        }
    }
}

impl InnerGossipState {
    fn get_client(&self, node_id: u64) -> Result<RpcClient, Error> {
        self.peers.get(&node_id)
            .and_then(|gossip| self.clients.get(gossip.get_address()))
            .cloned()
            .ok_or_else(|| format_err!("Not connected to '{}'", node_id))
    }

    fn node_id(&self) -> u64 {
        self.current.get_node_id()
    }

    fn remove_connection(&mut self, addr: &str) {
        self.connections.remove(addr);
        self.clients.remove(addr);
    }

    fn add_connection(
        &mut self,
        addr: &str,
        sender: oneshot::Sender<()>,
        client: RpcClient,
    ) {
        self.connections.insert(addr.to_string(), sender);
        self.clients.insert(addr.to_string(), client);
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
            .filter(|(id, _)| !self.peers.contains_key(id))
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

    fn update_meta_leader(&mut self, node_id: u64) {
        self.current.set_meta_leader_id(node_id);
    }

    fn meta_leader_id(&self) -> Option<u64> {
        if self.current.meta_leader_id != 0 {
            return Some(self.current.meta_leader_id)
        }

        self.peers.values()
            .find(|peer| peer.meta_leader_id != 0)
            .map(|peer| peer.meta_leader_id)
    }
}

struct ClientContext {
    state: GossipStateRef,
    client: RpcClient,
}

fn connect_to_client(state: GossipState, self_id: u64, address: &str) {
    let client = RpcClient::new(self_id, address);
    let (sender, receiver) = oneshot::channel();
    state.add_connection(address, sender, client.clone());
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
    let consumer = ClientContext::new(state.new_ref(), client).run(receiver);
    tokio::spawn(consumer);
    tokio::spawn(producer);
}

impl ClientContext {
    pub fn new(state: GossipStateRef, client: RpcClient) -> Self {
        Self { state, client }
    }

    pub fn run(self, receiver: mpsc::Receiver<ClientEvent>) -> impl Future<Item = (), Error = ()> {
        receiver.for_each(move |_event| {
            let state = self.state.upgrade();
            let current_gossip = state.get_current();
            self.client
                .gossip(&current_gossip)
                .map(move |gossip| state.merge_gossip(gossip))
                .then(|_| Ok(()))
        })
    }
}
