use crate::clock::{Clock, HybridTimestamp};
use crate::event_emitter::EventEmitter;
use crate::proto::gossip::*;
use crate::proto::gossip_grpc::*;
use crate::proto::PeerState;
use crate::rpc_client::RpcClient;
use failure::{err_msg, format_err, Error};
use futures::prelude::*;
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
                .map_err(|_| error!("Failed to update gossip state")),
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

#[derive(Clone)]
pub enum PeerStateEvent {
    PeerJoined(u64)
}

impl GossipServer {
    pub fn new(node_id: u64, bootstrap: &[String], self_address: &str, clock: Clock) -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let state = GossipState::new(node_id, self_address, bootstrap, sender.clone(), clock);
        run_gossip_event_handler(receiver, state.new_ref(), node_id);
        GossipServer { state, sender }
    }

    pub fn build_service(&self) -> Service {
        create_gossip(self.clone())
    }

    pub fn state(&self) -> GossipState {
        self.state.clone()
    }

    pub fn update_meta_leader(&self, id: u64) -> impl Future<Item = (), Error = ()> {
        self.event(GossipEvent::MetaLeaderChanged(id))
    }

    pub fn update_node_liveness(&self, peer: PeerState) -> impl Future<Item = (), Error = ()> {
        self.event(GossipEvent::PeerUpdate(peer))
    }

    fn event(&self, event: GossipEvent) -> impl Future<Item = (), Error = ()> {
        self.sender.clone().send(event).map(|_| ()).map_err(|_| ())
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
                connect_to_client(state.upgrade(), self_id, &address);
            }
            GossipEvent::GossipReceived(data) => {
                state.upgrade().merge_gossip(data);
            }
            GossipEvent::MetaLeaderChanged(id) => {
                state.upgrade().update_meta_leader(id);
            }
            GossipEvent::PeerUpdate(peer) => {
                state.upgrade().update_node_liveness(&peer);
            }
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
    clock: Clock,
    current: GossipData,
    connections: HashMap<String, oneshot::Sender<()>>,
    clients: HashMap<String, RpcClient>,
    peers: HashMap<u64, GossipData>,
    event_publisher: mpsc::Sender<GossipEvent>,
    event_emitter: EventEmitter<PeerStateEvent>,
}

enum GossipEvent {
    GossipReceived(GossipData),
    NewPeerDiscovered(String),
    MetaLeaderChanged(u64),
    PeerUpdate(PeerState),
}

impl GossipState {
    fn new(
        node_id: u64,
        self_address: &str,
        bootstrap: &[String],
        event_publisher: mpsc::Sender<GossipEvent>,
        clock: Clock,
    ) -> Self {
        let mut current = GossipData::new();
        current.set_node_id(node_id);
        current.set_address(self_address.to_string());
        let event_emitter = EventEmitter::new(32);
        let inner = InnerGossipState {
            current,
            event_publisher,
            event_emitter,
            clock,
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

    pub fn subscribe(&self) -> mpsc::Receiver<PeerStateEvent> {
        let mut locked = self.inner.write().unwrap();
        locked.event_emitter.subscribe()
    }

    fn get_current(&self) -> GossipData {
        let locked = self.inner.read().unwrap();
        let mut gossip = locked.current.clone();
        gossip.set_updated_at(locked.clock.now().into());
        gossip
    }

    pub fn get_client(&self, node_id: u64) -> Result<RpcClient, Error> {
        self.inner.read().unwrap().get_client(node_id)
    }

    pub fn get_meta_leader_client(&self) -> Result<RpcClient, Error> {
        let locked = self.inner.read().unwrap();
        locked
            .meta_leader_id()
            .ok_or_else(|| err_msg("Leader not available"))
            .and_then(|node_id| self.get_client(node_id))
    }

    fn merge_gossip(&self, gossip: GossipData) {
        self.inner.write().unwrap().merge_gossip(gossip)
    }

    pub fn update_meta_leader(&self, node_id: u64) {
        self.inner.write().unwrap().update_meta_leader(node_id)
    }

    fn update_node_liveness(&self, peer_state: &PeerState) {
        self.inner.write().unwrap().update_node_liveness(peer_state)
    }

    fn new_ref(&self) -> GossipStateRef {
        GossipStateRef {
            inner: Arc::downgrade(&self.inner),
        }
    }

    fn update_clock(&self, peer_sent_at: HybridTimestamp) {
        self.inner
            .read()
            .unwrap()
            .clock
            .update(&peer_sent_at)
            .unwrap_or_else(|err| error!("Failed to update clock: {:?}", err));
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
        self.peers
            .get(&node_id)
            .and_then(|gossip| self.clients.get(gossip.get_address()))
            .cloned()
            .ok_or_else(|| format_err!("Not connected to '{}'", node_id))
    }

    fn add_connection(&mut self, addr: &str, sender: oneshot::Sender<()>, client: RpcClient) {
        self.connections.insert(addr.to_string(), sender);
        self.clients.insert(addr.to_string(), client);
    }

    fn merge_gossip(&mut self, gossip: GossipData) {
        let peer_id = gossip.get_node_id();

        let current_addrs = self.current.mut_peer_addresses();
        if current_addrs.get(&peer_id).is_none() {
            let address = gossip.get_address();
            current_addrs.insert(peer_id, address.to_string());
            self.publish_peer_discovered(address);
        }

        gossip
            .get_node_liveness()
            .values()
            .for_each(|peer| self.update_node_liveness(peer));
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
            return Some(self.current.meta_leader_id);
        }

        self.peers
            .values()
            .filter(|peer| peer.meta_leader_id != 0)
            .max_by_key(|peer| -> HybridTimestamp { peer.get_updated_at().into() })
            .map(|peer| peer.meta_leader_id)
    }

    fn update_node_liveness(&mut self, peer: &PeerState) {
        let peer_id = peer.get_peer().id;
        if self.current.get_node_liveness().get(&peer_id).is_none() {
            self.emit_new_live_node(peer_id)
        }
        self.current
            .mut_node_liveness()
            .insert(peer_id, peer.clone());
    }

    fn emit_new_live_node(&self, peer_id: u64) {
        self.event_emitter.emit(PeerStateEvent::PeerJoined(peer_id))
    }
}

struct ClientContext {
    state: GossipStateRef,
    client: RpcClient,
}

fn connect_to_client(state: GossipState, self_id: u64, address: &str) {
    let mut locked_state = state.inner.write().unwrap();
    if locked_state.connections.contains_key(address) {
        return;
    }

    info!("Discovered: {}", address);
    let client = RpcClient::new(self_id, address);
    let (sender, receiver) = oneshot::channel();
    locked_state.add_connection(address, sender, client.clone());
    drop(locked_state);
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
        // TODO: should age out nodes that have been failing gossip for a while
        // TODO: should have a separate heartbeat loop for tracking peer offsets
        receiver.for_each(move |_event| {
            let state = self.state.upgrade();
            let current_gossip = state.get_current();
            self.client
                .gossip(&current_gossip)
                .map(move |gossip| {
                    if gossip.get_node_id() != current_gossip.get_node_id() {
                        state.update_clock(gossip.get_updated_at().into());
                    }
                    state.merge_gossip(gossip)
                })
                .then(move |_| Ok(()))
        })
    }
}
