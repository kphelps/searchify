use actix::prelude::*;
use crate::network::{
    NetworkActor,
    SendRaftMessage,
};
use failure::Error;
use futures::Future;
use log::{error, info};
use protobuf::parse_from_bytes;
use rand;
use raft::{
    self,
    Config,
    eraftpb,
    raw_node::{
        Peer,
        RawNode,
        Ready,
    },
    storage::MemStorage,
};
use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tokio_async_await::compat::backward::Compat;

struct RaftState {
    network: Option<Addr<NetworkActor>>,
    raft_node: RawNode<MemStorage>,
    node_id: u64,
}

pub struct RaftClient {
    tick_interval: Duration,
    state: Rc<RefCell<RaftState>>,
}

#[derive(Message)]
#[rtype(result="Result<(), Error>")]
struct TickMessage;

#[derive(Message)]
#[rtype(result="Result<(), Error>")]
struct ProposeMessage {
    id: u8,
}

#[derive(Message)]
#[rtype(result="Result<(), Error>")]
pub struct RaftMessageReceived(pub eraftpb::Message);

#[derive(Message)]
#[rtype(result="Result<(), Error>")]
pub struct PeerConnected {
    pub peer_id: u64,
}

#[derive(Message)]
pub struct InitNetwork(pub Addr<NetworkActor>);

impl RaftClient {
    pub fn new(node_id: u64) -> Result<Self, Error> {
        let state = RaftState::new(node_id)?;
        Ok(Self {
            tick_interval: Duration::from_millis(100),
            state: Rc::new(RefCell::new(state)),
        })
    }

    fn init_network(&self, network: Addr<NetworkActor>) {
        self.state.borrow_mut().network = Some(network);
    }

    fn raft_tick(&self) -> impl Future<Item=(), Error=Error> {
        Compat::new(RaftState::raft_tick(self.state.clone()))
    }

    fn raft_propose(&self, context: Vec<u8>, data: Vec<u8>) -> Result<(), Error> {
        self.state.borrow_mut().raft_node.propose(context, data).map_err(|e| e.into())
    }

    fn raft_step(&self, message: eraftpb::Message) -> Result<(), Error> {
        self.state.borrow_mut().raft_node.step(message).map_err(|e| e.into())
    }

    fn raft_peer_connected(&self, id: u64) -> Result<(), Error> {
        let mut conf_change = eraftpb::ConfChange::new();
        conf_change.set_id(self.state.borrow().raft_node.raft.id);
        conf_change.set_change_type(eraftpb::ConfChangeType::AddNode);
        conf_change.set_node_id(id);
        self.state.borrow_mut()
            .raft_node
            .propose_conf_change(vec![], conf_change)
            .map_err(|e| e.into())
    }

    fn schedule_next_tick(&self, ctx: &mut Context<Self>) {
        ctx.notify_later(TickMessage, self.tick_interval);
    }
}

impl RaftState {

    pub fn new(node_id: u64) -> Result<Self, Error> {
        let config = Config {
            id: node_id,
            peers: vec![node_id],
            heartbeat_tick: 3,
            election_tick: 10,
            ..Default::default()
        };
        let storage = MemStorage::new();
        config.validate()?;
        let peers: Vec<Peer> = vec![1u64, 2, 3]
            .into_iter()
            .filter(|n| *n != node_id)
            .map(|n| Peer{id: n, context: None})
            .collect();
        let node = RawNode::new(&config, storage, peers)?;
        Ok(Self {
            network: None,
            raft_node: node,
            node_id: node_id,
        })
    }

    async fn raft_tick(locked: Rc<RefCell<RaftState>>) -> Result<(), Error> {
        let mut state = locked.borrow_mut();
        state.raft_node.tick();
        if !state.raft_node.has_ready() {
            return Ok(());
        }

        let mut ready = state.raft_node.ready();
        if state.is_leader() {
            state.send_messages(&mut ready);
        }
        state.apply_snapshot(&ready)?;
        state.append_entries(&ready)?;
        state.apply_hardstate(&ready);
        if !state.is_leader() {
            state.send_messages(&mut ready);
        }
        state.apply_committed_entries(&ready)?;
        state.advance_raft(ready);
        let _ = state.compact();
        Ok(())
    }

    fn is_leader(&self) -> bool {
        self.raft_node.raft.leader_id == self.node_id
    }

    fn apply_snapshot(&mut self, ready: &Ready) -> Result<(), Error> {
        if !raft::is_empty_snap(&ready.snapshot) {
            self.raft_node
                .mut_store()
                .wl()
                .apply_snapshot(ready.snapshot.clone())?
        }
        Ok(())
    }

    fn append_entries(&mut self, ready: &Ready) -> Result<(), Error> {
        self.raft_node
            .mut_store()
            .wl()
            .append(&ready.entries)
            .map_err(|e| e.into())
    }

    fn apply_hardstate(&mut self, ready: &Ready) {
        if let Some(ref hardstate) = ready.hs {
            self.raft_node
                .mut_store()
                .wl()
                .set_hardstate(hardstate.clone());
        }
    }

    fn send_messages(&self, ready: &mut Ready) {
        for message in ready.messages.drain(..) {
            let f = self.network.as_ref().unwrap()
                .send(SendRaftMessage{message: message})
                .map(|_| ())
                .map_err(|e| error!("Error sending message: {}", e));
            Arbiter::spawn(f);
        }
    }

    fn apply_committed_entries(&mut self, ready: &Ready) -> Result<(), Error> {
        if let Some(ref committed_entries) = ready.committed_entries {
            let mut last_apply_index = 0;
            let mut conf_state = None;
            for entry in committed_entries {
                info!("Entry: {:?}", entry);
                last_apply_index = entry.get_index();

                if entry.get_data().is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }

                match entry.get_entry_type() {
                    eraftpb::EntryType::EntryNormal => self.handle_normal_entry(entry),
                    eraftpb::EntryType::EntryConfChange => {
                        conf_state = Some(self.handle_conf_change_entry(entry))
                    },
                }
            }
            if last_apply_index > 0 {
                let mut mem = self.raft_node.mut_store().wl();
                mem.create_snapshot(last_apply_index, conf_state, vec![])?;
            }
        }
        Ok(())
    }

    fn handle_normal_entry(&mut self, _entry: &eraftpb::Entry) {
    }

    fn handle_conf_change_entry(&mut self, entry: &eraftpb::Entry) -> eraftpb::ConfState {
        let cc = parse_from_bytes::<eraftpb::ConfChange>(entry.get_data()).expect("Valid protobuf");

        match cc.get_change_type() {
            eraftpb::ConfChangeType::AddNode => {
                // self.node.mut_store().wl().add_node(peer);
            }
            eraftpb::ConfChangeType::RemoveNode => {
                // self.node.mut_store().wl().remove_node(cc.node_id);
            }
            _ => (), // no learners right now
        }


        // TODO: callbacks?
        self.raft_node.apply_conf_change(&cc)
    }

    fn advance_raft(&mut self, ready: Ready) {
        self.raft_node.advance(ready);
    }

    fn compact(&mut self) -> Result<(), Error> {
        let raft_applied = self.raft_node.raft.raft_log.get_applied();
        Ok(self.raft_node.mut_store().wl().compact(raft_applied)?)
    }
}

impl Actor for RaftClient {
    type Context = Context<Self>;
}

impl Handler<InitNetwork> for RaftClient {
    type Result = ();

    fn handle(&mut self, message: InitNetwork, ctx: &mut Context<Self>) -> Self::Result {
        self.init_network(message.0);
        self.schedule_next_tick(ctx);
    }
}

impl Handler<TickMessage> for RaftClient {
    type Result = ResponseActFuture<Self, (), Error>;

    fn handle(&mut self, _: TickMessage, _: &mut Context<Self>) -> Self::Result {
        let f = self.raft_tick()
            .into_actor(self)
            .map(|_, actor, ctx| actor.schedule_next_tick(ctx))
            .map_err(|err, actor, ctx| {
                error!("Error ticking: {}", err);
                actor.schedule_next_tick(ctx);
                err
            });
        Box::new(f)
    }
}

impl Handler<ProposeMessage> for RaftClient {
    type Result = Result<(), Error>;

    fn handle(&mut self, message: ProposeMessage, _ctx: &mut Context<Self>)
        -> Self::Result
    {
        self.raft_propose(vec![], vec![message.id])
            .map_err(|e| e.into())
    }
}

impl Handler<RaftMessageReceived> for RaftClient {
    type Result = Result<(), Error>;

    fn handle(&mut self, message: RaftMessageReceived, _ctx: &mut Context<Self>)
        -> Self::Result
    {
        self.raft_step(message.0)
    }
}

impl Handler<PeerConnected> for RaftClient  {
    type Result = Result<(), Error>;

    fn handle(&mut self, message: PeerConnected, _ctx: &mut Context<Self>) -> Self::Result {
        self.raft_peer_connected(message.peer_id)
    }
}
