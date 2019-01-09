use crate::network::NetworkActor;
use crate::node_router::NodeRouterHandle;
use crate::proto::EntryContext;
use crate::raft_router::RaftRouter;
use crate::raft_storage::RaftStorage;
use crate::storage_engine::MessageWriteBatch;
use actix::prelude::*;
use failure::Error;
use futures::prelude::*;
use futures::sync::oneshot::Sender;
use log::*;
use protobuf::{parse_from_bytes, Message};
use raft::{
    self, eraftpb,
    raw_node::{RawNode, Ready},
    storage::Storage,
    Config,
};
use std::boxed::Box;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

struct RaftState<T> {
    state_machine: T,
    node_router: NodeRouterHandle,
    raft_node: RawNode<RaftStorage>,
    node_id: u64,
    raft_group_id: u64,
    leader_id: u64,
    observers: HashMap<u64, Box<StateMachineObserver<T> + Send + Sync + 'static>>,
}

pub struct RaftClient<T>
where
    T: RaftStateMachine + 'static,
{
    tick_interval: Duration,
    state: Rc<RefCell<RaftState<T>>>,
    network: Addr<NetworkActor>,
    raft_router: RaftRouter<T>,
}

#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
struct TickMessage;

pub trait StateMachineObserver<S> {
    fn observe(self: Box<Self>, state_machine: &S);
}

pub struct FutureStateMachineObserver<T, F> {
    sender: Sender<T>,
    observe_impl: F,
}

impl<T, F> FutureStateMachineObserver<T, F> {
    pub fn new(sender: Sender<T>, observe: F) -> Self {
        Self {
            sender,
            observe_impl: observe,
        }
    }
}

impl<T, F, S> StateMachineObserver<S> for FutureStateMachineObserver<T, F>
where
    F: FnOnce(&S) -> T,
{
    fn observe(self: Box<Self>, state_machine: &S) {
        let result = (self.observe_impl)(state_machine);
        if let Err(_) = self.sender.send(result) {
            error!("Failed to observe state machine");
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
pub struct RaftPropose<S>
where
    S: RaftStateMachine,
{
    pub raft_group_id: u64,
    pub entry: <S as RaftStateMachine>::EntryType,
    pub observer: Box<dyn StateMachineObserver<S> + Send + Sync>,
}

impl<S> RaftPropose<S>
where
    S: RaftStateMachine,
{
    pub fn new(
        entry: S::EntryType,
        observer: impl StateMachineObserver<S> + Send + Sync + 'static,
    ) -> Self {
        Self::new_for_group(0, entry, observer)
    }

    pub fn new_for_group(
        raft_group_id: u64,
        entry: S::EntryType,
        observer: impl StateMachineObserver<S> + Send + Sync + 'static,
    ) -> Self {
        Self {
            raft_group_id,
            entry,
            observer: Box::new(observer),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
pub struct RaftMessageReceived {
    pub raft_group_id: u64,
    pub message: eraftpb::Message,
}

#[derive(Message)]
pub struct InitNetwork(pub Addr<NetworkActor>);

pub trait RaftStateMachine {
    type EntryType: Message;

    fn apply(&mut self, entry: Self::EntryType) -> Result<(), Error>;
}

pub trait RaftEntryHandler<T> {
    type Result;

    fn handle(&mut self, entry: T) -> Self::Result;
}

impl<T> RaftClient<T>
where
    T: RaftStateMachine + 'static,
{
    pub fn new(
        node_id: u64,
        storage: RaftStorage,
        state_machine: T,
        node_router: NodeRouterHandle,
        network: &Addr<NetworkActor>,
        raft_router: &RaftRouter<T>,
    ) -> Result<Self, Error> {
        let state = RaftState::new(node_id, storage, state_machine, node_router)?;
        Ok(Self {
            tick_interval: Duration::from_millis(100),
            state: Rc::new(RefCell::new(state)),
            network: network.clone(),
            raft_router: raft_router.clone(),
        })
    }

    fn raft_tick(&self) -> Result<(), Error> {
        self.state.borrow_mut().raft_tick()
    }

    fn raft_propose_entry(&self, m: RaftPropose<T>) -> Result<(), Error> {
        self.state.borrow_mut().propose_entry(m)
    }

    fn raft_step(&self, message: eraftpb::Message) -> Result<(), Error> {
        self.state
            .borrow_mut()
            .raft_node
            .step(message)
            .map_err(|e| e.into())
    }

    fn schedule_next_tick(&self, ctx: &mut Context<Self>) {
        ctx.notify_later(TickMessage, self.tick_interval);
    }
}

impl<T> RaftState<T>
where
    T: RaftStateMachine + 'static,
{
    pub fn new(
        node_id: u64,
        storage: RaftStorage,
        state_machine: T,
        node_router: NodeRouterHandle,
    ) -> Result<Self, Error> {
        let raft_group_id = storage.raft_group_id();
        let config = Config {
            id: node_id,
            peers: vec![],
            heartbeat_tick: 3,
            election_tick: 10,
            applied: storage.last_applied_index(),
            tag: format!("[group-{}]", raft_group_id),
            ..Default::default()
        };
        config.validate()?;
        let initial_state = storage.initial_state()?;
        let mut node = RawNode::new(&config, storage, vec![])?;

        let peers = initial_state.conf_state.get_nodes();
        if peers.len() == 1 && peers[0] == node_id {
            node.campaign()?;
        }

        Ok(Self {
            raft_node: node,
            observers: HashMap::new(),
            leader_id: 0,
            node_id,
            raft_group_id,
            state_machine,
            node_router,
        })
    }

    fn raft_tick(&mut self) -> Result<(), Error> {
        self.raft_node.tick();
        if !self.raft_node.has_ready() {
            return Ok(());
        }

        let mut ready = self.raft_node.ready();
        if self.is_leader() {
            self.send_messages(&mut ready);
        }
        let mut batch = self.raft_node.mut_store().batch();
        self.apply_snapshot(&ready)?;
        self.append_entries(&ready, &mut batch)?;
        self.apply_hardstate(&ready, &mut batch)?;
        if !self.is_leader() {
            self.send_messages(&mut ready);
        }
        self.apply_committed_entries(&ready, &mut batch)?;
        batch.commit()?;
        self.advance_raft(ready);
        let _ = self.compact();
        self.update_leader_id();
        Ok(())
    }

    fn update_leader_id(&mut self) {
        if self.leader_id != self.raft_node.raft.leader_id {
            self.leader_id = self.raft_node.raft.leader_id;
            self.node_router.set_leader_id(self.leader_id);
        }
    }

    fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<(), Error> {
        debug!("Propose({}, {})", context.len(), data.len());
        self.raft_node.propose(context, data).map_err(|e| e.into())
    }

    fn propose_entry(&mut self, m: RaftPropose<T>) -> Result<(), Error> {
        let data = m.entry.write_to_bytes()?;
        let id = rand::random::<u64>();
        let mut ctx = EntryContext::new();
        ctx.id = id;
        let ctx_bytes = ctx.write_to_bytes()?;
        self.observers.insert(id, m.observer);
        let result = self.propose(ctx_bytes, data);
        if let Err(_) = result {
            self.observers.remove(&id);
        }
        result
    }

    fn is_leader(&self) -> bool {
        self.raft_node.raft.leader_id == self.node_id
    }

    fn apply_snapshot(&mut self, ready: &Ready) -> Result<(), Error> {
        if !raft::is_empty_snap(&ready.snapshot) {
            self.raft_node
                .mut_store()
                .apply_snapshot(ready.snapshot.clone())?
        }
        Ok(())
    }

    fn append_entries(
        &mut self,
        ready: &Ready,
        batch: &mut MessageWriteBatch,
    ) -> Result<(), Error> {
        self.raft_node.mut_store().append(&ready.entries, batch)
    }

    fn apply_hardstate(
        &mut self,
        ready: &Ready,
        batch: &mut MessageWriteBatch,
    ) -> Result<(), Error> {
        if let Some(ref hardstate) = ready.hs {
            self.raft_node.mut_store().set_hardstate(hardstate, batch)?;
        }
        Ok(())
    }

    fn send_messages(&self, ready: &mut Ready) {
        for message in ready.messages.drain(..) {
            let f = self
                .node_router
                .route_raft_message(message, self.raft_group_id)
                .map_err(|e| debug!("Error sending raft message: {}", e));
            Arbiter::spawn(f);
        }
    }

    fn apply_committed_entries(
        &mut self,
        ready: &Ready,
        batch: &mut MessageWriteBatch,
    ) -> Result<(), Error> {
        if let Some(ref committed_entries) = ready.committed_entries {
            let mut last_apply_index = 0;
            let mut conf_state = None;
            for entry in committed_entries {
                last_apply_index = entry.get_index();

                if entry.get_data().is_empty() && entry.get_context().is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }

                match entry.get_entry_type() {
                    eraftpb::EntryType::EntryNormal => self.handle_normal_entry(entry)?,
                    eraftpb::EntryType::EntryConfChange => {
                        conf_state = Some(self.handle_conf_change_entry(entry))
                    }
                }
            }
            if last_apply_index > 0 {
                self.raft_node
                    .mut_store()
                    .create_snapshot(last_apply_index, conf_state, vec![])?;
                self.raft_node
                    .mut_store()
                    .update_apply_index(last_apply_index);
                self.raft_node.mut_store().persist_apply_state(batch)?;
            }
        }
        Ok(())
    }

    fn handle_normal_entry(&mut self, entry: &eraftpb::Entry) -> Result<(), Error> {
        debug!("NormalEntry: {:?}", entry);
        let ctx = parse_from_bytes::<EntryContext>(&entry.context)?;
        let parsed = parse_from_bytes::<T::EntryType>(&entry.data)?;
        let apply_result = self.state_machine.apply(parsed);
        if let Err(err) = apply_result {
            let parsed = parse_from_bytes::<T::EntryType>(&entry.data)?;
            warn!("Failed to apply '{:?}': {}", parsed, err);
        }
        if let Some(observer) = self.observers.remove(&ctx.id) {
            observer.observe(&self.state_machine);
        }
        Ok(())
    }

    fn handle_conf_change_entry(&mut self, entry: &eraftpb::Entry) -> eraftpb::ConfState {
        let cc = parse_from_bytes::<eraftpb::ConfChange>(entry.get_data()).expect("Valid protobuf");

        match cc.get_change_type() {
            eraftpb::ConfChangeType::AddNode => {
                // self.node.mut_store().add_node(peer);
            }
            eraftpb::ConfChangeType::RemoveNode => {
                // self.node.mut_store().remove_node(cc.node_id);
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
        self.raft_node.mut_store().compact(raft_applied)
    }
}

impl<T> Actor for RaftClient<T>
where
    T: RaftStateMachine + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let f = self
            .raft_router
            .register(self.state.borrow().raft_group_id, ctx.address())
            .map_err(|_| ())
            .into_actor(self);
        ctx.spawn(f);
        self.schedule_next_tick(ctx);
    }
}

impl<T> Handler<TickMessage> for RaftClient<T>
where
    T: RaftStateMachine + 'static,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, _: TickMessage, ctx: &mut Context<Self>) -> Self::Result {
        let result = self.raft_tick();
        self.schedule_next_tick(ctx);
        result
    }
}

impl<T> Handler<RaftPropose<T>> for RaftClient<T>
where
    T: RaftStateMachine + 'static,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, message: RaftPropose<T>, _ctx: &mut Context<Self>) -> Self::Result {
        self.raft_propose_entry(message)
    }
}

impl<T> Handler<RaftMessageReceived> for RaftClient<T>
where
    T: RaftStateMachine + 'static,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, message: RaftMessageReceived, _ctx: &mut Context<Self>) -> Self::Result {
        self.raft_step(message.message)
    }
}
