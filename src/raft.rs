use crate::event_emitter::EventEmitter;
use crate::metrics::RAFT_TICK_HISTOGRAM;
use crate::node_router::NodeRouterHandle;
use crate::proto::EntryContext;
use crate::raft_router::RaftRouter;
use crate::raft_storage::RaftStorage;
use crate::storage_engine::MessageWriteBatch;
use failure::Error;
use futures::{
    future,
    prelude::*,
    stream,
    sync::{mpsc, oneshot},
};
use log::*;
use prost::Message as _;
use protobuf::{parse_from_bytes, Message};
use raft::{
    self, eraftpb,
    raw_node::{RawNode, Ready},
    storage::Storage,
    Config, StateRole,
};
use std::boxed::Box;
use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::timer::Interval;

pub type TaskFn = Box<dyn Fn() -> Box<dyn Future<Item = (), Error = ()> + Send> + Send>;

#[derive(Clone)]
pub enum RaftEvent {
    LeaderChanged(u64),
}

struct RaftState<T> {
    state_machine: T,
    node_router: NodeRouterHandle,
    raft_node: RawNode<RaftStorage>,
    node_id: u64,
    raft_group_id: u64,
    leader_id: u64,
    current_role: StateRole,
    leader_task: Option<TaskFn>,
    follower_task: Option<TaskFn>,
    _role_task_handle: Option<oneshot::Sender<()>>,
    observers: HashMap<u64, Box<StateMachineObserver<T> + Send + Sync + 'static>>,
    event_emitter: EventEmitter<RaftEvent>,
}

enum RaftStateMessage<T>
where
    T: RaftStateMachine + 'static,
{
    Message(RaftMessageReceived),
    Propose(RaftPropose<T>),
}

pub struct RaftClient<T>
where
    T: RaftStateMachine + 'static,
{
    sender: mpsc::Sender<RaftStateMessage<T>>,
    inner: Arc<Mutex<RaftState<T>>>,
}

impl<T> Clone for RaftClient<T>
where
    T: RaftStateMachine + 'static,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            inner: self.inner.clone(),
        }
    }
}

pub trait StateMachineObserver<S> {
    fn observe(self: Box<Self>, state_machine: &S);
}

pub struct FutureStateMachineObserver<T, F> {
    sender: oneshot::Sender<T>,
    observe_impl: F,
}

impl<T, F> FutureStateMachineObserver<T, F> {
    pub fn new(sender: oneshot::Sender<T>, observe: F) -> Self {
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
        if self.sender.send(result).is_err() {
            error!("Failed to observe state machine");
        }
    }
}

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

#[derive(Clone)]
pub struct RaftMessageReceived {
    pub raft_group_id: u64,
    pub message: eraftpb::Message,
}

pub trait RaftStateMachine {
    type EntryType: Message;

    fn apply(&mut self, entry: Self::EntryType) -> Result<bool, Error>;
    fn peers(&self) -> Result<Vec<u64>, Error>;
}

pub trait RaftEntryHandler<T> {
    type Result;

    fn handle(&mut self, entry: T) -> Self::Result;
}

impl<T> RaftClient<T>
where
    T: RaftStateMachine + Send + 'static,
{
    pub fn new(
        node_id: u64,
        storage: RaftStorage,
        state_machine: T,
        node_router: NodeRouterHandle,
        raft_router: &RaftRouter<T>,
        leader_task: Option<TaskFn>,
        follower_task: Option<TaskFn>,
    ) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::channel(4096);
        let state = RaftState::new(
            node_id,
            storage,
            state_machine,
            node_router,
            leader_task,
            follower_task,
        )?;
        let group_id = state.raft_group_id;
        let inner = Arc::new(Mutex::new(state));
        RaftState::run(inner.clone(), receiver, Duration::from_millis(200));
        let client = Self { sender, inner };
        client.register(group_id, raft_router);
        Ok(client)
    }

    fn register(&self, raft_group_id: u64, raft_router: &RaftRouter<T>) {
        let f = raft_router
            .register(raft_group_id, self.clone())
            .map_err(move |_| error!("Failed to register raft group '{}'", raft_group_id));
        tokio::spawn(f);
    }

    pub fn propose_entry(&self, m: RaftPropose<T>) -> impl Future<Item = (), Error = Error> {
        self.send(RaftStateMessage::Propose(m))
    }

    pub fn receive_message(
        &self,
        message: RaftMessageReceived,
    ) -> impl Future<Item = (), Error = Error> {
        self.send(RaftStateMessage::Message(message))
    }

    fn send(&self, event: RaftStateMessage<T>) -> impl Future<Item = (), Error = Error> {
        self.sender.clone().send(event).map(|_| ()).from_err()
    }

    pub fn subscribe(&self) -> mpsc::Receiver<RaftEvent> {
        self.inner.lock().unwrap().event_emitter.subscribe()
    }
}

enum StateEvent<T>
where
    T: RaftStateMachine + 'static,
{
    Tick,
    Event(RaftStateMessage<T>),
    Done,
}

impl<T> StateEvent<T>
where
    T: RaftStateMachine + 'static,
{
    fn is_done(&self) -> bool {
        match self {
            StateEvent::Done => true,
            _ => false,
        }
    }
}

impl<T> RaftState<T>
where
    T: RaftStateMachine + Send + 'static,
{
    pub fn new(
        node_id: u64,
        storage: RaftStorage,
        state_machine: T,
        node_router: NodeRouterHandle,
        leader_task: Option<TaskFn>,
        follower_task: Option<TaskFn>,
    ) -> Result<Self, Error> {
        let raft_group_id = storage.raft_group_id();
        let config = Config {
            id: node_id,
            heartbeat_tick: 3,
            election_tick: 10,
            applied: storage.last_applied_index(),
            tag: format!("[group-{}]", raft_group_id),
            ..Default::default()
        };
        config.validate()?;
        let initial_state = storage.initial_state()?;
        let mut node = RawNode::new(&config, storage)?;

        let peers = initial_state.conf_state.nodes;
        if peers.len() == 1 && peers[0] == node_id {
            node.campaign()?;
        }

        let current_role = node.raft.state.clone();

        let mut state = Self {
            raft_node: node,
            observers: HashMap::new(),
            leader_id: 0,
            node_id,
            raft_group_id,
            state_machine,
            node_router,
            current_role,
            leader_task,
            follower_task,
            _role_task_handle: None,
            event_emitter: EventEmitter::new(16),
        };
        state.handle_role_change();
        Ok(state)
    }

    fn run(
        this: Arc<Mutex<Self>>,
        receiver: mpsc::Receiver<RaftStateMessage<T>>,
        tick_interval: Duration,
    ) {
        let tick_stream = Interval::new_interval(tick_interval)
            .map(|_| StateEvent::Tick)
            .map_err(|err| error!("Error in raft tick loop: {:?}", err));
        let message_stream = receiver
            .map(StateEvent::Event)
            .chain(stream::once(Ok(StateEvent::Done)))
            .select(tick_stream)
            .take_while(|item| future::ok(!item.is_done()));
        let f = message_stream.for_each(move |event| {
            if let Err(err) = this.lock().unwrap().handle_event(event) {
                debug!("Raft event failure: {:?}", err)
            }
            Ok(())
        });
        tokio::spawn(f.then(|_| Ok(())));
    }

    fn handle_event(&mut self, event: StateEvent<T>) -> Result<(), Error> {
        match event {
            StateEvent::Tick => {
                let timer = RAFT_TICK_HISTOGRAM.start_timer();
                let out = self.raft_tick();
                timer.observe_duration();
                out
            }
            StateEvent::Event(event) => match event {
                RaftStateMessage::Message(message) => {
                    self.raft_node.step(message.message)?;
                    Ok(())
                }
                RaftStateMessage::Propose(proposal) => self.propose_entry(proposal),
            },
            StateEvent::Done => unreachable!(),
        }
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
        self.check_for_role_change();
        self.propose_membership_changes()?;
        Ok(())
    }

    fn update_leader_id(&mut self) {
        if self.leader_id != self.raft_node.raft.leader_id {
            self.leader_id = self.raft_node.raft.leader_id;
            self.event_emitter
                .emit(RaftEvent::LeaderChanged(self.leader_id));
        }
    }

    fn check_for_role_change(&mut self) {
        if self.current_role == self.raft_node.raft.state {
            return;
        }
        self.current_role = self.raft_node.raft.state.clone();
        self._role_task_handle = None;
        self.handle_role_change();
    }

    fn handle_role_change(&mut self) {
        match self.current_role {
            StateRole::Leader => self.change_role_to_leader(),
            StateRole::Follower => self.change_role_to_follower(),
            _ => info!("[group-{}] became candidate", self.raft_group_id),
        }
    }

    fn change_role_to_leader(&mut self) {
        info!("[group-{}] became leader", self.raft_group_id);
        if let Some(ref f) = self.leader_task {
            let (sender, receiver) = oneshot::channel();
            let f = f().select(receiver.map_err(|_| ()));
            tokio::spawn(f.then(|_| Ok(())));
            self._role_task_handle = Some(sender);
        }
    }

    fn change_role_to_follower(&mut self) {
        info!("[group-{}] became follower", self.raft_group_id);
        if let Some(ref f) = self.follower_task {
            let (sender, receiver) = oneshot::channel();
            let f = f().select(receiver.map_err(|_| ()));
            tokio::spawn(f.then(|_| Ok(())));
            self._role_task_handle = Some(sender);
        }
    }

    fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<(), Error> {
        if self.raft_group_id != 0 {
            trace!("Propose({}, {})", context.len(), data.len());
        }
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
        if result.is_err() {
            self.observers.remove(&id);
        }
        result
    }

    fn propose_membership_changes(&mut self) -> Result<(), Error> {
        if !self.is_leader() || self.raft_node.raft.is_in_membership_change() {
            return Ok(());
        }

        let peers = self.state_machine.peers()?;

        let group_state = self.raft_node.get_store().raft_group.clone();
        let masters = group_state
            .get_peers()
            .iter()
            .map(|p| p.id)
            .collect::<HashSet<u64>>();
        let existing_learners = group_state
            .get_learners()
            .iter()
            .map(|p| p.id)
            .collect::<HashSet<u64>>();
        let new_learners = peers
            .into_iter()
            .filter(|p| !masters.contains(p))
            .collect::<HashSet<u64>>();
        if new_learners == existing_learners {
            return Ok(());
        }

        info!(
            "Proposing membership change. Voters {:?}, Learners {:?}, Old Learners {:?}",
            masters, new_learners, existing_learners
        );

        self.raft_node
            .raft
            .propose_membership_change((masters, new_learners))?;
        Ok(())
    }

    fn is_leader(&self) -> bool {
        self.raft_node.raft.leader_id == self.node_id
    }

    fn apply_snapshot(&mut self, ready: &Ready) -> Result<(), Error> {
        if !raft::is_empty_snap(&ready.snapshot()) {
            self.raft_node
                .mut_store()
                .apply_snapshot(ready.snapshot().clone())?
        }
        Ok(())
    }

    fn append_entries(
        &mut self,
        ready: &Ready,
        batch: &mut MessageWriteBatch,
    ) -> Result<(), Error> {
        self.raft_node.mut_store().append(&ready.entries(), batch)
    }

    fn apply_hardstate(
        &mut self,
        ready: &Ready,
        batch: &mut MessageWriteBatch,
    ) -> Result<(), Error> {
        if let Some(ref hardstate) = ready.hs() {
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
            tokio::spawn(f);
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
                if entry.data.is_empty() && entry.context.is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }

                let did_apply = match entry.get_entry_type() {
                    eraftpb::EntryType::EntryNormal => self.handle_normal_entry(entry)?,
                    eraftpb::EntryType::EntryConfChange => {
                        conf_state = Some(self.handle_conf_change_entry(entry)?);
                        true
                    }
                };
                if did_apply {
                    last_apply_index = entry.index;
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

    fn handle_normal_entry(&mut self, entry: &eraftpb::Entry) -> Result<bool, Error> {
        debug!("NormalEntry: {:?}", entry);
        let ctx = parse_from_bytes::<EntryContext>(&entry.context)?;
        let parsed = parse_from_bytes::<T::EntryType>(&entry.data)?;
        let apply_result = self.state_machine.apply(parsed);
        if let Err(ref err) = apply_result {
            let parsed = parse_from_bytes::<T::EntryType>(&entry.data)?;
            warn!("Failed to apply '{:?}': {}", parsed, err);
        }
        if let Some(observer) = self.observers.remove(&ctx.id) {
            observer.observe(&self.state_machine);
        }
        Ok(apply_result.unwrap_or(false))
    }

    fn handle_conf_change_entry(
        &mut self,
        entry: &eraftpb::Entry,
    ) -> Result<eraftpb::ConfState, Error> {
        let cc = eraftpb::ConfChange::decode(&entry.data).expect("Valid protobuf");

        debug!("ConfChange: {:?}", cc);

        match cc.get_change_type() {
            eraftpb::ConfChangeType::AddNode => {
                self.raft_node.mut_store().add_node(cc.node_id)?;
            }
            eraftpb::ConfChangeType::RemoveNode => {
                self.raft_node.mut_store().remove_node(cc.node_id)?;
            }
            eraftpb::ConfChangeType::AddLearnerNode => {
                self.raft_node.mut_store().add_learner(cc.node_id)?;
            }
            eraftpb::ConfChangeType::BeginMembershipChange => {
                self.raft_node.raft.begin_membership_change(&cc)?;
                if let Some(conf) = cc.configuration {
                    self.raft_node
                        .mut_store()
                        .membership_change(&conf.nodes, &conf.learners)?;
                }
            }
            eraftpb::ConfChangeType::FinalizeMembershipChange => {
                if let Err(err) = self.raft_node.raft.finalize_membership_change(&cc) {
                    warn!("Finalizing membership change error: {:?}", err);
                }
            }
        }

        Ok(self.raft_node.raft.prs().configuration().clone().into())
    }

    fn advance_raft(&mut self, ready: Ready) {
        self.raft_node.advance(ready);
    }

    fn compact(&mut self) -> Result<(), Error> {
        let raft_applied = self.raft_node.raft.raft_log.get_applied();
        self.raft_node.mut_store().compact(raft_applied)
    }
}
