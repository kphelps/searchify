use crate::raft::{RaftClient, RaftMessageReceived, RaftPropose, RaftStateMachine};
use actix::prelude::*;
use failure::{format_err, Error};
use futures::{future, prelude::*, sync::mpsc};
use log::*;
use std::clone::Clone;
use std::collections::HashMap;
use std::sync::Arc;

pub struct RaftRouter<K>
where
    K: RaftStateMachine + 'static,
{
    inner: Arc<Inner<K>>,
    sender: mpsc::Sender<RaftRouterEvent<K>>,
}

impl<K> Clone for RaftRouter<K>
where
    K: RaftStateMachine + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            sender: self.sender.clone(),
        }
    }
}

enum RaftRouterEvent<K>
where
    K: RaftStateMachine + 'static,
{
    NewGroup(u64, Addr<RaftClient<K>>),
    Message(RaftMessageReceived),
    Propose(RaftPropose<K>),
}

struct Inner<K>
where
    K: RaftStateMachine + 'static,
{
    node_id: u64,
    receiver: mpsc::Receiver<RaftRouterEvent<K>>,
    raft_groups: HashMap<u64, Addr<RaftClient<K>>>,
}

impl<K> RaftRouter<K>
where
    K: RaftStateMachine + 'static,
{
    pub fn new(node_id: u64) -> Self {
        let (sender, receiver) = mpsc::channel(4096);
        let inner = Arc::new(Inner::new(node_id, receiver));
        let handle = Self { inner, sender };
        handle.run();
        handle
    }

    fn run(&self) {
        let f = future::loop_fn(self.inner.clone(), |inner| {
            inner.tick().map(move |_| future::Loop::Continue(inner))
        })
        .map_err(|_| ());
        tokio::spawn(f);
    }

    pub fn register(
        &self,
        id: u64,
        addr: Addr<RaftClient<K>>,
    ) -> impl Future<Item = (), Error = Error> {
        self.send(RaftRouterEvent::NewGroup(id, addr))
    }

    pub fn handle_raft_message(
        &self,
        message: RaftMessageReceived,
    ) -> impl Future<Item = (), Error = Error> {
        self.send(RaftRouterEvent::Message(message))
    }

    pub fn propose(&self, proposal: RaftPropose<K>) -> impl Future<Item = (), Error = Error> {
        self.send(RaftRouterEvent::Propose(proposal))
    }

    fn send(&self, event: RaftRouterEvent<K>) -> impl Future<Item = (), Error = Error> {
        self.sender.clone().send(event).map(|_| ()).from_err()
    }
}

impl<K> Inner<K>
where
    K: RaftStateMachine,
{
    fn new(node_id: u64, receiver: mpsc::Receiver<RaftRouterEvent<K>>) -> Self {
        Self {
            node_id,
            receiver,
            raft_groups: HashMap::new(),
        }
    }

    fn tick(&self) -> impl Future<Item = (), Error = Error> {
        future::ok(())
    }

    fn handle_raft_message(
        &self,
        message: RaftMessageReceived,
    ) -> Box<Future<Item = (), Error = Error>> {
        debug!(
            "[group-{}] Received message for {}",
            message.raft_group_id, message.message.to
        );
        let raft = self.raft_groups.get(&message.raft_group_id).cloned();
        if raft.is_none() {
            let error = format_err!("Unknown raft group: {}", message.raft_group_id);
            return Box::new(future::err(error));
        }
        Box::new(raft.unwrap().send(message).flatten())
    }
}
