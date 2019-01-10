use crate::raft::{RaftClient, RaftMessageReceived, RaftPropose, RaftStateMachine};
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
    sender: mpsc::Sender<RaftRouterEvent<K>>,
}

impl<K> Clone for RaftRouter<K>
where
    K: RaftStateMachine + 'static,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

enum RaftRouterEvent<K>
where
    K: RaftStateMachine + 'static,
{
    NewGroup(u64, RaftClient<K>),
    Message(RaftMessageReceived),
    Propose(RaftPropose<K>),
}

struct Inner<K>
where
    K: RaftStateMachine + 'static,
{
    node_id: u64,
    raft_groups: HashMap<u64, RaftClient<K>>,
}

impl<K> RaftRouter<K>
where
    K: RaftStateMachine + Send + 'static,
{
    pub fn new(node_id: u64) -> Self {
        let (sender, receiver) = mpsc::channel(4096);
        let inner = Inner::new(node_id);
        inner.run(receiver);
        Self { sender }
    }

    pub fn register(&self, id: u64, addr: RaftClient<K>) -> impl Future<Item = (), Error = Error> {
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
    K: RaftStateMachine + Send,
{
    fn new(node_id: u64) -> Self {
        Self {
            node_id,
            raft_groups: HashMap::new(),
        }
    }

    fn run(mut self, receiver: mpsc::Receiver<RaftRouterEvent<K>>) {
        let f = receiver.for_each(move |event| self.handle_event(event));
        tokio::spawn(f);
    }

    fn handle_event(
        &mut self,
        event: RaftRouterEvent<K>,
    ) -> impl Future<Item = (), Error = ()> + Send {
        let f: Box<Future<Item = (), Error = Error> + Send> = match event {
            RaftRouterEvent::NewGroup(id, addr) => Box::new(self.add_group(id, addr)),
            RaftRouterEvent::Message(message) => Box::new(self.handle_raft_message(message)),
            RaftRouterEvent::Propose(propose) => Box::new(self.handle_raft_propose(propose)),
        };
        f.map_err(|err| error!("Error routing raft event: {:?}", err))
    }

    fn add_group(
        &mut self,
        id: u64,
        addr: RaftClient<K>,
    ) -> impl Future<Item = (), Error = Error> + Send {
        self.raft_groups.insert(id, addr);
        future::ok(())
    }

    fn handle_raft_message(
        &self,
        message: RaftMessageReceived,
    ) -> impl Future<Item = (), Error = Error> + Send {
        debug!(
            "[group-{}] Received message for {}",
            message.raft_group_id, message.message.to
        );
        self.get_group(message.raft_group_id)
            .and_then(move |raft| raft.receive_message(message))
    }

    fn handle_raft_propose(
        &self,
        message: RaftPropose<K>,
    ) -> impl Future<Item = (), Error = Error> + Send {
        self.get_group(message.raft_group_id)
            .and_then(move |raft| raft.propose_entry(message))
    }

    fn get_group(&self, id: u64) -> impl Future<Item = RaftClient<K>, Error = Error> + Send {
        self.raft_groups
            .get(&id)
            .cloned()
            .ok_or_else(move || format_err!("Unknown raft group: {}", id))
            .into_future()
    }
}
