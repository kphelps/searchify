use crate::raft::{RaftClient, RaftMessageReceived, RaftPropose, RaftStateMachine};
use failure::{format_err, Error};
use futures::{future, prelude::*};
use log::*;
use std::clone::Clone;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct RaftRouter<K>
where
    K: RaftStateMachine + 'static,
{
    inner: Arc<RwLock<Inner<K>>>,
}

impl<K> Clone for RaftRouter<K>
where
    K: RaftStateMachine + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<K>
where
    K: RaftStateMachine + 'static,
{
    raft_groups: HashMap<u64, RaftClient<K>>,
}

impl<K> RaftRouter<K>
where
    K: RaftStateMachine + Send + 'static,
{
    pub fn new() -> Self {
        let inner = Inner::new();
        Self { inner: Arc::new(RwLock::new(inner)) }
    }

    pub fn register(&self, id: u64, addr: RaftClient<K>) -> impl Future<Item = (), Error = Error> {
        let mut locked = self.inner.write().unwrap();
        locked.add_group(id, addr)
    }

    pub fn has_group(&self, id: u64) -> bool {
        let locked = self.inner.read().unwrap();
        locked.has_group(id)
    }

    pub fn handle_raft_message(
        &self,
        message: RaftMessageReceived,
    ) -> impl Future<Item = (), Error = Error> {
        let locked = self.inner.read().unwrap();
        locked.handle_raft_message(message)
    }

    pub fn propose(&self, proposal: RaftPropose<K>) -> impl Future<Item = (), Error = Error> {
        let locked = self.inner.read().unwrap();
        locked.handle_raft_propose(proposal)
    }
}

impl<K> Inner<K>
where
    K: RaftStateMachine + Send,
{
    fn new() -> Self {
        Self {
            raft_groups: HashMap::new(),
        }
    }

    fn has_group(&self, id: u64) -> bool {
        self.raft_groups.contains_key(&id)
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
        debug!(
            "[group-{}] Proposal {:?}",
            message.raft_group_id, message.entry
        );
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
