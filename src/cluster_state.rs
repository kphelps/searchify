use crate::event_emitter::EventEmitter;
use crate::key_value_state_machine::{KeyValueStateMachine, MetaStateEvent};
use crate::proto::*;
use futures::sync::mpsc;
use log::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

#[derive(Clone)]
pub enum ClusterStateEvent {
    ShardAllocated(ShardState),
}

#[derive(Clone)]
pub struct ClusterState {
    inner: Arc<RwLock<ClusterStateInner>>,
}

#[derive(Clone)]
pub struct ClusterStateRef {
    inner: Weak<RwLock<ClusterStateInner>>,
}

struct ClusterStateInner {
    node_id: u64,
    indices: HashMap<String, IndexState>,
    // shards assigned to this node
    shards: HashMap<u64, ShardState>,
    remote_shards: HashMap<u64, ShardState>,

    event_emitter: EventEmitter<ClusterStateEvent>,
}

pub struct ClusterStateUpdater {
    cluster_state: ClusterState,
}

impl ClusterState {
    pub fn new(node_id: u64) -> Self {
        let inner = ClusterStateInner {
            node_id,
            indices: HashMap::new(),
            shards: HashMap::new(),
            remote_shards: HashMap::new(),
            event_emitter: EventEmitter::new(16),
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn handle(&self) -> ClusterStateRef {
        ClusterStateRef {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub fn initialize(&self, kv: &KeyValueStateMachine) {
        let mut state = self.inner.write().unwrap();
        kv.list_indices().into_iter().for_each(|index| {
            index.get_shards().iter().for_each(|shard| {
                if shard
                    .get_replicas()
                    .iter()
                    .find(|peer| peer.id == state.node_id)
                    .is_some()
                {
                    state.shards.insert(shard.id, shard.clone());
                }
            });
            state.indices.insert(index.name.clone(), index);
        });
    }

    pub fn get_index(&self, name: &str) -> Option<IndexState> {
        self.inner.read().unwrap().indices.get(name).cloned()
    }

    pub fn get_shard(&self, id: u64) -> Option<ShardState> {
        let locked = self.inner.read().unwrap();
        locked
            .shards
            .get(&id)
            .or_else(|| locked.remote_shards.get(&id))
            .cloned()
    }

    pub fn shards_for_node(&self) -> Vec<ShardState> {
        self.inner
            .read()
            .unwrap()
            .shards
            .values()
            .cloned()
            .collect()
    }

    pub fn subscribe(&self) -> mpsc::Receiver<ClusterStateEvent> {
        self.inner.write().unwrap().event_emitter.subscribe()
    }
}

impl ClusterStateUpdater {
    pub fn new(cluster_state: ClusterState) -> Self {
        Self { cluster_state }
    }

    pub fn handle_event(&self, event: MetaStateEvent) {
        debug!("ClusterStateEvent: {:?}", event);
        match event {
            MetaStateEvent::IndexUpdate(name, maybe_index) => {
                let mut state = self.cluster_state.inner.write().unwrap();
                if let Some(index) = maybe_index {
                    info!("Index updated: {}", name);
                    state.indices.insert(name, index.clone());
                } else {
                    info!("Index deleted: {}", name);
                    state.indices.remove(&name);
                }
            }
            MetaStateEvent::ShardUpdate(id, maybe_shard) => {
                let mut state = self.cluster_state.inner.write().unwrap();
                if let Some(shard) = maybe_shard {
                    if shard
                        .get_replicas()
                        .iter()
                        .find(|peer| peer.id == state.node_id)
                        .is_some()
                    {
                        info!("Shard allocated: {}", id);
                        state.shards.insert(id, shard.clone());
                        state
                            .event_emitter
                            .emit(ClusterStateEvent::ShardAllocated(shard));
                    } else {
                        state.remote_shards.insert(id, shard.clone());
                    }
                } else {
                    if let Some(shard) = state.shards.remove(&id) {
                        info!("Shard deleted: {}", shard.id);
                    }
                    let _ = state.remote_shards.remove(&id);
                }
            }
            _ => (),
        }
    }
}

impl ClusterStateRef {
    pub fn upgrade(&self) -> Option<ClusterState> {
        self.inner.upgrade().map(|inner| ClusterState { inner })
    }
}
