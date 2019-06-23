use crate::key_value_state_machine::MetaStateEvent;
use crate::proto::IndexState;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct ClusterState {
    inner: Arc<RwLock<ClusterStateInner>>,
}

struct ClusterStateInner {
    indices: HashMap<String, IndexState>,
}

pub struct ClusterStateUpdater {
    cluster_state: ClusterState,
}

impl ClusterState {
    pub fn new() -> Self {
        let inner = ClusterStateInner {
            indices: HashMap::new(),
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn get_index(&self, name: &str) -> Option<IndexState> {
        self.inner.read().unwrap().indices.get(name).cloned()
    }
}


impl ClusterStateUpdater {
    pub fn new(cluster_state: ClusterState) -> Self {
        Self { cluster_state }
    }

    pub fn handle_event(&self, event: MetaStateEvent) {
        match event {
            MetaStateEvent::IndexUpdate(name, maybe_index) => {
                let mut locked = self.cluster_state.inner.write().unwrap();
                if let Some(index) = maybe_index {
                    log::info!("Index updated: {}", name);
                    locked.indices.insert(name, index);
                } else {
                    log::info!("Index deleted: {}", name);
                    locked.indices.remove(&name);
                }
            },
            _ => (),
        }
    }
}
