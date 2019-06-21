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
