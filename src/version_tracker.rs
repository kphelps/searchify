use crate::document::{DocumentId, DocumentVersion};
use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TrackedVersion {
    Live(DocumentVersion),
    Deleted,
}

// Keep track of versions that are not yet committed
pub struct VersionTracker {
    new_versions: HashMap<DocumentId, TrackedVersion>,
    old_versions: HashMap<DocumentId, TrackedVersion>,
}

impl VersionTracker {
    pub fn new() -> Self {
        Self {
            new_versions: HashMap::new(),
            old_versions: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: &DocumentId, version: DocumentVersion) {
        self.new_versions.insert(id.clone(), TrackedVersion::Live(version));
    }

    pub fn delete(&mut self, id: &DocumentId) {
        self.new_versions.insert(id.clone(), TrackedVersion::Deleted);
    }

    pub fn pre_commmit(&mut self) {
        let new_versions = std::mem::replace(&mut self.new_versions, HashMap::new());
        self.old_versions = new_versions;
    }

    pub fn post_commit(&mut self) {
        self.old_versions = HashMap::new();
    }

    pub fn get(&self, id: &DocumentId) -> Option<TrackedVersion> {
        self.new_versions
            .get(id)
            .or_else(|| self.old_versions.get(id))
            .cloned()
    }
}
