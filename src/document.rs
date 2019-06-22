use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DocumentId(String);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DocumentVersion(u64);

#[allow(dead_code)] // TODO
pub enum ExpectedVersion {
    Any,
    Version(DocumentVersion),
    Deleted,
}

impl DocumentId {
    pub fn routing_id(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(self.0.as_bytes());
        hasher.finish()
    }

    pub fn id(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for DocumentId {
    fn from(string: String) -> Self {
        DocumentId(string)
    }
}

impl<'a> From<&'a str> for DocumentId {
    fn from(string: &'a str) -> Self {
        Self::from(string.to_string())
    }
}

impl From<DocumentId> for String {
    fn from(document_id: DocumentId) -> Self {
        document_id.0
    }
}

impl From<u64> for DocumentVersion {
    fn from(version: u64) -> Self {
        DocumentVersion(version)
    }
}
