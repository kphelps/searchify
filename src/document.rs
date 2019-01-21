use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct DocumentId(String);

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

impl From<DocumentId> for String {
    fn from(document_id: DocumentId) -> Self {
        document_id.0
    }
}
