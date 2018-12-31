use crate::keys::{MetaKey, KeySpace};
use crate::persistent_map::PersistentMap;
use crate::proto::*;

struct IndexMetaTable {
    indices: PersistentMap<String, IndexState>,
    shards: PersistentMap<(String, u64), IndexState>,
}

impl IndexMetaTable {

    pub fn new(engine: StorageEngine) -> Self {
        Self {
            indices: PersistentMap::new(
                engine.clone(),
                MetaKey::new().add(KeySpace::Index),
            ),
            shards: PersistentMap::new(
                engine.clone(),
                MetaKey::new().add(KeySpace::Shard),
            ),
        }
    }
}
