use crate::cached_persistent_map::CachedPersistentMap;
use crate::id_generator::IdGenerator;
use crate::keys::{self, KeySpace};
use crate::kv_index::KvIndex;
use crate::proto::ShardState;
use crate::storage_engine::StorageEngine;
use failure::Error;
use std::collections::HashSet;

pub struct ShardTracker {
    shards: CachedPersistentMap<u64, ShardState>,
    id_generator: IdGenerator,
    by_node: KvIndex<u64>,
    by_index: KvIndex<u64>,
    _under_replicated: HashSet<u64>,
}

impl ShardTracker {
    pub fn new(storage: &StorageEngine) -> Result<Self, Error> {
        let shards = CachedPersistentMap::new(storage, KeySpace::Shard.as_key())?;
        let id_generator = IdGenerator::new(storage, keys::id_key(KeySpace::Shard))?;
        let mut tracker = Self {
            shards,
            id_generator,
            by_node: KvIndex::new(),
            by_index: KvIndex::new(),
            _under_replicated: HashSet::new(),
        };
        tracker.initialize_indices();
        Ok(tracker)
    }

    pub fn create_shard(&mut self, shard: &mut ShardState) -> Result<(), Error> {
        let id = self.id_generator.next()?;
        shard.set_id(id);
        self.shards.insert(&id, shard)?;
        self.update_indices_for_shard(shard);
        Ok(())
    }

    pub fn get_shards_for_index(&self, index_id: u64) -> Vec<ShardState> {
        self.by_index.get(&index_id, &self.shards)
    }

    pub fn delete_shards_for_index(&mut self, index_id: u64) -> Result<Vec<u64>, Error> {
        self.by_index
            .get(&index_id, &self.shards)
            .iter()
            .map(|shard| self.delete_shard(shard))
            .collect::<Result<Vec<u64>, Error>>()
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn replica_len(&self) -> usize {
        self.shards.values().iter().map(|s| s.get_replicas().len()).sum()
    }

    pub fn unassigned_len(&self) -> usize {
        // TODO
        0
    }

    fn delete_shard(&mut self, shard: &ShardState) -> Result<u64, Error> {
        self.shards.delete(&shard.id)?;
        self.update_indices_for_deleted_shard(shard);
        Ok(shard.id)
    }

    fn initialize_indices(&mut self) {
        self.shards.cache().clone().values().for_each(|shard| {
            self.update_indices_for_shard(&shard);
        });
    }

    fn update_indices_for_shard(&mut self, shard: &ShardState) {
        let node_ids = shard
            .get_replicas()
            .iter()
            .map(|node| node.get_id())
            .collect();
        self.by_node.bulk_insert(shard.get_id(), node_ids);
        self.by_index.insert(shard.get_id(), shard.get_index_id());
    }

    fn update_indices_for_deleted_shard(&mut self, shard: &ShardState) {
        let node_ids = shard
            .get_replicas()
            .iter()
            .map(|node| node.get_id())
            .collect();
        self.by_node.bulk_remove(shard.get_id(), node_ids);
        self.by_index.remove(shard.get_id(), shard.get_index_id());
    }
}
