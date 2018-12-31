use crate::keys::{self, KeySpace};
use crate::cached_persistent_map::CachedPersistentMap;
use crate::id_generator::IdGenerator;
use crate::proto::ShardState;
use crate::storage_engine::StorageEngine;
use failure::Error;
use std::collections::{HashMap, HashSet};

pub struct ShardTracker {
    shards: CachedPersistentMap<u64, ShardState>,
    id_generator: IdGenerator,
    by_node: HashMap<u64, HashSet<u64>>,
    under_replicated: HashSet<u64>,
}

impl ShardTracker {

    pub fn new(storage: &StorageEngine) -> Result<Self, Error> {
        let shards = CachedPersistentMap::new(storage, KeySpace::Shard.as_key())?;
        let id_generator = IdGenerator::new(storage, keys::id_key(KeySpace::Shard))?;
        let mut tracker = Self {
            shards,
            id_generator,
            by_node: HashMap::new(),
            under_replicated: HashSet::new(),
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

    pub fn get_shards_assigned_to_node(&self, node_id: u64) -> Vec<ShardState> {
        let maybe_shards = self.by_node.get(&node_id);
        if maybe_shards.is_none() {
            return vec![];
        }

        maybe_shards
            .unwrap()
            .iter()
            .map(|shard_id| self.shards.get(shard_id))
            .flatten()
            .cloned()
            .collect()
    }

    fn initialize_indices(&mut self) {
        self.shards.cache().clone().values().for_each(|shard| {
            self.update_indices_for_shard(&shard)
        });
    }

    fn add_shard_to_node(&mut self, shard_id: u64, node_id: u64) {
        let set = self.by_node.entry(node_id).or_insert_with(HashSet::new);
        set.insert(shard_id);
    }

    fn update_indices_for_shard(&mut self, shard: &ShardState) {
        shard.get_replicas().iter()
            .for_each(|node| self.add_shard_to_node(shard.get_id(), node.get_id()))
    }
}
