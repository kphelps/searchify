use crate::keys::{self, KeySpace};
use crate::cached_persistent_map::CachedPersistentMap;
use crate::id_generator::IdGenerator;
use crate::kv_index::UniqueKvIndex;
use crate::proto::IndexState;
use crate::storage_engine::StorageEngine;
use failure::{Error, err_msg, format_err};

pub struct IndexTracker {
    indices: CachedPersistentMap<u64, IndexState>,
    id_generator: IdGenerator,
    by_name: UniqueKvIndex<String>,
}

impl IndexTracker {

    pub fn new(storage: &StorageEngine) -> Result<Self, Error> {
        let indices = CachedPersistentMap::new(storage, KeySpace::Index.as_key())?;
        let id_generator = IdGenerator::new(storage, keys::id_key(KeySpace::Index))?;
        let mut tracker = Self {
            indices,
            id_generator,
            by_name: UniqueKvIndex::new(),
        };
        tracker.initialize_indices()?;
        Ok(tracker)
    }

    pub fn all(&self) -> Vec<IndexState> {
        self.indices.cache().values().cloned().collect()
    }

    pub fn create(&mut self, index: &mut IndexState) -> Result<(), Error> {
        self.validate(index)?;
        let id = self.id_generator.next()?;
        index.set_id(id);
        self.indices.insert(&id, index)?;
        self.update_indices_for_index(index)?;
        Ok(())
    }

    pub fn delete(&mut self, name: &str) -> Result<IndexState, Error> {
        let index_state = self.by_name.get(&name.to_string(), &self.indices)
            .ok_or(err_msg("Index not found"))?;
        self.indices.delete(&index_state.id)?;
        self.by_name.remove(&index_state.name);
        Ok(index_state)
    }

    pub fn find_by_name(&self, name: &str) -> Option<IndexState> {
        self.by_name.get(&name.to_string(), &self.indices)
    }

    fn initialize_indices(&mut self) -> Result<(), Error> {
        self.indices.cache().clone().values().map(|index| {
            self.update_indices_for_index(&index)
        }).collect()
    }

    fn update_indices_for_index(&mut self, index: &IndexState) -> Result<(), Error> {
        self.by_name.insert(index.id, index.name.clone())
    }

    fn validate(&self, index: &IndexState) -> Result<(), Error> {
        if !self.by_name.can_insert(&index.name) {
            return Err(format_err!("Index '{}' already exists", index.name))
        }
        Ok(())
    }
}
