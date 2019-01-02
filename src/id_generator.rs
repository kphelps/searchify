use crate::keys::MetaKey;
use crate::cached_persistent_cell::CachedPersistentCell;
use crate::proto::SequenceState;
use crate::storage_engine::StorageEngine;
use failure::Error;

pub struct IdGenerator {
    storage: CachedPersistentCell<SequenceState>,
}

impl IdGenerator {
    pub fn new(engine: &StorageEngine, key: MetaKey) -> Result<Self, Error> {
        let mut storage = CachedPersistentCell::new(engine, key)?;
        if let None = storage.get() {
            let mut state = SequenceState::new();
            state.value = 1;
            storage.set(&state)?;
        }
        Ok(Self{storage})
    }

    pub fn next(&mut self) -> Result<u64, Error> {
        let next = self.storage.get().cloned().unwrap();
        let mut to_save = next.clone();
        to_save.value += 1;
        self.storage.set(&to_save)?;
        Ok(next.value)
    }
}
