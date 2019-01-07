use crate::keys::MetaKey;
use crate::persistent_cell::PersistentCell;
use crate::storage_engine::{Persistable, StorageEngine};
use failure::Error;

pub struct CachedPersistentCell<T> {
    cache: Option<T>,
    storage: PersistentCell<T>,
}

impl<T> CachedPersistentCell<T>
where T: Persistable + Clone
{
    pub fn new(engine: &StorageEngine, key: MetaKey) -> Result<Self, Error> {
        let mut cell = Self {
            cache: None,
            storage: PersistentCell::new(engine, key),
        };
        cell.cache = cell.storage.get()?;
        Ok(cell)
    }

    pub fn set(&mut self, value: &T) -> Result<(), Error> {
        self.storage.set(value)?;
        self.cache = Some(value.clone());
        Ok(())
    }

    pub fn get(&self) -> Option<&T> {
        self.cache.as_ref()
    }

    pub fn delete(&mut  self) -> Result<(), Error> {
        self.storage.delete()?;
        self.cache = None;
        Ok(())
    }
}
