use crate::keys::MetaKey;
use crate::storage_engine::{MessageWriteBatch, Persistable, StorageEngine};
use failure::Error;
use std::marker::PhantomData;

pub struct PersistentCell<T> {
    engine: StorageEngine,
    key: MetaKey,
    _data: PhantomData<T>,
}

impl<T> PersistentCell<T>
where
    T: Persistable + Clone,
{
    pub fn new(engine: &StorageEngine, key: MetaKey) -> Self {
        Self {
            engine: engine.clone(),
            key,
            _data: PhantomData,
        }
    }

    pub fn set(&self, value: &T) -> Result<(), Error> {
        self.engine.put_message(&self.key, value)
    }

    #[allow(dead_code)]
    pub fn batch_set(&self, batch: &mut MessageWriteBatch, value: &T) -> Result<(), Error> {
        batch.put(&self.key, value)
    }

    pub fn get(&self) -> Result<Option<T>, Error> {
        self.engine.get_message(&self.key)
    }

    #[allow(dead_code)]
    pub fn delete(&self) -> Result<(), Error> {
        self.engine.delete(&self.key)
    }
}
