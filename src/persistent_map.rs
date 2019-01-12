use crate::keys::{FromKey, KeyPart, MetaKey};
use crate::storage_engine::{Persistable, StorageEngine};
use failure::{err_msg, Error};
use std::hash::Hash;
use std::marker::PhantomData;

pub struct PersistentMap<K, V> {
    engine: StorageEngine,
    prefix: MetaKey,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K, V> PersistentMap<K, V>
where
    K: Into<KeyPart> + FromKey + Clone + Eq + Hash,
    V: Persistable + Clone,
{
    pub fn new(engine: &StorageEngine, prefix: MetaKey) -> Self {
        Self {
            engine: engine.clone(),
            prefix,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    pub fn insert(&mut self, k: &K, v: &V) -> Result<(), Error> {
        self.engine.put_message(self.build_key(k), v)
    }

    #[allow(dead_code)]
    pub fn get(&self, k: &K) -> Result<Option<V>, Error> {
        self.engine.get_message(self.build_key(k))
    }

    pub fn delete(&mut self, k: &K) -> Result<(), Error> {
        self.engine.delete(self.build_key(k))
    }

    pub fn scan<F>(&self, mut f: F) -> Result<(), Error>
    where
        F: FnMut(K, V) -> Result<bool, Error>,
    {
        let prefix = self.prefix.clone();

        self.engine
            .clone()
            .scan_prefix(self.prefix.clone(), |k, v| {
                let stripped_key = prefix
                    .strip_prefix(k)
                    .ok_or_else(|| err_msg("Invalid prefix key"))?;
                let key = K::from_key(&stripped_key);
                let value = <V as Persistable>::from_bytes(v)?;
                f(key, value)
            })
    }

    fn build_key(&self, k: &K) -> MetaKey {
        self.prefix.clone().add(k.clone())
    }
}
