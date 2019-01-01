use crate::keys::{FromKey, KeyPart, MetaKey};
use crate::kv_index::Keyable;
use crate::persistent_map::PersistentMap;
use crate::storage_engine::StorageEngine;
use failure::Error;
use protobuf::Message;
use std::collections::HashMap;
use std::hash::Hash;

pub struct CachedPersistentMap<K, V> {
    persistent: PersistentMap<K, V>,
    cache: HashMap<K, V>,
}

impl<K, V> CachedPersistentMap<K, V>
where K: Into<KeyPart> + FromKey + Clone + Eq + Hash,
      V: Message + Clone
{
    pub fn new(engine: &StorageEngine, prefix: MetaKey) -> Result<Self, Error> {
        let mut map = Self {
            persistent: PersistentMap::new(engine, prefix),
            cache: HashMap::new(),
        };
        map.init_cache()?;
        Ok(map)
    }

    pub fn insert(&mut self, k: &K, v: &V) -> Result<(), Error> {
        self.persistent.insert(k, v)?;
        self.cache.insert(k.clone(), v.clone());
        Ok(())
    }

    pub fn delete(&mut self, k: &K) -> Result<V, Error> {
        self.persistent.delete(k)?;
        Ok(self.cache.remove(k).expect("Index out of sync"))
    }

    fn init_cache(&mut self) -> Result<(), Error> {
        let mut cache = HashMap::new();
        self.persistent.scan(|k, v| {
            cache.insert(k, v);
            Ok(true)
        })?;
        self.cache = cache;
        Ok(())
    }

    pub fn cache(&self) -> &HashMap<K, V> {
        &self.cache
    }
}

impl<K, V> Keyable<K> for CachedPersistentMap<K, V>
    where K: Eq + Hash,
        V: Clone
{
    type Value = V;

    fn get(&self, k: &K) -> Option<Self::Value> {
        self.cache.get(k).cloned()
    }
}
