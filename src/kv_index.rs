use failure::{Error, Fail};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::hash::Hash;

#[derive(Debug, Fail)]
enum KvIndexError {
    #[fail(display = "unique index violation")]
    UniqueIndexViolation,
}

pub trait Keyable<K> {
    type Value;

    fn get(&self, k: &K) -> Option<Self::Value>;
}

pub struct KvIndex<K> {
    index: HashMap<K, HashSet<u64>>,
}

pub struct UniqueKvIndex<K> {
    index: HashMap<K, u64>,
}

impl<K> KvIndex<K>
    where K: Eq + Hash
{
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    pub fn get<T>(&self, key: &K, data: &T) -> Vec<T::Value>
        where T: Keyable<u64>
    {
        if let Some(values) = self.index.get(key) {
            values.iter()
                .map(|id| data.get(id).expect("Index out of sync"))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn insert(&mut self, id: u64, key: K) {
        let set = self.index.entry(key).or_insert_with(HashSet::new);
        set.insert(id);
    }

    pub fn bulk_insert(&mut self, id: u64, keys: Vec<K>) {
        keys.into_iter().for_each(|key| self.insert(id, key));
    }

    pub fn remove(&mut self, id: u64, key: K) {
        let set = self.index.entry(key).or_insert_with(HashSet::new);
        set.remove(&id);
    }

    pub fn bulk_remove(&mut self, id: u64, keys: Vec<K>) {
        keys.into_iter().for_each(|key| self.remove(id, key));
    }
}

impl<K> UniqueKvIndex<K>
    where K: Eq + Hash
{
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    pub fn get<T>(&self, key: &K, data: &T) -> Option<T::Value>
        where T: Keyable<u64>
    {
        self.index.get(key).map(|id| data.get(id).expect("Index out of sync"))
    }

    pub fn insert(&mut self, id: u64, key: K) -> Result<(), Error> {
        match self.index.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(id);
                Ok(())
            },
            Entry::Occupied(_) => Err(KvIndexError::UniqueIndexViolation.into()),
        }
    }

    pub fn remove(&mut self, key: &K) {
        self.index.remove(key);
    }
}
