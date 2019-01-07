use crate::keys::IntoKey;
use failure::{err_msg, Error};
use protobuf::{self, Message};
use rocksdb::{DB, ReadOptions, Writable, WriteBatch};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

pub trait Persistable: Sized {
    fn to_bytes(&self) -> Result<Vec<u8>, Error>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error>;
}

#[derive(Clone)]
pub struct StorageEngine {
    pub db: Arc<DB>,
}

impl StorageEngine {
    pub fn new<P>(path: P) -> Result<Self, Error>
        where P: AsRef<Path>
    {
        let db = DB::open_default(path.as_ref().to_str().unwrap()).map_err(err_msg)?;
        Ok(Self {
            db: Arc::new(db),
        })
    }

    pub fn get_message<P, K>(&self, key: K) -> Result<Option<P>, Error>
    where K: IntoKey,
          P: Persistable
    {
        let opt = self.db.get(&key.into_key()).map_err(err_msg)?;
        if let Some(inner) = opt {
            let parsed = <P as Persistable>::from_bytes(&inner)?;
            return Ok(Some(parsed));
        }
        Ok(None)
    }

    pub fn put_message<P, K>(&self, key: K, value: &P) -> Result<(), Error>
    where K: IntoKey,
          P: Persistable
    {
        let bytes = value.to_bytes()?;
        self.db.put(&key.into_key(), &bytes).map_err(err_msg)
    }

    pub fn scan<F, K1, K2>(&self, start_key: K1, end_key: K2, mut f: F) -> Result<(), Error>
    where F: FnMut(&[u8], &[u8]) -> Result<bool, Error>,
          K1: IntoKey,
          K2: IntoKey
    {
        let mut options = ReadOptions::new();
        let start_bytes: &[u8] = &start_key.into_key();
        options.set_iterate_lower_bound(start_bytes);
        options.set_iterate_upper_bound(&end_key.into_key());
        let mut it = self.db.iter_opt(options);
        it.seek(start_bytes.into());
        while it.valid() {
            let r = f(it.key(), it.value())?;
            if !r || !it.next() {
                break;
            }
        }
        Ok(())
    }

    pub fn scan_prefix<F, K>(&self, prefix_key: K, f: F) -> Result<(), Error>
        where F: FnMut(&[u8], &[u8]) -> Result<bool, Error>,
              K: IntoKey
    {
        let (start, end) = prefix_key.into_prefix();
        self.scan(start, end, f)
    }

    pub fn batch(&self) -> MessageWriteBatch {
        MessageWriteBatch::new(self.db.clone())
    }

    pub fn delete<K>(&self, key: K) -> Result<(), Error>
        where K: IntoKey
    {
        self.db.delete(&key.into_key()).map_err(err_msg)
    }
}

pub struct MessageWriteBatch {
    db: Arc<DB>,
    batch: WriteBatch,
}

impl MessageWriteBatch {
    pub fn new(db: Arc<DB>) -> Self {
        let batch = WriteBatch::new();
        Self {
            db,
            batch,
        }
    }

    pub fn put<P, K>(&mut self, key: K, value: &P) -> Result<(), Error>
    where K: IntoKey,
          P: Persistable
    {
        let bytes = value.to_bytes()?;
        self.batch.put(&key.into_key(), &bytes).map_err(err_msg)
    }

    pub fn commit(self) -> Result<(), Error> {
        self.db.write(self.batch).map_err(err_msg)
    }
}

impl<T> Persistable for T where T: Message {
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.write_to_bytes().map_err(|e| e.into())
    }

    fn from_bytes(bytes: &[u8]) -> Result<T, Error> {
        protobuf::parse_from_bytes(bytes).map_err(|e| e.into())
    }
}

macro_rules! impl_persistable {
    ($tipe:ty) => {
        impl $crate::storage_engine::Persistable for $tipe {
            fn to_bytes(&self) -> Result<Vec<u8>, failure::Error> {
                Ok(serde_json::to_vec(self)?)
            }

            fn from_bytes(bytes: &[u8]) -> Result<$tipe, failure::Error> {
                Ok(serde_json::from_slice(bytes)?)
            }
        }
    }
}
