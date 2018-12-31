use crate::keys::IntoKey;
use failure::{err_msg, Error};
use protobuf::{self, Message};
use rocksdb::{DB, ReadOptions, Writable, WriteBatch};
use std::path::Path;
use std::sync::Arc;

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

    pub fn get_message<M, K>(&self, key: K) -> Result<Option<M>, Error>
    where K: IntoKey,
          M: Message
    {
        let opt = self.db.get(&key.into_key()).map_err(err_msg)?;
        if let Some(inner) = opt {
            return Ok(Some(protobuf::parse_from_bytes(&inner)?))
        }
        Ok(None)
    }

    pub fn put_message<M, K>(&self, key: K, value: &M) -> Result<(), Error>
    where K: IntoKey,
          M: Message
    {
        let bytes = value.write_to_bytes()?;
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

    pub fn put<M, K>(&mut self, key: K, value: &M) -> Result<(), Error>
    where K: IntoKey,
          M: Message
    {
        let bytes = value.write_to_bytes()?;
        self.batch.put(&key.into_key(), &bytes).map_err(err_msg)
    }

    pub fn commit(self) -> Result<(), Error> {
        self.db.write(self.batch).map_err(err_msg)
    }
}

