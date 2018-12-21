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
    pub fn new<P>(storage_root: P) -> Result<Self, Error>
        where P: AsRef<Path>
    {
        let path = storage_root.as_ref().join("cluster");
        let db = DB::open_default(path.to_str().unwrap()).map_err(err_msg)?;
        Ok(Self {
            db: Arc::new(db),
        })
    }

    pub fn get_message<M: Message>(&self, key: &[u8]) -> Result<Option<M>, Error> {
        let opt = self.db.get(key).map_err(err_msg)?;
        if let Some(inner) = opt {
            return Ok(Some(protobuf::parse_from_bytes(&inner)?))
        }
        Ok(None)
    }

    pub fn put_message<M: Message>(&self, key: &[u8], value: &M) -> Result<(), Error> {
        let bytes = value.write_to_bytes()?;
        Ok(self.db.put(key, &bytes).map_err(err_msg)?)
    }

    pub fn scan<F>(&self, start_key: &[u8], end_key: &[u8], mut f: F) -> Result<(), Error>
        where F: FnMut(&[u8], &[u8]) -> Result<bool, Error>
    {
        let mut options = ReadOptions::new();
        options.set_iterate_lower_bound(start_key);
        options.set_iterate_upper_bound(end_key);
        let mut it = self.db.iter_opt(options);
        it.seek(start_key.into());
        while it.valid() {
            let r = f(it.key(), it.value())?;
            if !r || !it.next() {
                break;
            }
        }
        Ok(())
    }

    pub fn batch(&self) -> MessageWriteBatch {
        MessageWriteBatch::new(self.db.clone())
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

    pub fn put<M: Message>(&mut self, key: &[u8], value: &M) -> Result<(), Error> {
        let bytes = value.write_to_bytes()?;
        self.batch.put(key, &bytes).map_err(err_msg)
    }

    pub fn commit(self) -> Result<(), Error> {
        self.db.write(self.batch).map_err(err_msg)
    }
}

