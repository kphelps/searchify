use crate::proto::{
    EmptyResponse,
    Key,
    KeyValue,
    KeyValueEntry,
    KeyValueEntry_oneof_entry,
};
use crate::raft::{
    FutureStateMachineObserver,
    RaftPropose,
    RaftStateMachine,
};
use crate::storage_engine::StorageEngine;
use failure::{Error, err_msg};
use futures::sync::oneshot::Sender;
use rocksdb::Writable;

pub struct KeyValueStateMachine {
    engine: StorageEngine,
}

impl KeyValueStateMachine {
    pub fn new(engine: StorageEngine) -> Self {
        Self{
            engine,
        }
    }
}

impl RaftStateMachine for KeyValueStateMachine {
    type EntryType = KeyValueEntry;

    fn apply(&mut self, entry: KeyValueEntry) {
        if let None = entry.entry {
            return;
        }

        match entry.entry.unwrap() {
            KeyValueEntry_oneof_entry::set(kv) => { self.set(kv); },
        }
    }
}

type SimpleObserver<T, F> = FutureStateMachineObserver<T, F>;
type SimplePropose<T, F> = RaftPropose<SimpleObserver<T, F>, KeyValueStateMachine>;

impl KeyValueStateMachine {
    fn set(&mut self, key_value: KeyValue) -> Result<(), Error> {
        self.engine.db.put(&key_value.key, &key_value.value).map_err(err_msg)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.engine.db.get(key)
            .map_err(err_msg)
            .map(|opt| opt.map(|db_vec| db_vec.to_vec()))
    }

    pub fn propose_set(key_value: KeyValue, sender: Sender<EmptyResponse>)
        -> SimplePropose<EmptyResponse, impl FnOnce(&Self) -> EmptyResponse>
    {
        let mut entry = KeyValueEntry::new();
        entry.set_set(key_value);
        let observer = SimpleObserver::new(
            sender,
            |_: &KeyValueStateMachine| EmptyResponse::new(),
        );
        SimplePropose::new(entry, observer)
    }

    pub fn propose_get(key: Key, sender: Sender<KeyValue>)
        -> SimplePropose<KeyValue, impl FnOnce(&Self) -> KeyValue>
    {
        let entry = KeyValueEntry::new();
        let observer = SimpleObserver::new(
            sender,
            move |sm: &KeyValueStateMachine| {
                let value = sm.get(&key.key);
                let mut kv = KeyValue::new();
                kv.set_key(key.key);
                // TODO: need to differ between actual errors and not found
                if let Ok(Some(inner)) = value {
                    kv.set_value(inner);
                }
                kv
            },
        );
        SimplePropose::new(entry, observer)
    }
}
