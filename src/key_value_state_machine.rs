use crate::proto::{
    KeyValueEntry,
    KeyValueEntry_oneof_entry,
};
use crate::raft::{
    RaftStateMachine,
};

pub struct KeyValueStateMachine {
}

impl KeyValueStateMachine {
    pub fn new() -> Self {
        Self{
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
            KeyValueEntry_oneof_entry::set(_kv) => {
            }
        }


    }
}
