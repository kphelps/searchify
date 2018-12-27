use crate::proto::SearchEntry;
use crate::raft::RaftStateMachine;

pub struct SearchStateMachine {
}

impl SearchStateMachine {

    // TODO
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self{}
    }
}

impl RaftStateMachine for SearchStateMachine {
    type EntryType = SearchEntry;

    fn apply(&mut self, _entry: SearchEntry) {
    }
}
