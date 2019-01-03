use crate::proto::*;
use crate::raft::{
    FutureStateMachineObserver,
    RaftPropose,
    RaftStateMachine,
};
use crate::search_storage::SearchStorage;
use failure::Error;
use futures::sync::oneshot::Sender;
use std::path::Path;
use tantivy::Document;

pub struct SearchStateMachine {
    storage: SearchStorage
}

type SimpleObserver<T, F> = FutureStateMachineObserver<T, F>;
type SimplePropose<T, F> = RaftPropose<SimpleObserver<T, F>, SearchStateMachine>;

impl RaftStateMachine for SearchStateMachine {
    type EntryType = SearchEntry;

    fn apply(&mut self, entry: SearchEntry) {
        if entry.operation.is_none() {
            return
        }
        match entry.operation.unwrap() {
            SearchEntry_oneof_operation::add_document(operation) => { self.add_document(operation); },
        }
    }
}

impl SearchStateMachine {

    pub fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self{
            storage: SearchStorage::new(path)?,
        })
    }

    fn add_document(&mut self, request: AddDocumentOperation) -> Result<(), Error> {
        let document = Document::new();
        self.storage.index(document)
    }

    pub fn propose_add_document(request: IndexDocumentRequest, sender: Sender<IndexDocumentResponse>)
        -> SimplePropose<IndexDocumentResponse, impl FnOnce(&Self) -> IndexDocumentResponse>
    {
        let mut entry = SearchEntry::new();
        let operation = AddDocumentOperation::new();
        entry.set_add_document(operation);
        let observer = SimpleObserver::new(
            sender,
            |_: &Self| IndexDocumentResponse::new(),
        );
        SimplePropose::new_for_group(request.shard_id, entry, observer)
    }
}
