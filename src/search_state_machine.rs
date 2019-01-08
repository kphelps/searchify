use crate::mappings::Mappings;
use crate::proto::*;
use crate::raft::{FutureStateMachineObserver, RaftPropose, RaftStateMachine};
use crate::search_storage::SearchStorage;
use failure::Error;
use futures::sync::oneshot::Sender;
use futures::{future, Future};
use std::path::Path;

pub struct SearchStateMachine {
    storage: SearchStorage,
    mappings: Mappings,
}

type SimpleObserver<T, F> = FutureStateMachineObserver<T, F>;
type SimplePropose<T, F> = RaftPropose<SimpleObserver<T, F>, SearchStateMachine>;

impl RaftStateMachine for SearchStateMachine {
    type EntryType = SearchEntry;

    fn apply(&mut self, entry: SearchEntry) -> Result<(), Error> {
        if entry.operation.is_none() {
            return Ok(());
        }
        match entry.operation.unwrap() {
            SearchEntry_oneof_operation::add_document(operation) => self.add_document(operation),
        }
    }
}

impl SearchStateMachine {
    pub fn new(shard_id: u64, path: impl AsRef<Path>, mappings: Mappings) -> Result<Self, Error> {
        let storage = SearchStorage::new(shard_id, path, mappings.schema()?)?;
        Ok(Self { storage, mappings })
    }

    fn add_document(&mut self, request: AddDocumentOperation) -> Result<(), Error> {
        let document: serde_json::Value = serde_json::from_str(request.get_payload())?;
        let mapped_doc = self.mappings.map_to_document(&document)?;
        self.storage.index(mapped_doc)
    }

    pub fn propose_add_document(
        mut request: IndexDocumentRequest,
        sender: Sender<IndexDocumentResponse>,
    ) -> SimplePropose<IndexDocumentResponse, impl FnOnce(&Self) -> IndexDocumentResponse> {
        let mut entry = SearchEntry::new();
        let mut operation = AddDocumentOperation::new();
        operation.set_payload(request.take_payload());
        entry.set_add_document(operation);
        let observer = SimpleObserver::new(sender, |_: &Self| IndexDocumentResponse::new());
        SimplePropose::new_for_group(request.shard_id, entry, observer)
    }

    pub fn search(
        request: SearchRequest,
        sender: Sender<Result<SearchResponse, Error>>,
    ) -> SimplePropose<
        Result<SearchResponse, Error>,
        impl FnOnce(&Self) -> Result<SearchResponse, Error>,
    > {
        let entry = SearchEntry::new();
        let shard_id = request.shard_id;
        // TODO: This should go through the leaseholder and avoid raft altogether
        let observer = SimpleObserver::new(sender, move |sm: &Self| sm.storage.search(request));
        SimplePropose::new_for_group(shard_id, entry, observer)
    }
}
