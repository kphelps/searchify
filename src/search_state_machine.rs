use crate::document::ExpectedVersion;
use crate::mappings::Mappings;
use crate::proto::*;
use crate::raft::{FutureStateMachineObserver, RaftPropose, RaftStateMachine};
use crate::search_storage::SearchStorage;
use failure::Error;
use futures::sync::oneshot::Sender;
use std::path::Path;

pub struct SearchStateMachine {
    storage: SearchStorage,
}

type SimpleObserver<T, F> = FutureStateMachineObserver<T, F>;
type SimplePropose = RaftPropose<SearchStateMachine>;

impl RaftStateMachine for SearchStateMachine {
    type EntryType = SearchEntry;

    fn apply(&mut self, entry: SearchEntry) -> Result<bool, Error> {
        if entry.operation.is_none() {
            return Ok(false);
        }
        let committed = match entry.operation.unwrap() {
            SearchEntry_oneof_operation::add_document(operation) => {
                self.add_document(operation)?;
                false
            }
            SearchEntry_oneof_operation::delete_document(operation) => {
                self.delete_document(operation)?;
                false
            }
            SearchEntry_oneof_operation::refresh(_) => {
                self.refresh()?;
                true
            }
        };
        Ok(committed)
    }
}

impl SearchStateMachine {
    pub fn new(shard_id: u64, path: impl AsRef<Path>, mappings: Mappings) -> Result<Self, Error> {
        let storage = SearchStorage::new(shard_id, path, mappings)?;
        Ok(Self { storage })
    }

    fn add_document(&mut self, mut request: AddDocumentOperation) -> Result<(), Error> {
        let document = serde_json::from_str(request.get_payload())?;
        self.storage.index(
            &request.take_id().into(),
            &document,
            ExpectedVersion::Any,
        )
    }

    fn delete_document(&mut self, mut request: DeleteDocumentOperation) -> Result<(), Error> {
        self.storage.delete(request.take_id().into())
    }

    fn refresh(&mut self) -> Result<(), Error> {
        self.storage.refresh()
    }

    pub fn propose_add_document(
        mut request: IndexDocumentRequest,
        sender: Sender<IndexDocumentResponse>,
    ) -> SimplePropose {
        let mut entry = SearchEntry::new();
        let mut operation = AddDocumentOperation::new();
        operation.set_payload(request.take_payload());
        operation.set_id(request.take_document_id());
        entry.set_add_document(operation);
        let observer = SimpleObserver::new(sender, |_: &Self| IndexDocumentResponse::new());
        SimplePropose::new_for_group(request.shard_id, entry, observer)
    }

    pub fn propose_delete_document(
        mut request: DeleteDocumentRequest,
        sender: Sender<DeleteDocumentResponse>,
    ) -> SimplePropose {
        let mut entry = SearchEntry::new();
        let mut operation = DeleteDocumentOperation::new();
        operation.set_id(request.take_document_id());
        entry.set_delete_document(operation);
        let observer = SimpleObserver::new(sender, |_: &Self| {
            let mut response = DeleteDocumentResponse::new();
            response.success = true;
            response
        });
        SimplePropose::new_for_group(request.shard_id, entry, observer)
    }

    pub fn search(
        request: SearchRequest,
        sender: Sender<Result<SearchResponse, Error>>,
    ) -> SimplePropose {
        Self::propose_read(request.shard_id, sender, move |sm: &Self| {
            sm.storage.search(request)
        })
    }

    pub fn propose_refresh(
        request: RefreshRequest,
        sender: Sender<RefreshResponse>,
    ) -> SimplePropose {
        let mut entry = SearchEntry::new();
        let operation = RefreshOperation::new();
        entry.set_refresh(operation);
        let observer = SimpleObserver::new(sender, |_: &Self| RefreshResponse::new());
        SimplePropose::new_for_group(request.shard_id, entry, observer)
    }

    pub fn get_document(
        request: GetDocumentRequest,
        sender: Sender<Result<GetDocumentResponse, Error>>,
    ) -> SimplePropose {
        Self::propose_read(request.shard_id, sender, move |sm: &Self| {
            sm.storage.get(request.document_id.into())
        })
    }

    fn propose_read<R, F>(shard_id: u64, sender: Sender<R>, f: F) -> SimplePropose
    where
        F: FnOnce(&Self) -> R + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        // TODO: This should go through the leaseholder and avoid raft altogether
        let entry = SearchEntry::new();
        let observer = SimpleObserver::new(sender, f);
        SimplePropose::new_for_group(shard_id, entry, observer)
    }
}
