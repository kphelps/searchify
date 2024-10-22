use crate::async_work_queue::AsyncWorkQueue;
use crate::document::ExpectedVersion;
use crate::mappings::Mappings;
use crate::proto::*;
use crate::raft::{
    FutureStateMachineObserver, RaftPropose, RaftStateMachine, StateMachineObserver,
};
use crate::search_storage::{SearchStorage, SearchStorageReader, SearchStorageWriter};
use failure::Error;
use futures::prelude::*;
use futures::sync::oneshot::Sender;
use std::path::Path;

pub struct SearchStateMachine {
    storage: SearchStorage,
    apply_queue: AsyncWorkQueue<SearchEntry>,
}

type SimpleObserver<T, F> = FutureStateMachineObserver<T, F>;
type SimplePropose = RaftPropose<SearchStateMachine>;

impl RaftStateMachine for SearchStateMachine {
    type EntryType = SearchEntry;
    type Observable = SearchStorageReader;

    fn apply(
        &mut self,
        id: u64,
        entry: SearchEntry,
        observer: Option<Box<dyn StateMachineObserver<Self> + Send + Sync>>,
    ) -> Result<(), Error> {
        let reader = self.storage.reader();
        let f = self
            .apply_queue
            .push(id, entry)
            .map(move |_| {
                if let Some(observer) = observer {
                    observer.observe(&reader);
                }
            })
            .map_err(|_| ());
        tokio::spawn(f);
        Ok(())
    }

    fn peers(&self) -> Result<Vec<u64>, Error> {
        Ok(Vec::new())
    }

    fn last_applied(&self) -> u64 {
        self.apply_queue.last_handled()
    }
}

struct SearchEntryApplier {
    storage: SearchStorageWriter,
}

impl SearchStateMachine {
    pub fn new(path: impl AsRef<Path>, mappings: Mappings) -> Result<Self, Error> {
        let storage = SearchStorage::new(path, mappings)?;
        let mut applier = SearchEntryApplier {
            storage: storage.writer(),
        };
        let apply_queue = AsyncWorkQueue::new(move |entry: SearchEntry| applier.apply(entry));
        Ok(Self {
            storage,
            apply_queue,
        })
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
        let observer = SimpleObserver::new(sender, |_: &SearchStorageReader| {
            IndexDocumentResponse::new()
        });
        SimplePropose::new_for_group(request.shard_id, entry, observer)
    }

    pub fn propose_bulk(request: BulkRequest, sender: Sender<BulkResponse>) -> SimplePropose {
        let mut entry = SearchEntry::new();
        let mut operation = BulkEntry::new();

        let entries = request.operations.into_iter().map(|mut op| {
            let mut entry = BulkEntryItem::new();
            match op.get_op_type() {
                BulkOpType::INDEX => {
                    let mut add_doc = AddDocumentOperation::new();
                    add_doc.set_id(op.take_document_id());
                    add_doc.set_payload(String::from_utf8(op.take_payload()).unwrap());
                    entry.set_add_document(add_doc);
                }
                BulkOpType::CREATE => {
                    let mut add_doc = AddDocumentOperation::new();
                    add_doc.set_id(op.take_document_id());
                    add_doc.set_payload(String::from_utf8(op.take_payload()).unwrap());
                    entry.set_add_document(add_doc);
                }
                BulkOpType::UPDATE => {
                    let mut add_doc = AddDocumentOperation::new();
                    add_doc.set_id(op.take_document_id());
                    add_doc.set_payload(String::from_utf8(op.take_payload()).unwrap());
                    entry.set_add_document(add_doc);
                }
                BulkOpType::DELETE => {
                    let mut delete_doc = DeleteDocumentOperation::new();
                    delete_doc.set_id(op.take_document_id());
                    entry.set_delete_document(delete_doc);
                }
            };
            entry
        });
        operation.set_operations(entries.collect());

        entry.set_bulk(operation);
        let observer = SimpleObserver::new(sender, |_: &SearchStorageReader| BulkResponse::new());
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
        let observer = SimpleObserver::new(sender, |_: &SearchStorageReader| {
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
        Self::propose_read(request.shard_id, sender, move |sm: &SearchStorageReader| {
            sm.search(request)
        })
    }

    pub fn propose_refresh(
        request: RefreshRequest,
        sender: Sender<RefreshResponse>,
    ) -> SimplePropose {
        let mut entry = SearchEntry::new();
        let operation = RefreshOperation::new();
        entry.set_refresh(operation);
        let observer =
            SimpleObserver::new(sender, |_: &SearchStorageReader| RefreshResponse::new());
        SimplePropose::new_for_group(request.shard_id, entry, observer)
    }

    pub fn get_document(
        request: GetDocumentRequest,
        sender: Sender<Result<GetDocumentResponse, Error>>,
    ) -> SimplePropose {
        Self::propose_read(request.shard_id, sender, move |sm: &SearchStorageReader| {
            sm.get(request.document_id.into())
        })
    }

    fn propose_read<R, F>(shard_id: u64, sender: Sender<R>, f: F) -> SimplePropose
    where
        F: FnOnce(&SearchStorageReader) -> R + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        // TODO: This should go through the leaseholder and avoid raft altogether
        let entry = SearchEntry::new();
        let observer = SimpleObserver::new(sender, f);
        SimplePropose::new_for_group(shard_id, entry, observer)
    }
}

impl SearchEntryApplier {
    fn apply(&mut self, entry: SearchEntry) -> Result<bool, Error> {
        if entry.operation.is_none() {
            return Ok(false);
        }

        match entry.operation.unwrap() {
            SearchEntry_oneof_operation::add_document(operation) => {
                self.add_document(operation)?;
            }
            SearchEntry_oneof_operation::delete_document(operation) => {
                self.delete_document(operation)?;
            }
            SearchEntry_oneof_operation::refresh(_) => {
                self.refresh()?;
                return Ok(true);
            }
            SearchEntry_oneof_operation::bulk(bulk) => {
                self.bulk(bulk)?;
            }
        };
        Ok(false)
    }

    fn add_document(&mut self, mut request: AddDocumentOperation) -> Result<(), Error> {
        let document = serde_json::from_str(request.get_payload())?;
        self.storage
            .index(&request.take_id().into(), &document, ExpectedVersion::Any)
    }

    fn delete_document(&mut self, mut request: DeleteDocumentOperation) -> Result<(), Error> {
        self.storage.delete(request.take_id().into())
    }

    fn refresh(&mut self) -> Result<(), Error> {
        self.storage.refresh()
    }

    fn bulk(&mut self, request: BulkEntry) -> Result<(), Error> {
        request
            .operations
            .into_iter()
            .map(|item| {
                match item.operation.unwrap() {
                    BulkEntryItem_oneof_operation::add_document(operation) => {
                        self.add_document(operation)?;
                    }
                    BulkEntryItem_oneof_operation::delete_document(operation) => {
                        self.delete_document(operation)?;
                    }
                };
                Ok(())
            })
            .collect::<Result<(), Error>>()
    }
}
