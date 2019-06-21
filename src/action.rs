use failure::Error;
use futures::prelude::*;
use crate::node_router::NodeRouterHandle;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

pub struct ActionContext {
    node_router: NodeRouterHandle,
}

impl ActionContext {
    pub fn new(node_router: NodeRouterHandle) -> Self {
        Self {
            node_router,
        }
    }
}

pub trait Action<Request> {
    type Response;

    fn execute(&self, request: Request, ctx: ActionContext) -> Box<Future<Item=Self::Response, Error=Error>>;
}

struct BulkRequest {
    index: String,
    operations: Vec<BulkOperation>,
}

struct BulkResponse {
}

enum BulkOperation {
    Create(CreateOperation),
    Update(UpdateOperation),
    Delete(DeleteOperation),
}

impl BulkOperation {
    fn document_id(&self) -> &str {
        match self {
            BulkOperation::Create(inner) => &inner.id,
            BulkOperation::Update(inner) => &inner.id,
            BulkOperation::Delete(inner) => &inner.id,
        }
    }
}

struct CreateOperation {
    id: String,
    document: JsonValue,
}

struct UpdateOperation {
    id: String,
    document: JsonValue,
}

struct DeleteOperation {
    id: String,
}

struct BulkAction;

impl Action<BulkRequest> for BulkAction {
    type Response = BulkResponse;

    fn execute(&self, request: BulkRequest, ctx: ActionContext) -> Box<Future<Item=Self::Response, Error=Error>> {
        let index = request.index;
        // let mut by_shard = HashMap::new();
        request.operations.into_iter().for_each(|operation| {
            let doc_id = operation.document_id();
            // let shard_id = ctx.node_router.get_shard_for_document(index, doc_id.into());
        });
        Box::new(futures::future::ok(BulkResponse{}))
    }
}

pub struct DeleteIndexAction;

struct DeleteIndexRequest {
    name: String,
}

impl Action<DeleteIndexRequest> for DeleteIndexAction {
    type Response = ();

    fn execute(&self, request: DeleteIndexRequest, ctx: ActionContext) -> Box<Future<Item=Self::Response, Error=Error>> {
        Box::new(ctx.node_router.delete_index(request.name))
    }
}
