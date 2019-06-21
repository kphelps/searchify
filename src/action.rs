use actix_web::{HttpRequest, HttpResponse};
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

pub trait Action {
    type Path;
    type Request;
    type Response;

    fn handle_http<F>(&self, (request): (&HttpRequest)) -> F
        where F: Future<Item=HttpResponse, Error=Error> + 'static
    {
        // let action_request = self.parse_http(path.into_inner(), request);
        // let f = state.action_executor.execute(action, action_request)
        //     .map(|action_response| action.to_http_response(action_response));
        // Box::new(f)
        
    }
    fn method(&self) -> actix_web::http::Method;
    fn path(&self) -> String;
    fn parse_http(&self, path: Self::Path, request: &HttpRequest) -> Self::Request;
    fn to_http_response(&self, response: Self::Response) -> HttpResponse;
    fn execute(&self, request: Self::Request, ctx: ActionContext) -> Box<Future<Item=Self::Response, Error=Error>>;
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

impl Action for BulkAction {
    type Path = String;
    type Request = BulkRequest;
    type Response = BulkResponse;

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}/_bulk".to_string()
    }

    fn parse_http(&self, index: String, request: &HttpRequest) -> BulkRequest {
        BulkRequest {
            index,
            operations: Vec::new()
        }
    }

    fn to_http_response(&self, response: BulkResponse) -> HttpResponse {
        HttpResponse::NoContent().finish()
    }

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

impl Action for DeleteIndexAction {
    type Path = String;
    type Request = DeleteIndexRequest;
    type Response = ();

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::DELETE
    }

    fn path(&self) -> String {
        "/{name}".to_string()
    }

    fn parse_http(&self, index: String, request: &HttpRequest) -> DeleteIndexRequest {
        DeleteIndexRequest { name: index }
    }

    fn to_http_response(&self, response: ()) -> HttpResponse {
        HttpResponse::NoContent().finish()
    }

    fn execute(&self, request: DeleteIndexRequest, ctx: ActionContext) -> Box<Future<Item=Self::Response, Error=Error>> {
        Box::new(ctx.node_router.delete_index(request.name))
    }
}
