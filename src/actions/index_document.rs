use super::{Action, ActionContext};
use actix_web::*;
use failure::Error;
use futures::prelude::*;
use serde::*;
use serde_json::Value as JsonValue;

#[derive(Clone, Copy)]
pub struct IndexDocumentAction;

pub struct IndexDocumentRequest {
    name: String,
    id: String,
    document: JsonValue,
}

#[derive(Serialize)]
pub struct IndexDocumentResponse {}

impl Action for IndexDocumentAction {
    type Path = (String, String);
    type Payload = web::Json<JsonValue>;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = IndexDocumentRequest;
    type Response = IndexDocumentResponse;

    fn method(&self) -> actix_web::http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}/{id}".to_string()
    }

    fn parse_http(
        &self,
        (name, id): (String, String),
        _request: &HttpRequest,
        document: Self::Payload,
    ) -> Self::ParseFuture {
        Ok(IndexDocumentRequest {
            name,
            id,
            document: document.into_inner(),
        })
    }

    fn to_http_response(&self, response: IndexDocumentResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(
        &self,
        request: IndexDocumentRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let f = ctx
            .node_router
            .index_document(request.name, request.id.into(), request.document)
            .map(|_| IndexDocumentResponse {});
        Box::new(f)
    }
}
