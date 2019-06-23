use super::{Action, ActionContext};
use actix_web::{*, web::Payload};
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
    type ParseFuture = Box<Future<Item = Self::Request, Error = Error>>;
    type Request = IndexDocumentRequest;
    type Response = IndexDocumentResponse;

    fn method(&self) -> actix_web::http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}/{id}".to_string()
    }

    fn parse_http(&self, (name, id): (String, String), request: &HttpRequest, _payload: Payload) -> Self::ParseFuture {
        let f = web::Json::<JsonValue>::extract(&request)
            .map_err(|_| failure::err_msg("Failed to parse body"))
            .map(|j| j.into_inner())
            .map(|document| IndexDocumentRequest { name, id, document });
        Box::new(f)
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
