use super::{Action, ActionContext, ShardResultResponse};
use actix_web::{web::Payload, HttpRequest, HttpResponse};
use failure::Error;
use futures::prelude::*;
use serde::*;

#[derive(Clone, Copy)]
pub struct DeleteDocumentAction;

pub struct DeleteDocumentRequest {
    name: String,
    id: String,
}

#[derive(Serialize)]
pub struct DeleteDocumentResponse {
    #[serde(rename = "_shards")]
    shards: ShardResultResponse,
    #[serde(rename = "_index")]
    index_name: String,
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_version")]
    version: String,
    result: String,
}

impl Action for DeleteDocumentAction {
    type Path = (String, String);
    type Payload = Payload;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = DeleteDocumentRequest;
    type Response = DeleteDocumentResponse;

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::DELETE
    }

    fn path(&self) -> String {
        "/{name}/{id}".to_string()
    }

    fn parse_http(
        &self,
        (name, id): (String, String),
        _request: &HttpRequest,
        _payload: Self::Payload,
    ) -> Result<DeleteDocumentRequest, Error> {
        Ok(DeleteDocumentRequest { name, id })
    }

    fn to_http_response(&self, response: DeleteDocumentResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(
        &self,
        request: DeleteDocumentRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let f = ctx
            .node_router
            .delete_document(request.name.clone(), request.id.clone().into())
            .and_then(move |response| {
                Ok(DeleteDocumentResponse {
                    shards: ShardResultResponse {
                        total: 1,
                        successful: if response.success { 1 } else { 0 },
                        failed: if response.success { 0 } else { 1 },
                        skipped: None,
                    },
                    index_name: request.name,
                    id: request.id,
                    version: "0".to_string(),
                    result: "deleted".to_string(),
                })
            });
        Box::new(f)
    }
}
