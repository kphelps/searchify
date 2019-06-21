use super::{Action, ActionContext};
use serde::*;
use actix_web::{HttpRequest, HttpResponse};
use failure::Error;
use futures::prelude::*;

#[derive(Clone, Copy)]
pub struct GetDocumentAction;

pub struct GetDocumentRequest {
    name: String,
    id: String,
}

#[derive(Serialize)]
struct Index {
    index_name: String,
    shard_count: u64,
    replica_count: u64,
}

#[derive(Serialize)]
pub struct GetDocumentResponse {
    indices: Vec<Index>,
}

impl Action for GetDocumentAction {
    type Path = (String, String);
    type ParseFuture = Result<Self::Request, Error>;
    type Request = GetDocumentRequest;
    type Response = GetDocumentResponse;

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::GET
    }

    fn path(&self) -> String {
        "/{name}/{id}".to_string()
    }

    fn parse_http(&self, (name, id): (String, String), _request: &HttpRequest)
        -> Result<GetDocumentRequest, Error>
    {
        Ok(GetDocumentRequest { name, id })
    }

    fn to_http_response(&self, response: GetDocumentResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(&self, request: GetDocumentRequest, ctx: ActionContext) -> Box<Future<Item=Self::Response, Error=Error>> {
        let f = ctx.node_router.get_document(request.name.clone(), request.id.clone().into())
            .and_then(move |response| {
                let source = if response.found {
                    let j = response.get_source();
                    let value = serde_json::from_slice(j)?;
                    Some(value)
                } else {
                    None
                };
                Ok(GetDocumentResponse{
                    index_name: request.name,
                    id: request.id,
                    found: response.found,
                    version: 0,
                    source: source,
                })
            });
        Box::new(f)
    }
}
