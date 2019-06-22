use super::{Action, ActionContext};
use serde::*;
use actix_web::*;
use failure::Error;
use futures::prelude::*;

#[derive(Clone, Copy)]
pub struct RefreshAction;

pub struct RefreshRequest {
    name: String,
}

#[derive(Serialize)]
pub struct RefreshResponse {
}

impl Action for RefreshAction {
    type Path = String;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = RefreshRequest;
    type Response = RefreshResponse;

    fn method(&self) -> actix_web::http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}/_refresh".to_string()
    }

    fn parse_http(&self, name: String, _request: &HttpRequest)
        -> Self::ParseFuture
    {
        Ok(RefreshRequest { name })
    }

    fn to_http_response(&self, response: RefreshResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(&self, request: RefreshRequest, ctx: ActionContext)
        -> Box<Future<Item=Self::Response, Error=Error>>
    {
        let f = ctx.node_router.refresh_index(&request.name).map(|_| RefreshResponse {});
        Box::new(f)
    }
}
