use super::{Action, ActionContext};
use actix_web::{web::Payload, *};
use failure::Error;
use futures::prelude::*;
use serde::*;

#[derive(Clone, Copy)]
pub struct RefreshAction;

pub struct RefreshRequest {
    name: String,
}

#[derive(Serialize)]
pub struct RefreshResponse {}

impl Action for RefreshAction {
    type Path = String;
    type Payload = Payload;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = RefreshRequest;
    type Response = RefreshResponse;

    fn method(&self) -> actix_web::http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}/_refresh".to_string()
    }

    fn parse_http(
        &self,
        name: String,
        _request: &HttpRequest,
        _payload: Self::Payload,
    ) -> Self::ParseFuture {
        Ok(RefreshRequest { name })
    }

    fn to_http_response(&self, response: RefreshResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(
        &self,
        request: RefreshRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let f = ctx
            .node_router
            .refresh_index(&request.name)
            .map(|_| RefreshResponse {})
            .from_err();
        Box::new(f)
    }
}
