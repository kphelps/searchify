use super::{Action, ActionContext};
use actix_web::{web::Payload, *};
use failure::Error;
use futures::prelude::*;
use log::*;
use crate::node_router::SearchError;

#[derive(Clone, Copy)]
pub struct IndexExistsAction;

pub struct IndexExistsRequest {
    name: String,
}

impl Action for IndexExistsAction {
    type Path = String;
    type Payload = Payload;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = IndexExistsRequest;
    type Response = bool;

    fn method(&self) -> actix_web::http::Method {
        http::Method::HEAD
    }

    fn path(&self) -> String {
        "/{name}".to_string()
    }

    fn parse_http(
        &self,
        name: String,
        _request: &HttpRequest,
        _payload: Self::Payload,
    ) -> Self::ParseFuture {
        Ok(IndexExistsRequest { name })
    }

    fn to_http_response(&self, exists: bool) -> HttpResponse {
        if exists {
            HttpResponse::NoContent().finish()
        } else {
            HttpResponse::NotFound().finish()
        }
    }

    fn execute(
        &self,
        request: IndexExistsRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let f = ctx.node_router
            .get_index(request.name)
            .then(|result| {
                info!("Err: {:?}", result);
                match result {
                    Ok(_) => Ok(true),
                    Err(SearchError::IndexNotFound) => Ok(false),
                    Err(err) => Err(err.into()),
                }
            });
        Box::new(f)
    }
}
