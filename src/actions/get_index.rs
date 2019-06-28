use super::{Action, ActionContext, Index};
use actix_web::{web::Payload, *};
use failure::Error;
use futures::prelude::*;

#[derive(Clone, Copy)]
pub struct GetIndexAction;

pub struct GetIndexRequest {
    name: String,
}

impl Action for GetIndexAction {
    type Path = String;
    type Payload = Payload;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = GetIndexRequest;
    type Response = Index;

    fn method(&self) -> actix_web::http::Method {
        http::Method::GET
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
        Ok(GetIndexRequest { name })
    }

    fn to_http_response(&self, response: Index) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(
        &self,
        request: GetIndexRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let f = ctx.node_router.get_index(request.name).map(|state| Index {
            index_name: state.name,
            shard_count: state.shard_count,
            replica_count: state.replica_count,
        }).from_err();
        Box::new(f)
    }
}
