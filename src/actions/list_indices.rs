use super::{Action, ActionContext, Index};
use actix_web::{HttpRequest, HttpResponse, web::Payload};
use failure::Error;
use futures::prelude::*;
use serde::*;

#[derive(Clone, Copy)]
pub struct ListIndicesAction;

#[derive(Serialize)]
pub struct ListIndicesResponse {
    indices: Vec<Index>,
}

impl Action for ListIndicesAction {
    type Path = ();
    type ParseFuture = Result<Self::Request, Error>;
    type Request = ();
    type Response = ListIndicesResponse;

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::GET
    }

    fn path(&self) -> String {
        "/_cat/indices".to_string()
    }

    fn parse_http(&self, _: (), _request: &HttpRequest, _payload: Payload) -> Result<(), Error> {
        Ok(())
    }

    fn to_http_response(&self, response: ListIndicesResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(
        &self,
        _: (),
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let f = ctx.node_router.list_indices().map(|response| {
            let indices = response.indices.into_iter().map(|index| Index {
                index_name: index.name,
                shard_count: index.shard_count,
                replica_count: index.replica_count,
            });
            ListIndicesResponse {
                indices: indices.collect(),
            }
        });
        Box::new(f)
    }
}
