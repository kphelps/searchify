use super::{Action, ActionContext, Index};
use actix_web::{web::Payload, HttpRequest, HttpResponse};
use failure::Error;
use futures::prelude::*;
use serde::*;

#[derive(Clone, Copy)]
pub struct ClusterHealthAction;

#[derive(Serialize)]
pub enum ClusterStatus {
    Green,
    Yellow,
    Red,
}

#[derive(Serialize)]
pub struct ClusterHealthResponse {
    status: ClusterStatus,
    number_of_nodes: u64,
    indices: u64,
    replica_sets: u64,
    active_shards: u64,
    unassigned_shards: u64,
}

impl Action for ClusterHealthAction {
    type Path = ();
    type Payload = Payload;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = ();
    type Response = ClusterHealthResponse;

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::GET
    }

    fn path(&self) -> String {
        "/_cluster/health".to_string()
    }

    fn parse_http(
        &self,
        _: (),
        _request: &HttpRequest,
        _payload: Self::Payload,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn to_http_response(&self, response: ClusterHealthResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(
        &self,
        _: (),
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let f = ctx.node_router.leader_client()
            .into_future()
            .from_err()
            .and_then(|client| client.health())
            .map(|response| {
                let status = ClusterStatus::Green;
                ClusterHealthResponse{
                    status,
                    number_of_nodes: response.number_of_nodes,
                    indices: response.indices,
                    replica_sets: response.replica_sets,
                    active_shards: response.active_shards,
                    unassigned_shards: response.unassigned_shards,
                }
            });
        Box::new(f)
    }
}
