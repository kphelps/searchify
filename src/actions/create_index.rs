use crate::mappings::Mappings;
use super::{Action, ActionContext};
use serde::*;
use actix_web::*;
use failure::Error;
use futures::prelude::*;

#[derive(Clone, Copy)]
pub struct CreateIndexAction;

#[derive(Deserialize)]
struct IndexSettings {
    number_of_shards: u64,
    number_of_replicas: u64,
}

#[derive(Deserialize)]
struct CreateIndexBody {
    settings: IndexSettings,
    mappings: Mappings,
}

pub struct CreateIndexRequest {
    name: String,
    settings: IndexSettings,
    mappings: Mappings,
}

#[derive(Serialize)]
pub struct CreateIndexResponse {
    index_name: String,
}

impl Action for CreateIndexAction {
    type Path = String;
    type ParseFuture = Box<Future<Item = Self::Request, Error = Error>>;
    type Request = CreateIndexRequest;
    type Response = CreateIndexResponse;

    fn method(&self) -> actix_web::http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}".to_string()
    }

    fn parse_http(&self, name: String, request: &HttpRequest)
        -> Self::ParseFuture
    {
        let f = web::Json::<CreateIndexBody>::extract(&request)
            .map_err(|_| failure::err_msg("Failed to parse body"))
            .map(|j| j.into_inner())
            .map(|body| CreateIndexRequest {
                name,
                settings: body.settings,
                mappings: body.mappings,
            });
        Box::new(f)
    }

    fn to_http_response(&self, response: CreateIndexResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(&self, request: CreateIndexRequest, ctx: ActionContext)
        -> Box<Future<Item=Self::Response, Error=Error>>
    {
        let index_name = request.name.clone();
        let action = ctx.node_router.create_index(
            request.name.clone(),
            request.settings.number_of_shards,
            request.settings.number_of_replicas,
            request.mappings,
        ).from_err();
        let f = action.map(move |_| CreateIndexResponse { index_name });
        Box::new(f)
    }
}
