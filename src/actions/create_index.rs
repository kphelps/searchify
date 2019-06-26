use super::{Action, ActionContext};
use crate::mappings::Mappings;
use actix_web::*;
use failure::Error;
use futures::prelude::*;
use serde::*;

#[derive(Clone, Copy)]
pub struct CreateIndexAction;

#[derive(Deserialize)]
struct IndexSettings {
    number_of_shards: u64,
    number_of_replicas: u64,
}

#[derive(Deserialize)]
pub struct CreateIndexBody {
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
    type Payload = web::Json<CreateIndexBody>;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = CreateIndexRequest;
    type Response = CreateIndexResponse;

    fn method(&self) -> actix_web::http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}".to_string()
    }

    fn parse_http(
        &self,
        name: String,
        _request: &HttpRequest,
        body: Self::Payload,
    ) -> Self::ParseFuture {
        let body = body.into_inner();
        Ok(CreateIndexRequest {
            name,
            settings: body.settings,
            mappings: body.mappings,
        })
    }

    fn to_http_response(&self, response: CreateIndexResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(
        &self,
        request: CreateIndexRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let index_name = request.name.clone();
        let action = ctx
            .node_router
            .create_index(
                request.name.clone(),
                request.settings.number_of_shards,
                request.settings.number_of_replicas,
                request.mappings,
            )
            .from_err();
        let f = action.map(move |_| CreateIndexResponse { index_name });
        Box::new(f)
    }
}
