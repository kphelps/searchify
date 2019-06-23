use super::{Action, ActionContext, ShardResultResponse};
use crate::proto::MergedSearchResponse;
use crate::query_api::SearchQuery;
use actix_web::{*, web::Payload};
use failure::Error;
use futures::prelude::*;
use serde::*;
use std::time::Instant;

#[derive(Clone, Copy)]
pub struct SearchAction;

#[derive(Deserialize)]
pub struct SearchBody {
    query: SearchQuery,
}

pub struct SearchRequest {
    name: String,
    body: SearchBody,
}

#[derive(Serialize)]
pub struct SearchResponse {
    took: u64,
    timed_out: bool,
    shards: ShardResultResponse,
    hits: Vec<SearchHitResponse>,
}

#[derive(Serialize)]
pub struct SearchHitResponse {
    #[serde(rename = "_index")]
    index_name: String,
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_score")]
    score: f32,
    #[serde(rename = "_source")]
    source: Option<serde_json::Value>,
}

impl Action for SearchAction {
    type Path = String;
    type ParseFuture = Box<Future<Item = Self::Request, Error = Error>>;
    type Request = SearchRequest;
    type Response = SearchResponse;

    fn method(&self) -> actix_web::http::Method {
        http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}".to_string()
    }

    fn parse_http(&self, name: String, request: &HttpRequest, _payload: Payload) -> Self::ParseFuture {
        let f = web::Json::<SearchBody>::extract(&request)
            .map_err(|_| failure::err_msg("Failed to parse body"))
            .map(|j| j.into_inner())
            .map(|body| SearchRequest { name, body });
        Box::new(f)
    }

    fn to_http_response(&self, response: SearchResponse) -> HttpResponse {
        HttpResponse::Ok().json(response)
    }

    fn execute(
        &self,
        request: SearchRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let start = Instant::now();
        let query_string = serde_json::to_vec(&request.body.query).unwrap();
        let f = ctx
            .node_router
            .search(request.name.clone(), query_string)
            .and_then(move |result| process_search_results(result, request.name, start));
        Box::new(f)
    }
}

fn process_search_results(
    result: MergedSearchResponse,
    index_name: String,
    start: Instant,
) -> Result<SearchResponse, Error> {
    let dt = start.elapsed();
    let took = dt.as_secs() as u64 * 1000 + dt.subsec_millis() as u64;
    let hits = result
        .get_hits()
        .into_iter()
        .map(|hit| {
            Ok(SearchHitResponse {
                index_name: index_name.clone(),
                id: hit.id.clone(),
                score: hit.score,
                source: serde_json::from_slice(hit.get_source())?,
            })
        })
        .collect::<Result<Vec<SearchHitResponse>, Error>>();
    hits.map(|hits| SearchResponse {
        took,
        hits,
        timed_out: false,
        shards: ShardResultResponse {
            total: result.shard_count,
            successful: result.success_count,
            failed: result.shard_count - result.success_count,
            skipped: Some(0),
        },
    })
}
