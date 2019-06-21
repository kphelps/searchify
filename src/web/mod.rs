use actix_web::*;
use crate::action_executor::ActionExecutor;
use crate::action::DeleteIndexAction;
use crate::config::Config;
use crate::mappings::Mappings;
use crate::node_router::NodeRouterHandle;
use crate::query_api::SearchQuery;
use failure::Error;
use futures::{prelude::*, sync::oneshot};
use std::time::Instant;
use tokio::net::TcpListener;
use tower_web::*;

pub struct HttpServer {
    _handle: oneshot::Sender<()>,
}

#[derive(Clone)]
struct HelloWorld {
    action_executor: ActionExecutor,
    node_router: NodeRouterHandle,
}

#[derive(Response)]
struct CreateIndexResponse {
    index_name: String,
}

#[derive(Deserialize)]
struct IndexSettings {
    number_of_shards: u64,
    number_of_replicas: u64,
}

#[derive(Extract)]
struct CreateIndexRequest {
    settings: IndexSettings,
    mappings: Mappings,
}

#[derive(Extract)]
struct SearchRequest {
    query: SearchQuery,
}

#[derive(Response)]
struct SearchResponse {
    took: u64,
    timed_out: bool,
    shards: ShardResultResponse,
    hits: Vec<SearchHitResponse>,
}

#[derive(Response, Serialize)]
struct ShardResultResponse {
    total: u64,
    successful: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped: Option<u64>,
    failed: u64,
}

#[derive(Response, Serialize)]
struct SearchHitResponse {
    #[serde(rename = "_index")]
    index_name: String,
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_score")]
    score: f32,
    #[serde(rename = "_source")]
    source: Option<serde_json::Value>,
}

#[derive(Response)]
struct IndexDocumentResponse {}


#[derive(Response, Serialize)]
#[web(status = "200")]
struct GetDocumentResponse {
    #[serde(rename = "_index")]
    index_name: String,
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_version")]
    version: u64,
    #[serde(rename = "found")]
    found: bool,
    #[serde(rename = "_source")]
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<serde_json::Value>,
}

#[derive(Response, Serialize)]
struct DeleteDocumentResponse {
    #[serde(rename = "_shards")]
    shards: ShardResultResponse,
    #[serde(rename = "_index")]
    index_name: String,
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_version")]
    version: String,
    result: String,
}

#[derive(Response)]
struct RefreshResponse {}

#[derive(Response)]
#[web(status = "204")]
struct DeletedResponse {}

#[derive(Response, Serialize)]
struct Index {
    index_name: String,
    shard_count: u64,
    replica_count: u64,
}

#[derive(Response)]
struct ListIndicesResponse {
    indices: Vec<Index>,
}

#[derive(Response)]
struct InternalError {
    message: String,
}

// impl_web! {
//     impl HelloWorld {
//         #[post("/:name")]
//         #[content_type("json")]
//         fn create_index(
//             &self,
//             body: CreateIndexRequest,
//             name: String,
//         ) -> impl Future<Item=CreateIndexResponse, Error=Error> + Send {
//             let action = self.node_router.create_index(
//                 name.clone(),
//                 body.settings.number_of_shards,
//                 body.settings.number_of_replicas,
//                 body.mappings,
//             ).from_err();
//             action.map(move |_| CreateIndexResponse { index_name: name })
//         }

//         #[post("/:name/_search")]
//         #[content_type("json")]
//         fn search_index(
//             &self,
//             body: SearchRequest,
//             name: String,
//         ) -> impl Future<Item = SearchResponse, Error = Error> + Send {
//             let start = Instant::now();
//             let query_string = serde_json::to_vec(&body.query).unwrap();
//             self.node_router.search(name.clone(), query_string)
//                 .and_then(move |result| {
//                     let dt = start.elapsed();
//                     let took = dt.as_secs() as u64 * 1000 + dt.subsec_millis() as u64;
//                     let hits = result.get_hits().into_iter().map(|hit| Ok(SearchHitResponse {
//                         index_name: name.clone(),
//                         id: hit.id.clone(),
//                         score: hit.score,
//                         source: serde_json::from_slice(hit.get_source())?
//                     })).collect::<Result<Vec<SearchHitResponse>, Error>>();
//                     hits.map(|hits| SearchResponse {
//                         took,
//                         hits,
//                         timed_out: false,
//                         shards: ShardResultResponse {
//                             total: result.shard_count,
//                             successful: result.success_count,
//                             failed: result.shard_count - result.success_count,
//                             skipped: Some(0),
//                         },
//                     })
//                 })
//                 .map_err(|e| {
//                     log::error!("Err: {:?}", e);
//                     e
//                 })
//         }

//         #[post("/:name/:id")]
//         #[content_type("json")]
//         fn index_document(
//             &self,
//             name: String,
//             id: String,
//             body: Vec<u8>,
//         ) -> impl Future<Item = IndexDocumentResponse, Error = Error> + Send {
//             let v = serde_json::from_slice(&body).unwrap();
//             self.node_router
//                 .index_document(name, id.into(), v)
//                 .map(|_| IndexDocumentResponse {})
//         }

//         #[post("/:name/_refresh")]
//         #[content_type("json")]
//         fn refresh_index(&self, name: String) -> impl Future<Item = RefreshResponse, Error = Error> + Send {
//             self.node_router.refresh_index(&name).map(|_| RefreshResponse {})
//         }

//         #[delete("/:name/")]
//         #[content_type("json")]
//         fn delete_index(&self, name: String) -> impl Future<Item = DeletedResponse, Error = Error> + Send {
//             self.node_router.delete_index(name).map(|_| DeletedResponse{})
//         }

//         #[get("/:name/")]
//         #[content_type("json")]
//         fn get_index(&self, name: String) -> impl Future<Item = Index, Error = Error> + Send {
//             self.node_router.get_index(name)
//                 .map(|state| Index{
//                     index_name: state.name,
//                     shard_count: state.shard_count,
//                     replica_count: state.replica_count,
//                 })
//         }

//         #[get("/_cat/indices")]
//         #[content_type("json")]
//         fn list_indices(&self) -> impl Future<Item = ListIndicesResponse, Error = Error> + Send {
//             self.node_router
//                 .list_indices()
//                 .map(|response| {
//                     let indices = response.indices.into_iter().map(|index| Index {
//                         index_name: index.name,
//                         shard_count: index.shard_count,
//                         replica_count: index.replica_count,
//                     });
//                     ListIndicesResponse{
//                         indices: indices.collect(),
//                     }
//                 })
//         }

//         #[get("/:name/:id")]
//         #[content_type("json")]
//         fn get_document(
//             &self,
//             name: String,
//             id: String,
//         ) -> impl Future<Item = GetDocumentResponse, Error = Error> + Send {
//             self.node_router.get_document(name.clone(), id.clone().into())
//                 .and_then(move |response| {
//                     let source = if response.found {
//                         let j = response.get_source();
//                         let value = serde_json::from_slice(j)?;
//                         Some(value)
//                     } else {
//                         None
//                     };
//                     Ok(GetDocumentResponse{
//                         index_name: name,
//                         id: id,
//                         found: response.found,
//                         version: 0,
//                         source: source,
//                     })
//                 })
//         }

//         #[delete("/:name/:id")]
//         #[content_type("json")]
//         fn delete_document(
//             &self,
//             name: String,
//             id: String,
//         ) -> impl Future<Item = DeleteDocumentResponse, Error = Error> + Send {
//             self.node_router.delete_document(name.clone(), id.clone().into())
//                 .and_then(move |response| {
//                     Ok(DeleteDocumentResponse{
//                         shards: ShardResultResponse {
//                             total: 1,
//                             successful: if response.success { 1 } else { 0 },
//                             failed: if response.success { 0 } else { 1 },
//                             skipped: None,
//                         },
//                         index_name: name,
//                         id: id,
//                         version: "0".to_string(),
//                         result: "deleted".to_string(),
//                     })
//                 })
//         }
//     }
// }

fn action_route<A, R>(action: A) {
    
}

pub fn start_web(
    config: &Config,
    action_executor: ActionExecutor,
    node_router: NodeRouterHandle,
) -> Result<HttpServer, Error> {
    let address = format!("{}:{}", config.web.host, config.web.port).parse()?;
    let listener = TcpListener::bind(&address)?;
    let state = HelloWorld { action_executor, node_router };
    let (sender, receiver) = oneshot::channel();
    let build_app = || App::with_state(state.clone())
        .resource("/{name}", |r| r.method(http::Method::DELETE).with(action_route(DeleteIndexAction)))
        .finish();

    server::new(build_app).bind(address).run()

    // let f = ServiceBuilder::new()
    //     .middleware(tower_web::middleware::log::LogMiddleware::new("searchify"))
    //     .resource(HelloWorld { action_executor, node_router })
    //     .serve(listener.incoming());

    // tokio::spawn(f.select(receiver.map_err(|_| ())).then(|_| Ok(())));
    Ok(HttpServer { _handle: sender })
}
