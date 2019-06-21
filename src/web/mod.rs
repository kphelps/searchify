use actix_web::*;
use crate::actions::{self, Action};
use crate::action_executor::ActionExecutor;
use crate::config::Config;
use crate::mappings::Mappings;
use crate::node_router::NodeRouterHandle;
use crate::query_api::SearchQuery;
use failure::Error;
use futures::prelude::*;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower_web::*;

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

#[derive(Deserialize)]
struct CreateIndexRequest {
    settings: IndexSettings,
    mappings: Mappings,
}

#[derive(Deserialize)]
struct SearchRequest {
    query: SearchQuery,
}

#[derive(Response)]
struct SearchResponse {
    took: u64,
    timed_out: bool,
    // shards: ShardResultResponse,
    hits: Vec<SearchHitResponse>,
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
#[web(status = "204")]
struct DeletedResponse {}

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
// }

fn register_action<A, P>(cfg: &mut web::ServiceConfig, action: A)
where A: Action<Path=P> + Clone + 'static,
      P: FromRequest + serde::de::DeserializeOwned + 'static
{
    let path_string = action.path();
    let method = action.method();
    let func = move |request: HttpRequest| -> Box<Future<Item=HttpResponse, Error=Error> + 'static> {
        let path = web::Path::<P>::extract(&request).unwrap();
        let state = web::Data::<HelloWorld>::extract(&request).unwrap();
        let f = state.action_executor.execute_http(action.clone(), path, &request);
        Box::new(f)
    };
    cfg.route(&path_string, web::method(method).to(func));
}

fn register_actions(cfg: &mut web::ServiceConfig) {
    register_action(cfg, actions::DeleteIndexAction);
    register_action(cfg, actions::DeleteDocumentAction);
    register_action(cfg, actions::GetDocumentAction);
    register_action(cfg, actions::IndexDocumentAction);
    register_action(cfg, actions::RefreshAction);
}

pub fn start_web(
    config: &Config,
    action_executor: ActionExecutor,
    node_router: NodeRouterHandle,
) -> Result<(), Error> {
    let address: SocketAddr = format!("{}:{}", config.web.host, config.web.port).parse()?;
    let state = HelloWorld { action_executor, node_router };

    let build_app = move || App::new()
        .data(state.clone())
        .configure(register_actions);

    std::thread::spawn(move || actix_web::HttpServer::new(build_app).bind(address).unwrap().run());
    Ok(())
}
