use crate::config::Config;
use crate::mappings::Mappings;
use crate::node_router::NodeRouterHandle;
use crate::query_api::SearchQuery;
use failure::Error;
use futures::{prelude::*, sync::oneshot};
use tokio::net::TcpListener;
use tower_web::*;

pub struct HttpServer {
    _handle: oneshot::Sender<()>,
}

#[derive(Clone)]
struct HelloWorld {
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
struct SearchResponse {}

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
    source: Option<serde_json::Value>
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

impl_web! {
    impl HelloWorld {
        #[post("/:name")]
        #[content_type("json")]
        fn create_index(
            &self,
            body: CreateIndexRequest,
            name: String,
        ) -> impl Future<Item=CreateIndexResponse, Error=Error> + Send {
            let action = self.node_router.create_index(
                name.clone(),
                body.settings.number_of_shards,
                body.settings.number_of_replicas,
                body.mappings,
            ).from_err();
            action.map(move |_| CreateIndexResponse { index_name: name })
        }

        #[post("/:name/_search")]
        #[content_type("json")]
        fn search_index(
            &self,
            body: SearchRequest,
            name: String,
        ) -> impl Future<Item = SearchResponse, Error = Error> + Send {
            let query_string = serde_json::to_vec(&body.query).unwrap();
            self.node_router.search(name, query_string).map(|_| SearchResponse {})
                .map_err(|e| {
                    log::error!("Err: {:?}", e);
                    e
                })
        }

        #[post("/:name/:id")]
        #[content_type("json")]
        fn index_document(
            &self,
            name: String,
            id: String,
            body: Vec<u8>,
        ) -> impl Future<Item = IndexDocumentResponse, Error = Error> + Send {
            let v = serde_json::from_slice(&body).unwrap();
            self.node_router
                .index_document(name, id.into(), v)
                .map(|_| IndexDocumentResponse {})
        }

        #[post("/:name/_refresh")]
        #[content_type("json")]
        fn refresh_index(&self, name: String) -> impl Future<Item = RefreshResponse, Error = Error> + Send {
            self.node_router.refresh_index(&name).map(|_| RefreshResponse {})
        }

        #[delete("/:name/")]
        #[content_type("json")]
        fn delete_index(&self, name: String) -> impl Future<Item = DeletedResponse, Error = Error> + Send {
            self.node_router.delete_index(name).map(|_| DeletedResponse{})
        }

        #[get("/:name/")]
        #[content_type("json")]
        fn get_index(&self, name: String) -> impl Future<Item = Index, Error = Error> + Send {
            log::info!("Getting: {}", name);
            self.node_router.get_index(name)
                .map(|state| Index{
                    index_name: state.name,
                    shard_count: state.shard_count,
                    replica_count: state.replica_count,
                })
        }

        #[get("/_cat/indices")]
        #[content_type("json")]
        fn list_indices(&self) -> impl Future<Item = ListIndicesResponse, Error = Error> + Send {
            self.node_router
                .list_indices()
                .map(|response| {
                    let indices = response.indices.into_iter().map(|index| Index {
                        index_name: index.name,
                        shard_count: index.shard_count,
                        replica_count: index.replica_count,
                    });
                    ListIndicesResponse{
                        indices: indices.collect(),
                    }
                })
        }

        #[get("/:name/:id")]
        #[content_type("json")]
        fn get_document(
            &self,
            name: String,
            id: String,
        ) -> impl Future<Item = GetDocumentResponse, Error = Error> + Send {
            self.node_router.get_document(name.clone(), id.clone().into())
                .and_then(move |response| {
                    let source = if response.found {
                        let j = response.get_source();
                        let value = serde_json::from_slice(j)?;
                        Some(value)
                    } else {
                        None
                    };
                    Ok(GetDocumentResponse{
                        index_name: name,
                        id: id,
                        found: response.found,
                        version: 0,
                        source: source,
                    })
                })
        }
    }
}

pub fn start_web(config: &Config, node_router: NodeRouterHandle) -> Result<HttpServer, Error> {
    let address = format!("{}:{}", config.web.host, config.web.port).parse()?;
    let listener = TcpListener::bind(&address)?;

    let f = ServiceBuilder::new()
        .middleware(tower_web::middleware::log::LogMiddleware::new("searchify"))
        .resource(HelloWorld { node_router })
        .serve(listener.incoming());

    let (sender, receiver) = oneshot::channel();
    tokio::spawn(f.select(receiver.map_err(|_| ())).then(|_| Ok(())));
    Ok(HttpServer { _handle: sender })
}
