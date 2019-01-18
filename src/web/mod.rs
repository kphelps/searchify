use crate::config::Config;
use crate::mappings::Mappings;
use crate::node_router::NodeRouterHandle;
use crate::query_api::SearchQuery;
use failure::Error;
use futures::{prelude::*, sync::oneshot};
use http::response::Builder;
use http::Request;
use log::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tokio::net::TcpListener;
use tower_web::*;
use tower_web::Error as TowerError;

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
        }

        #[post("/:name/_doc/:id")]
        #[content_type("json")]
        fn index_document(
            &self,
            name: String,
            id: u64,
            body: Vec<u8>,
        ) -> impl Future<Item = IndexDocumentResponse, Error = Error> + Send {
            let v = serde_json::from_slice(&body).unwrap();
            let mut hasher = DefaultHasher::new();
            hasher.write_u64(id);
            self.node_router
                .index_document(name, hasher.finish(), v)
                .map(|_| IndexDocumentResponse {})
        }

        #[post("/:name/_refresh")]
        #[content_type("json")]
        fn refresh_index(&self, name: String) -> impl Future<Item = RefreshResponse, Error = Error> + Send {
            self.node_router.refresh_index(&name).map(|_| RefreshResponse {})
        }

        #[delete("/:name")]
        #[content_type("json")]
        fn delete_index(&self, name: String) -> impl Future<Item = DeletedResponse, Error = Error> + Send {
            self.node_router.delete_index(name).map(|_| DeletedResponse{})
        }

        #[get("/:name")]
        #[content_type("json")]
        fn get_index(&self, name: String) -> impl Future<Item = Index, Error = Error> + Send {
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
