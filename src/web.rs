use actix::prelude::*;
use actix_web::{
    self,
    App,
    Json,
    http::{Method},
    HttpRequest,
    HttpResponse,
    middleware::Logger,
};
use crate::config::Config;
use crate::node_router::NodeRouterHandle;
use failure::Error;
use futures::{prelude::*, future};
use serde_derive::Serialize;
use log::info;

#[derive(Serialize)]
struct Test {
    index_name: String,
}

#[derive(Serialize)]
struct Index {
    index_name: String,
    shard_count: u64,
    replica_count: u64,
}

struct RequestContext {
    node_router: NodeRouterHandle,
}

impl RequestContext {
    pub fn new(node_router: NodeRouterHandle) -> Self {
        Self {
            node_router,
        }
    }

    pub fn node_router(&self) -> NodeRouterHandle {
        self.node_router.clone()
    }
}

trait JsonFuture<T> = Future<Item=Json<T>, Error=Error>;
trait HttpResponseFuture = Future<Item=HttpResponse, Error=Error>;

fn create_index(request: &HttpRequest<RequestContext>) -> impl JsonFuture<Test> {
    let network = request.state().node_router();
    future::result(request.match_info().query("name"))
        .from_err::<Error>()
        .and_then(move |index_name: String| {
            network.create_index(index_name.clone())
                .map(|_| index_name)
        })
        .map(|index_name| Json(Test{index_name: index_name}))
        .from_err()
}

fn delete_index(request: &HttpRequest<RequestContext>) -> impl HttpResponseFuture {
    let network = request.state().node_router();
    future::result(request.match_info().query("name"))
        .from_err::<Error>()
        .and_then(move |index_name: String| network.delete_index(index_name.clone()))
        .map(|_| HttpResponse::NoContent().finish())
}

fn list_indices(request: &HttpRequest<RequestContext>) -> impl JsonFuture<Vec<Index>> {
    request.state().node_router().list_indices().map(|response| {
        let indices = response.indices.into_iter().map(|index| {
            Index {
                index_name: index.name,
                shard_count: index.shard_count,
                replica_count: index.replica_count,
            }
        });
        Json(indices.collect())
    })
}

#[derive(Serialize)]
struct IndexDocumentResponse {
}

fn index_document(request: &HttpRequest<RequestContext>) -> impl JsonFuture<IndexDocumentResponse> {
    let router = request.state().node_router();
    future::result(request.match_info().query("name")).from_err::<Error>()
        .and_then(move |index_name: String| {
            router.index_document(index_name).map(|_| Json(IndexDocumentResponse{}))
        })
}

pub fn start_web(
    config: &Config,
    node_router: NodeRouterHandle,
) {
    let app_ctor = move || App::with_state(RequestContext::new(node_router.clone()))
        .middleware(Logger::default())
        .resource("/{name}", |r| {
            r.post().a(create_index);
            r.delete().a(delete_index);
        })
        .resource("/{name}/_doc/{doc_id}", |r| {
            r.post().a(index_document);
        })
        .resource("/_cat/indices", |r| r.get().a(list_indices))
        .finish();

    let address = format!("{}:{}", config.web.host, config.web.port);
    actix_web::server::new(app_ctor)
        .bind(address).expect("Failed to bind")
        .shutdown_timeout(0)
        .start();
    info!("Started server");
}
