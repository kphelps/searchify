use crate::config::Config;
use crate::mappings::Mappings;
use crate::node_router::NodeRouterHandle;
use failure::Error;
use futures::{
    prelude::*,
    sync::oneshot,
};
use tokio::net::TcpListener;
use tower_web::{
    Response, ServiceBuilder,
    extract::{Context, Extract, Immediate},
    util::BufStream,
    *,
};
use serde_derive::*;

pub struct HttpServer {
    _handle: oneshot::Sender<()>,
}

#[derive(Clone, Debug)]
struct HelloWorld;

#[derive(Response)]
struct HelloResponse {
    message: &'static str,
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

impl<B: BufStream> Extract<B> for NodeRouterHandle {
    type Future = Immediate<NodeRouterHandle>;

    fn extract(context: &Context) -> Self::Future {
        let config = context.config::<NodeRouterHandle>().unwrap();
        Immediate::ok(config.clone())
    }
}

impl_web! {
    impl HelloWorld {
        #[get("/")]
        #[content_type("json")]
        fn hello_world(&self) -> Result<HelloResponse, ()> {
            Ok(HelloResponse {
                message: "hello world",
            })
        }

        #[post("/:name")]
        #[content_type("json")]
        fn create_index(
            &self,
            body: CreateIndexRequest,
            name: String,
            node_router: NodeRouterHandle,
        ) -> impl Future<Item=CreateIndexResponse, Error=Error> + Send {
            let action = node_router.create_index(
                name.clone(),
                body.settings.number_of_shards,
                body.settings.number_of_replicas,
                body.mappings,
            ).from_err();
            action.map(move |_| CreateIndexResponse { index_name: name })
        }
    }
}

pub fn start_web(config: &Config, node_router: NodeRouterHandle) -> Result<HttpServer, Error> {
    let address = format!("{}:{}", config.web.host, config.web.port).parse()?;
    let listener = TcpListener::bind(&address)?;

    let f = ServiceBuilder::new()
        .config(node_router)
        .middleware(tower_web::middleware::log::LogMiddleware::new("searchify"))
        .resource(HelloWorld)
        .serve(listener.incoming());

    let (sender, receiver) = oneshot::channel();
    tokio::spawn(f.select(receiver.map_err(|_| ())).then(|_| Ok(())));
    Ok(HttpServer { _handle: sender })
}


// #[derive(Serialize)]
// struct Test {
//     index_name: String,
// }

// #[derive(Deserialize)]
// struct IndexPath {
//     name: String,
// }

// #[derive(Serialize)]
// struct Index {
//     index_name: String,
//     shard_count: u64,
//     replica_count: u64,
// }

// struct RequestContext {
//     node_router: NodeRouterHandle,
// }

// impl RequestContext {
//     pub fn new(node_router: NodeRouterHandle) -> Self {
//         Self { node_router }
//     }

//     pub fn node_router(&self) -> NodeRouterHandle {
//         self.node_router.clone()
//     }
// }

// fn create_index(
//     (ctx, request, path): (
//         State<RequestContext>,
//         Json<CreateIndexRequest>,
//         Path<IndexPath>,
//     ),
// ) -> impl Future<Item = Json<Test>, Error = Error> {
//     ctx.node_router
//         .create_index(
//             path.name.clone(),
//             request.settings.number_of_shards,
//             request.settings.number_of_replicas,
//             request.mappings.clone(),
//         )
//         .map(move |_| {
//             Json(Test {
//                 index_name: path.name.clone(),
//             })
//         })
//         .from_err()
// }

// fn delete_index(
//     request: &HttpRequest<RequestContext>,
// ) -> impl Future<Item = HttpResponse, Error = Error> {
//     let network = request.state().node_router();
//     future::result(request.match_info().query("name"))
//         .from_err::<Error>()
//         .and_then(move |index_name: String| network.delete_index(index_name.clone()))
//         .map(|_| HttpResponse::NoContent().finish())
// }

// fn list_indices(
//     request: &HttpRequest<RequestContext>,
// ) -> impl Future<Item = Json<Vec<Index>>, Error = Error> {
//     request
//         .state()
//         .node_router()
//         .list_indices()
//         .map(|response| {
//             let indices = response.indices.into_iter().map(|index| Index {
//                 index_name: index.name,
//                 shard_count: index.shard_count,
//                 replica_count: index.replica_count,
//             });
//             Json(indices.collect())
//         })
// }

// #[derive(Serialize)]
// struct IndexDocumentResponse {}

// #[derive(Deserialize)]
// struct DocumentPath {
//     name: String,
//     document_id: u64,
// }

// fn index_document(
//     (ctx, payload, path): (State<RequestContext>, Json<Value>, Path<DocumentPath>),
// ) -> impl Future<Item = Json<IndexDocumentResponse>, Error = Error> {
//     let mut hasher = DefaultHasher::new();
//     hasher.write_u64(path.document_id);
//     ctx.node_router
//         .index_document(path.name.clone(), hasher.finish(), payload.0)
//         .map(|_| Json(IndexDocumentResponse {}))
// }

// #[derive(Deserialize)]
// struct SearchRequest {
//     query: SearchQuery,
// }

// #[derive(Serialize)]
// struct SearchResponse {}

// fn search_index(
//     (ctx, payload, path): (State<RequestContext>, Json<SearchRequest>, Path<IndexPath>),
// ) -> impl Future<Item = Json<SearchResponse>, Error = Error> {
//     let query_string = serde_json::to_vec(&payload.0.query).unwrap();
//     ctx.node_router
//         .search(path.name.to_string(), query_string)
//         .map(|_| Json(SearchResponse {}))
// }

// pub fn start_web(config: &Config, node_router: NodeRouterHandle) {
//     let app_ctor = move || {
//         App::with_state(RequestContext::new(node_router.clone()))
//             .middleware(Logger::default())
//             .resource("/{name}", |r| {
//                 r.post().with_async(create_index);
//                 r.delete().a(delete_index);
//             })
//             .resource("/{name}/_search", |r| {
//                 r.post().with_async(search_index);
//             })
//             .resource("/{name}/_doc/{document_id}", |r| {
//                 r.post().with_async(index_document);
//             })
//             .resource("/_cat/indices", |r| r.get().a(list_indices))
//             .finish()
//     };

//     let address = format!("{}:{}", config.web.host, config.web.port);
//     actix_web::server::new(app_ctor)
//         .bind(address)
//         .expect("Failed to bind")
//         .shutdown_timeout(0)
//         .start();
//     info!("Started server");
// }
