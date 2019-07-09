mod metrics_middleware;

use self::metrics_middleware::MetricsMiddleware;
use crate::action_executor::ActionExecutor;
use crate::actions::{self, Action};
use crate::config::Config;
use crate::node_router::SearchError;
use actix_web::middleware::Logger;
use actix_web::*;
use failure::Error;
use futures::prelude::*;
use log::*;
use std::net::SocketAddr;
use serde::*;

#[derive(Clone)]
struct WebApi {
    action_executor: ActionExecutor,
}

fn register_action<A, B, P>(cfg: &mut web::ServiceConfig, action: A)
where
    A: Action<Path = P, Payload = B> + Clone + 'static,
    P: FromRequest + serde::de::DeserializeOwned + 'static,
    B: FromRequest + 'static,
{
    let path_string = action.path();
    let method = action.method();
    let func = move |request: HttpRequest,
                     payload: B|
          -> Box<Future<Item = HttpResponse, Error = Error> + 'static> {
        let path = web::Path::<P>::extract(&request).unwrap();
        let state = web::Data::<WebApi>::extract(&request).unwrap();
        let f = state
            .action_executor
            .execute_http(action.clone(), path, &request, payload);
        Box::new(f)
    };
    cfg.route(&path_string, web::method(method).to(func));
}

fn metrics_endpoint() -> impl Responder {
    crate::metrics::dump()
}

fn register_actions(cfg: &mut web::ServiceConfig) {
    cfg.route("metrics", web::get().to(metrics_endpoint));

    register_action(cfg, actions::ClusterHealthAction);
    register_action(cfg, actions::ListIndicesAction);
    register_action(cfg, actions::DeleteIndexAction);
    register_action(cfg, actions::DeleteDocumentAction);
    register_action(cfg, actions::GetDocumentAction);
    register_action(cfg, actions::SearchAction);
    register_action(cfg, actions::RefreshAction);
    register_action(cfg, actions::BulkAction);
    register_action(cfg, actions::IndexDocumentAction);
    register_action(cfg, actions::CreateIndexAction);
    register_action(cfg, actions::GetIndexAction);
    register_action(cfg, actions::IndexExistsAction);
}

pub fn start_web(config: &Config, action_executor: ActionExecutor) -> Result<(), Error> {
    let address: SocketAddr = format!("{}:{}", config.web.host, config.web.port).parse()?;
    let state = WebApi { action_executor };

    let build_app = move || {
        App::new()
            .wrap(MetricsMiddleware::default())
            .wrap(Logger::default())
            .data(state.clone())
            .configure(register_actions)
    };

    std::thread::spawn(move || {
        info!("Starting API on {}", address);
        HttpServer::new(build_app)
            .bind(address)
            .unwrap()
            .run()
            .unwrap();
        info!("Stopped API");
    });
    Ok(())
}

#[derive(Serialize)]
struct ErrorBody {
    
}

impl ResponseError for SearchError {
    fn error_response(&self) -> HttpResponse {
        let response = match self {
            SearchError::IndexNotFound => HttpResponse::NotFound(),
            SearchError::ShardNotFound => HttpResponse::NotFound(),
            SearchError::ClusterStateUnavailable => HttpResponse::ServiceUnavailable(),
            SearchError::LeaderUnavailable => HttpResponse::ServiceUnavailable(),
            SearchError::Error(e) => HttpResponse::InternalServerError(),
        }
        HttpResponse::
    }
}
