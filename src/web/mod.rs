use crate::action_executor::ActionExecutor;
use crate::actions::{self, Action};
use crate::config::Config;
use actix_web::{*, web::Payload};
use failure::Error;
use futures::prelude::*;
use log::*;
use std::net::SocketAddr;

#[derive(Clone)]
struct WebApi {
    action_executor: ActionExecutor,
}

fn register_action<A, P>(cfg: &mut web::ServiceConfig, action: A)
where
    A: Action<Path = P> + Clone + 'static,
    P: FromRequest + serde::de::DeserializeOwned + 'static,
{
    let path_string = action.path();
    let method = action.method();
    let func =
        move |request: HttpRequest, payload: Payload| -> Box<Future<Item = HttpResponse, Error = Error> + 'static> {
            let path = web::Path::<P>::extract(&request).unwrap();
            let state = web::Data::<WebApi>::extract(&request).unwrap();
            let f = state
                .action_executor
                .execute_http(action.clone(), path, &request, payload);
            Box::new(f)
        };
    cfg.route(&path_string, web::method(method).to(func));
}

fn register_actions(cfg: &mut web::ServiceConfig) {
    register_action(cfg, actions::ListIndicesAction);
    register_action(cfg, actions::DeleteIndexAction);
    register_action(cfg, actions::DeleteDocumentAction);
    register_action(cfg, actions::GetDocumentAction);
    register_action(cfg, actions::IndexDocumentAction);
    register_action(cfg, actions::RefreshAction);
    register_action(cfg, actions::CreateIndexAction);
    register_action(cfg, actions::GetIndexAction);
}

pub fn start_web(config: &Config, action_executor: ActionExecutor) -> Result<(), Error> {
    let address: SocketAddr = format!("{}:{}", config.web.host, config.web.port).parse()?;
    let state = WebApi { action_executor };

    let build_app = move || App::new().data(state.clone()).configure(register_actions);

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
