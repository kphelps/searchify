use actix::prelude::*;
use actix_web::{
    self,
    App,
    Json,
    http::{Method},
    HttpRequest,
    middleware::Logger,
};
use crate::config::Config;
use crate::network::{
    CreateIndex,
    NetworkActor,
};
use failure::Error;
use futures::{prelude::*, future};
use serde_derive::Serialize;
use log::info;

#[derive(Serialize)]
struct Test {
    index_name: String,
}

struct RequestContext {
    network: Addr<NetworkActor>,
}

impl RequestContext {
    pub fn new(network: Addr<NetworkActor>) -> Self {
        Self {
            network: network,
        }
    }
}

fn create_index(request: &HttpRequest<RequestContext>) -> impl Future<Item=Json<Test>, Error=Error> {
    let network = request.state().network.clone();
    future::result(request.match_info().query("name"))
        .from_err::<Error>()
        .and_then(move |index_name: String| {
            let message = CreateIndex{
                index_name: index_name.clone(),
            };
            network.send(message).flatten().from_err()
                .map(|_| index_name)
        })
        .map(|index_name| Json(Test{index_name: index_name}))
        .from_err()
}

pub fn start_web(
    config: &Config,
    network: Addr<NetworkActor>,
) {
    let app_ctor = move || App::with_state(RequestContext::new(network.clone()))
        .middleware(Logger::default())
        .resource("/{name}", |r| r.method(Method::POST).a(create_index))
        .finish();

    let address = format!("{}:{}", config.web.host, config.web.port);
    actix_web::server::new(app_ctor)
        .bind(address).expect("Failed to bind")
        .shutdown_timeout(0)
        .start();
    info!("Started server");
}
