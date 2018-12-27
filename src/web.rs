use actix_web::{
    self,
    App,
    Json,
    http::{Method},
    middleware::Logger,
    Path,
    Result,
    Responder,
};
use actix_web_async_await::{compat};
use crate::config::Config;
use serde_derive::Serialize;
use log::info;

#[derive(Serialize)]
struct Test {
    index_name: String,
}

struct RequestContext {
}

impl RequestContext {
    pub fn new() -> Self {
        Self {}
    }
}

async fn create_index(info: Path<String>) -> Result<impl Responder> {
    let index_name = info.to_string();
    Ok(Json(Test{index_name: index_name}))
}

pub fn start_web(config: &Config) {
    let app_ctor = || App::with_state(RequestContext::new())
        .middleware(Logger::default())
        .route("/{name}", Method::POST, compat(create_index))
        .finish();

    let address = format!("{}:{}", config.web.host, config.web.port);
    actix_web::server::new(app_ctor)
        .bind(address).expect("Failed to bind")
        .shutdown_timeout(0)
        .start();
    info!("Started server");
}
