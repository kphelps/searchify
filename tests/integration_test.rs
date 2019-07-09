use futures::prelude::*;
use futures::future;
use futures::sync::oneshot;
use reqwest::r#async::{Client, Response};
use searchify::{Config, node};
use searchify::actions::ClusterHealthResponse;
use tempfile::tempdir;
use tokio;

#[test]
fn test_start_single_node() {
    std::env::set_var(
        "RUST_LOG",
        "actix_web=info,searchify=info,raft=info,tantivy=info",
    );
    env_logger::init();
    let root = tempdir().unwrap();
    let mut config_builder = Config::default_builder().unwrap();
    config_builder.set("node_id", 1).unwrap()
        .set("master_ids", vec![1]).unwrap()
        .set("storage_root", root.path().join("storage").to_str().unwrap().to_string()).unwrap()
        .set("web.port", 8080).unwrap()
        .set("advertised_host", "127.0.0.1").unwrap();
    let config = config_builder.try_into().unwrap();

    let (sender, receiver) = oneshot::channel();

    let client = SearchifyClient::new("http://localhost:8080");

    let f = future::lazy(move || {
        let f = node::run_system(&config, receiver);
        tokio::spawn(f.map_err(|_| ()));
        client.health()
    }).then(|result| {
        result.unwrap();
        drop(sender);
        let out: Result<(), ()> = Ok(());
        out
    });

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(f).unwrap();
}

struct SearchifyClient {
    root: String,
    client: Client,
}

impl SearchifyClient {
    pub fn new(root: &str) -> Self {
        Self {
            root: root.to_string(),
            client: Client::new(),
        }
    }

    pub fn health(&self) -> impl Future<Item = ClusterHealthResponse, Error = reqwest::Error> {
        self.get("_cluster/health")
            .and_then(|mut response| response.json())
    }

    fn get(&self, path: &str) -> impl Future<Item = Response, Error = reqwest::Error> {
        self.client
            .get(&format!("{}/{}", self.root, path))
            .send()
            .and_then(|response| response.error_for_status())
    }
}
