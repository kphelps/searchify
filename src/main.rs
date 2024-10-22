use clap::{App, Arg};
use failure::Error;
use log::*;

#[macro_use]
mod storage_engine;

mod action_executor;
mod actions;
mod async_work_queue;
mod cached_persistent_cell;
mod cached_persistent_map;
mod clock;
mod cluster_state;
mod config;
mod document;
mod event_emitter;
mod gossip;
mod id_generator;
mod index_coordinator;
mod index_tracker;
mod key_value_state_machine;
mod keys;
mod kv_index;
mod mappings;
mod metrics;
mod network;
mod node;
mod node_router;
mod persistent_cell;
mod persistent_map;
#[allow(renamed_and_removed_lints)]
mod proto;
mod query_api;
mod raft;
mod raft_router;
mod raft_storage;
mod rpc_client;
mod search_state_machine;
mod search_storage;
mod shard;
mod shard_tracker;
mod version_tracker;
mod web;

fn main() {
    std::env::set_var(
        "RUST_LOG",
        "actix_web=info,searchify=info,raft=info,tantivy=info",
    );
    env_logger::init();
    if let Err(err) = start() {
        error!("Failure: {}", err);
    }
}

fn start() -> Result<(), Error> {
    let matches = App::new("searchify")
        .version("0.1.0")
        .author("Kyle Phelps <kphelps@salsify.com>")
        .about("Distributed search engine")
        .arg(
            Arg::with_name("config")
                .short("c")
                .value_name("PATH")
                .default_value("./searchify")
                .help("path to configuration file"),
        )
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
    let settings = config::Config::new(config_path)?;
    node::run(&settings)?;
    Ok(())
}
