#![feature(async_await)]
#![feature(await_macro)]
#![feature(futures_api)]
#![feature(pin)]
#![feature(proc_macro_hygiene)]
#![feature(arbitrary_self_types)]

#![feature(trait_alias)]

use clap::{Arg, App};
use failure::Error;
use log::error;

#[macro_use] mod storage_engine;

mod cached_persistent_cell;
mod cached_persistent_map;
mod config;
mod key_value_state_machine;
mod keys;
mod id_generator;
mod kv_index;
mod index_tracker;
mod mappings;
mod network;
mod node;
mod node_router;
mod persistent_cell;
mod persistent_map;
mod proto;
mod raft;
mod raft_storage;
mod rpc_client;
mod shard;
mod search;
mod search_state_machine;
mod search_storage;
mod shard_tracker;
mod web;

fn main() {
    std::env::set_var("RUST_LOG", "searchify=info,actix_web=info,raft=info,tantivy=warn");
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
                .help("path to configuration file")
        )
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
    let settings = config::Config::new(config_path)?;
    node::run(&settings)
}
