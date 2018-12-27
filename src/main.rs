#![feature(await_macro)]
#![feature(futures_api)]
#![feature(async_await)]
#![feature(pin)]
#![feature(proc_macro_hygiene)]
#![feature(arbitrary_self_types)]

use clap::{Arg, App};
use failure::Error;
use log::error;

mod config;
mod key_value_state_machine;
mod network;
mod node;
mod proto;
mod raft;
mod raft_storage;
mod search_state_machine;
mod storage_engine;
mod web;

fn main() {
    std::env::set_var("RUST_LOG", "searchify=info,actix_web=info,raft=debug");
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
