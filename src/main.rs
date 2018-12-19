#![feature(await_macro)]
#![feature(futures_api)]
#![feature(async_await)]
#![feature(pin)]
#![feature(proc_macro_hygiene)]
#![feature(arbitrary_self_types)]

use clap::{Arg, App};
use log::error;
use rand;

mod cluster;
mod network;
mod node;
mod proto;
mod raft;
mod web;

fn main() {
    std::env::set_var("RUST_LOG", "searchify=info,actix_web=info,raft=debug");
    env_logger::init();

    let matches = App::new("searchify")
        .version("0.1.0")
        .author("Kyle Phelps <kphelps@salsify.com>")
        .about("Distributed search engine")
        .arg(
            Arg::with_name("port")
                .short("p")
                .value_name("PORT")
                .help("Port to listen for peers on")
        )
        .arg(
            Arg::with_name("seed")
                .multiple(true)
                .value_name("IP:PORT")
                .help("Peer addresses to seed the network")
        )
        .arg(
            Arg::with_name("id")
                .short("s")
                .value_name("ID")
                .help("Thihs server's seed ID")
        )
        .get_matches();

    let port = u16::from_str_radix(matches.value_of("port").unwrap_or("11666"), 10).expect("Invalid port");
    let seeds: Vec<String> = matches.values_of("seed")
        .expect("must provide at least one seed")
        .map(str::to_string)
        .collect();
    let rand_id = rand::random::<u64>().to_string();
    let seed_id_str = matches.value_of("id").unwrap_or(&rand_id);
    let seed_id = u64::from_str_radix(seed_id_str, 10).expect("Invalid seed id");

    if let Err(error) = node::run(seed_id, port, &seeds) {
        error!("{}", error);
    }
}
