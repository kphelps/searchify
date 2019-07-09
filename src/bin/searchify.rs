use searchify::{Config, node};
use clap::{App, Arg};
use failure::Error;
use log::*;

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
    let settings = Config::new(config_path)?;
    node::run(&settings)?;
    Ok(())
}
