use config::{ConfigError, Config as ConfigBuilder, File, Environment};
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub node_id: u64,
    pub port: u16,
    pub seeds: Vec<String>,
    pub storage_root: String,
}

impl Config {
    pub fn new(config_path: &str) -> Result<Self, ConfigError> {
        let mut s = ConfigBuilder::new();
        s.set_default("port", 11666)?
            .set_default("node_id", 1)?
            .set_default("storage_root", "./data")?
            .set_default("seeds", Vec::<String>::new())?
            .merge(File::with_name(config_path).required(false))?
            .merge(Environment::with_prefix("searchify"))?;
        s.try_into()
    }
}
