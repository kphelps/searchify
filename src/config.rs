use config::{ConfigError, Config as ConfigBuilder, File, Environment};
use serde_derive::Deserialize;
use std::path::Path;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub node_id: u64,
    pub port: u16,
    pub master_ids: Vec<u64>,
    pub seeds: Vec<String>,
    pub storage_root: String,
    pub search_storage_root: Option<String>,
    pub web: WebConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WebConfig {
    pub host: String,
    pub port: u16,
}

impl Config {
    pub fn new(config_path: &str) -> Result<Self, ConfigError> {
        let mut s = Self::default_builder()?;
        s.merge(File::with_name(config_path).required(false))?
            .merge(Environment::with_prefix("searchify"))?;
        s.try_into()
    }

    #[allow(dead_code)]
    pub fn default() -> Result<Self, ConfigError> {
        Self::default_builder()?.try_into()
    }

    fn default_builder() -> Result<ConfigBuilder, ConfigError> {
        let mut s = ConfigBuilder::new();
        s.set_default("port", 11666)?
            .set_default("node_id", 1)?
            .set_default("storage_root", "./data")?
            .set_default("master_ids", vec![1, 2, 3])?
            .set_default("seeds", Vec::<String>::new())?
            .set_default("web.host", "0.0.0.0")?
            .set_default("web.port", 8080)?;
        Ok(s)
    }

    pub fn search_storage_root(&self) -> String {
        self.search_storage_root.clone()
            .unwrap_or_else(|| Path::new(&self.storage_root).join("search").to_str().unwrap().to_string())
    }
}
