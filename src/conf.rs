use crate::federator::Id;
use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub(crate) struct FederatorConfig {
    pub(crate) host: BrokerConfig,
    pub(crate) neighbours: Vec<BrokerConfig>,
    pub(crate) redundancy: usize,
    pub(crate) core_ann_interval: u64,
    pub(crate) beacon_interval: u64,
    pub(crate) cache_size: usize,
}

#[derive(Deserialize, Debug)]
pub(crate) struct BrokerConfig {
    pub(crate) id: Id,
    pub(crate) uri: String,
}

pub(crate) fn load(config_file: &str) -> Result<FederatorConfig, ConfigError> {
    let mut config = Config::default();
    config.merge(File::with_name(config_file).format(FileFormat::Toml))?;
    config.try_into()
}
