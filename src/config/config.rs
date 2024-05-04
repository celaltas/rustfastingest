use dotenv::dotenv;
use eyre::Result;
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub parallel_files: usize,
    pub region: String,
    pub rust_log: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub connection_url: String,
    pub datacenter: String,
    pub concurrency_limit: usize,
    pub schema_file: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ElasticSearchConfig {
    pub url: String,
    pub enabled: bool,
    pub batch_size: usize,
    pub num_shards: usize,
    pub index: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub concurrency_limit: usize,
    pub refresh_interval: String,
    pub source_enabled: bool,
}





#[derive(Debug, Clone, Deserialize)]
pub struct GeneralConfig {
    pub app: AppConfig,
    pub db: DatabaseConfig,
    pub es: ElasticSearchConfig,
}

impl GeneralConfig {
    pub fn from_env() -> Result<GeneralConfig> {
        dotenv().ok();
        info!("Loading configuration");
        let mut c = config::Config::new();
        c.merge(config::Environment::default())?;
        let app_config: AppConfig = c.clone().try_into()?;
        let db_config: DatabaseConfig = c.clone().try_into()?;
        let es_config: ElasticSearchConfig = c.clone().try_into()?;
        let config = GeneralConfig {
            app: app_config,
            db: db_config,
            es: es_config,
        };
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::GeneralConfig;

    #[test]
    fn test_from_env() {
        let result = GeneralConfig::from_env();
        assert_eq!(result.is_ok(), true);
        let config = result.unwrap();
        println!("{:?}", config);
        assert_eq!(config.es.refresh_interval, "20s".to_string());
        assert_eq!(config.app.host, "127.0.0.1");
        assert_eq!(config.db.concurrency_limit, 10);
    }
}
