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
pub struct GeneralConfig {
    pub app: AppConfig,
    pub db: DatabaseConfig,
}

impl GeneralConfig {
    pub fn from_env() -> Result<GeneralConfig> {
        dotenv().ok();
        info!("Loading configuration");
        let mut c = config::Config::new();
        c.merge(config::Environment::default())?;
        let app_config: AppConfig = c.clone().try_into()?;
        let db_config: DatabaseConfig = c.try_into()?;
        let config = GeneralConfig {
            app: app_config,
            db: db_config,
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
        assert_eq!(config.app.host, "localhost");
        assert_eq!(config.db.concurrency_limit, 10);
    }
}
