use anyhow::Result;
use dotenvy::dotenv;
use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub redis_url: String,
    pub database_url: String,
    pub evolution_base_url: String,
    pub evolution_api_key: String,
    pub alert_webhook_url: Option<String>,
    pub server_port: u16,
    /// 8-15 second randomized delay between sends per instance
    pub min_send_delay_secs: u64,
    pub max_send_delay_secs: u64,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let _ = dotenv(); // ignore if .env missing (Railway uses real env vars)

        Ok(AppConfig {
            redis_url: required("REDIS_URL")?,
            database_url: required("DATABASE_URL")?,
            evolution_base_url: required("EVOLUTION_BASE_URL")?,
            evolution_api_key: required("EVOLUTION_API_KEY")?,
            alert_webhook_url: env::var("ALERT_WEBHOOK_URL").ok(),
            server_port: env::var("PORT")
                .or_else(|_| env::var("SERVER_PORT"))
                .unwrap_or_else(|_| "8080".to_string())
                .parse()?,
            min_send_delay_secs: env::var("MIN_SEND_DELAY_SECS")
                .unwrap_or_else(|_| "8".to_string())
                .parse()?,
            max_send_delay_secs: env::var("MAX_SEND_DELAY_SECS")
                .unwrap_or_else(|_| "15".to_string())
                .parse()?,
        })
    }
}

fn required(key: &str) -> Result<String> {
    env::var(key).map_err(|_| anyhow::anyhow!("Missing required env var: {}", key))
}
