use anyhow::Result;
use dotenvy::dotenv;
use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub redis_url: String,
    pub database_url: String,
    pub evo_base_url: String,
    pub evo_api_key: String,
    pub alert_webhook_url: Option<String>,
    pub server_port: u16,
    /// 8-15 second randomized delay between sends per instance
    pub min_send_delay_secs: u64,
    pub max_send_delay_secs: u64,
    pub platform_webhook_url: Option<String>,
    pub platform_api_key: Option<String>,
    /// Shared secret for authenticating webhook calls from evo API instances.
    /// All evo instances must send this in the `x-webhook-secret` header.
    pub webhook_shared_secret: String,
    /// Platform auth key — used to authenticate requests from the Leaex platform.
    pub pauth_api_key: String,
    /// Admin API key — used for admin-only operations.
    pub admin_api_key: String,
    /// Allowed CORS origins (comma-separated). Use "*" for dev only.
    pub cors_allowed_origins: Vec<String>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let _ = dotenv(); // ignore if .env missing (Railway uses real env vars)

        let cors_origins_str = env::var("CORS_ALLOWED_ORIGINS").unwrap_or_else(|_| "*".to_string());
        let cors_allowed_origins: Vec<String> = cors_origins_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        Ok(AppConfig {
            redis_url: required("REDIS_URL")?,
            database_url: required("DATABASE_URL")?,
            evo_base_url: required("EVO_BASE_URL")?,
            evo_api_key: required("EVO_API_KEY")?,
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
            platform_webhook_url: env::var("PLATFORM_WEBHOOK_URL").ok(),
            platform_api_key: env::var("PLATFORM_API_KEY").ok(),
            webhook_shared_secret: required("WEBHOOK_SHARED_SECRET")?,
            pauth_api_key: required("PAUTH_API_KEY")?,
            admin_api_key: required("ADMIN_API_KEY")?,
            cors_allowed_origins,
        })
    }
}

fn required(key: &str) -> Result<String> {
    env::var(key).map_err(|_| anyhow::anyhow!("Missing required env var: {}", key))
}
