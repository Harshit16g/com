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
    /// Supabase JWT secret for HS256 validation (HS256 for dev, RS256 for prod later)
    pub supabase_jwt_secret: String,
    /// Admin JWT secret for validating machine-to-machine admin tokens
    pub admin_jwt_secret: String,
    /// Shared secret for authenticating internal calls between wa_api and evo API.
    /// Replaces WEBHOOK_SHARED_SECRET and EVO_API_KEY.
    pub evo_internal_api_key: String,
    /// [DEPRECATED] Use evo_internal_api_key instead.
    pub webhook_shared_secret: String,
    /// [DEPRECATED] Now uses Supabase JWT.
    pub pauth_api_key: String,
    /// [DEPRECATED] Now uses admin_jwt_secret or Supabase JWT with admin role.
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

        let pauth_api_key = env::var("PAUTH_API_KEY")
            .or_else(|_| env::var("X_API_KEY"))
            .unwrap_or_default();

        let admin_api_key = env::var("ADMIN_API_KEY")
            .or_else(|_| env::var("X_ADMIN_KEY"))
            .unwrap_or_default();

        let evo_internal_api_key = env::var("EVO_INTERNAL_API_KEY")
            .or_else(|_| env::var("WEBHOOK_SHARED_SECRET"))
            .or_else(|_| env::var("EVO_API_KEY"))
            .unwrap_or_else(|_| "change-me-in-prod".to_string());

        let min_send_delay_secs: u64 = env::var("MIN_SEND_DELAY_SECS")
            .unwrap_or_else(|_| "8".to_string())
            .parse()?;
        let max_send_delay_secs: u64 = env::var("MAX_SEND_DELAY_SECS")
            .unwrap_or_else(|_| "15".to_string())
            .parse()?;

        if max_send_delay_secs < min_send_delay_secs {
            return Err(anyhow::anyhow!(
                "MAX_SEND_DELAY_SECS ({}) cannot be less than MIN_SEND_DELAY_SECS ({})",
                max_send_delay_secs,
                min_send_delay_secs
            ));
        }

        Ok(AppConfig {
            redis_url: required("REDIS_URL")?,
            database_url: required("DATABASE_URL")?,
            evo_base_url: required("EVO_BASE_URL")?,
            evo_api_key: env::var("EVO_API_KEY").unwrap_or_else(|_| evo_internal_api_key.clone()),
            alert_webhook_url: env::var("ALERT_WEBHOOK_URL").ok(),
            server_port: env::var("PORT")
                .or_else(|_| env::var("SERVER_PORT"))
                .unwrap_or_else(|_| "8080".to_string())
                .parse()?,
            min_send_delay_secs,
            max_send_delay_secs,
            platform_webhook_url: env::var("PLATFORM_WEBHOOK_URL").ok(),
            platform_api_key: env::var("PLATFORM_API_KEY").ok(),
            supabase_jwt_secret: env::var("SUPABASE_JWT_SECRET").unwrap_or_default(),
            admin_jwt_secret: env::var("ADMIN_JWT_SECRET").unwrap_or_default(),
            evo_internal_api_key,
            webhook_shared_secret: env::var("WEBHOOK_SHARED_SECRET").unwrap_or_default(),
            pauth_api_key,
            admin_api_key,
            cors_allowed_origins,
        })
    }
}

fn required(key: &str) -> Result<String> {
    env::var(key).map_err(|_| anyhow::anyhow!("Missing required env var: {}", key))
}
