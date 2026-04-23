use tracing::info;

use crate::{config::AppConfig, db::DbClient, evo::EvoClient, redis_client::RedisClient};

#[derive(Clone)]
pub struct AppState {
    pub db: DbClient,       // Internally wraps PgPool
    pub redis: RedisClient, // Internally wraps fred::RedisPool
    pub config: AppConfig,
    pub evo: EvoClient,
    pub platform_db: Option<sqlx::PgPool>,
}

pub async fn init() -> AppState {
    info!("Initializing shared state...");
    let config = AppConfig::from_env().expect("Failed to load environment config");

    let redis = RedisClient::new(&config.redis_url)
        .await
        .expect("Failed to connect to Redis pool");

    let db = DbClient::new(&config.database_url)
        .await
        .expect("Failed to connect to PG pool");

    let platform_db = if let Some(url) = &config.platform_database_url {
        info!("Connecting to platform database...");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .expect("Failed to connect to Platform PG pool");
        Some(pool)
    } else {
        None
    };

    let evo = EvoClient::new(&config.evo_base_url, &config.evo_api_key);

    info!("Shared state initialized successfully.");

    AppState {
        config,
        redis,
        db,
        evo,
        platform_db,
    }
}
