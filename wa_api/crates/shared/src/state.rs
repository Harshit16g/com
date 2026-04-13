use anyhow::Result;
use tracing::info;
use std::sync::Arc;

use crate::{
    config::AppConfig, db::DbClient, evolution::EvolutionClient, redis_client::RedisClient,
};

#[derive(Clone)]
pub struct AppState {
    pub db: DbClient, // Internally wraps PgPool
    pub redis: RedisClient, // Internally wraps fred::RedisPool
    pub config: AppConfig,
    pub evolution: EvolutionClient,
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
        
    let evolution = EvolutionClient::new(&config.evolution_base_url, &config.evolution_api_key);
    
    info!("Shared state initialized successfully.");

    AppState {
        config,
        redis,
        db,
        evolution,
    }
}
