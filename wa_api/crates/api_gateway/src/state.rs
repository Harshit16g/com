use anyhow::Result;
use shared::{
    config::AppConfig,
    db::DbClient,
    evolution::EvolutionClient,
    redis_client::RedisClient,
};

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub redis: RedisClient,
    pub db: DbClient,
    pub evolution: EvolutionClient,
}

impl AppState {
    pub async fn new(config: AppConfig) -> Result<Self> {
        let redis = RedisClient::new(&config.redis_url).await?;
        let db = DbClient::new(&config.database_url).await?;
        let evolution = EvolutionClient::new(&config.evolution_base_url, &config.evolution_api_key);

        Ok(AppState { config, redis, db, evolution })
    }
}
