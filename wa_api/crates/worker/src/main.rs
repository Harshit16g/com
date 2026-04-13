use anyhow::Result;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod processor;
mod rate_limiter;

use shared::{
    config::AppConfig,
    db::DbClient,
    evolution::EvolutionClient,
    redis_client::RedisClient,
};

#[derive(Clone)]
pub struct WorkerState {
    pub config: AppConfig,
    pub redis: RedisClient,
    pub db: DbClient,
    pub evolution: EvolutionClient,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false)
                .pretty(),
        )
        .init();

    let config = AppConfig::from_env()?;

    // Phase 1: 4 concurrent workers
    let worker_count: usize = std::env::var("WORKER_COUNT")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .unwrap_or(4);

    info!("Starting {} worker tasks", worker_count);

    let state = Arc::new(WorkerState {
        redis: RedisClient::new(&config.redis_url).await?,
        db: DbClient::new(&config.database_url).await?,
        evolution: EvolutionClient::new(&config.evolution_base_url, &config.evolution_api_key),
        config,
    });

    let mut handles = Vec::new();

    for worker_id in 0..worker_count {
        let state_clone = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            processor::run_worker(worker_id, state_clone).await;
        });
        handles.push(handle);
    }

    // Wait for all workers (they run forever unless panicked)
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}
