use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();

    let state = Arc::new(shared::state::init().await);

    // Spawn scheduler task
    tokio::spawn(scheduler::start(state.clone()));

    // Start worker pool based on configuration
    let worker_count = state.config.worker_count;
    tracing::info!("Starting worker pool with {} workers", worker_count);

    for i in 0..worker_count {
        let state = state.clone();
        tokio::spawn(async move {
            tracing::info!("Worker {} starting", i);
            loop {
                if let Err(e) = worker::start(state.clone()).await {
                    tracing::error!("Worker {} crashed: {:?}", i, e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });
    }

    signal::ctrl_c().await.unwrap();
    tracing::info!("Worker shutting down");
}
