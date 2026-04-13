use std::sync::Arc;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();

    let state = Arc::new(shared::state::init().await);

    let _handles = [
        tokio::spawn(api_gateway::start_server(state.clone())),
        tokio::spawn(pool_manager::start(state.clone())),
        tokio::spawn(health_monitor::start(state.clone())),
    ];

    tokio::select! {
        _ = signal::ctrl_c() => {
            tracing::info!("Shutdown signal received");
        }
    }

    tracing::info!("Shutting down tasks...");
}
