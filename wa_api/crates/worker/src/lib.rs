use anyhow::Result;
use std::sync::Arc;
use tracing::info;

mod processor;
mod rate_limiter;

use shared::state::AppState;

pub async fn start(state: Arc<AppState>) -> Result<()> {
    // Phase 1: 4 concurrent workers
    let worker_count: usize = std::env::var("WORKER_COUNT")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .unwrap_or(4);

    info!("Starting {} worker tasks", worker_count);

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
