use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

use shared::state::AppState;
use shared::{redis_client::RedisClient, utils::now_unix};
use std::sync::Arc;

pub async fn start(state: Arc<AppState>) -> Result<()> {
    let redis = &state.redis;

    info!("Scheduler started — 500ms tick");

    loop {
        if let Err(e) = tick(redis).await {
            error!("Scheduler tick error: {}", e);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// One scheduler tick: promote all due jobs from ZSET to ready LIST.
async fn tick(redis: &RedisClient) -> Result<()> {
    let now = now_unix();

    // Scan all tenant scheduled queues
    let tenants = redis.scan_scheduled_tenants().await?;

    for tenant_id in &tenants {
        // Atomic promotion from ZSET to ready LIST
        let moved_count = redis.move_scheduled_to_ready(tenant_id, now).await?;

        if moved_count > 0 {
            let ready_len = redis.queue_len_ready(tenant_id).await.unwrap_or(0);
            info!(
                tenant_id = %tenant_id,
                moved_jobs = moved_count,
                queue_len_ready = ready_len,
                "Promoted scheduled jobs to ready queue atomically"
            );
        }
    }

    Ok(())
}
