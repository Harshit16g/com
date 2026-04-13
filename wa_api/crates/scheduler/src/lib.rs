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
        let job_ids = redis.zrangebyscore_ready(tenant_id, now).await?;

        for job_id in &job_ids {
            // Get full job metadata
            let job = match redis.get_job(job_id).await? {
                Some(j) => j,
                None => {
                    // Job metadata expired — remove from ZSET
                    redis.zrem_scheduled(tenant_id, job_id).await?;
                    continue;
                }
            };

            // Spam guard pre-check (avoid moving blocked jobs to ready queue)
            let phone_hash = shared::utils::hash_phone(&job.recipient_phone);
            let today_count = redis.spam_guard_get_today(&phone_hash).await.unwrap_or(0);
            if today_count >= 5 {
                // Defer to tomorrow instead of promoting
                let tomorrow = chrono::Utc::now() + chrono::Duration::days(1);
                let new_score = tomorrow.timestamp() as f64;
                redis.zrem_scheduled(tenant_id, job_id).await?;
                redis.zadd_scheduled(tenant_id, new_score, job_id).await?;
                info!(job_id = %job_id, "Spam guard deferred job to tomorrow");
                continue;
            }

            // Move to ready queue
            let job_json = serde_json::to_string(&job)?;
            redis.lpush_ready(tenant_id, &job_json).await?;
            redis.zrem_scheduled(tenant_id, job_id).await?;
        }

        if !job_ids.is_empty() {
            let ready_len = redis.queue_len_ready(tenant_id).await.unwrap_or(0);
            info!(
                tenant_id = %tenant_id,
                moved_jobs = job_ids.len(),
                queue_len_ready = ready_len,
                "Promoted scheduled jobs to ready queue"
            );
        }
    }

    Ok(())
}
