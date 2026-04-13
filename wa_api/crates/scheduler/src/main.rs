use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use shared::{config::AppConfig, redis_client::RedisClient, utils::now_unix};

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
    let redis = RedisClient::new(&config.redis_url).await?;

    info!("Scheduler started — 500ms tick");

    loop {
        if let Err(e) = tick(&redis).await {
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
    }

    Ok(())
}
