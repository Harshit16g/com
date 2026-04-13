use chrono::Utc;
use serde_json::json;
use shared::{
    db::{DbClient, InteractionLogInsert},
    evolution::EvolutionError,
    redis_client::RedisClient,
    types::{InstanceHealth, JobStatus, MessagePayload, WhatsAppJob},
    utils::{hash_phone, now_unix_i64},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::{rate_limiter, WorkerState};

const RETRY_DELAYS: [u64; 3] = [30, 120, 600]; // +30s, +2min, +10min

pub async fn run_worker(worker_id: usize, state: Arc<WorkerState>) {
    info!(worker_id, "Worker started");

    loop {
        // Get tenant queues to listen on
        let tenant_ids = match state.redis.clone().scan_ready_tenants().await {
            Ok(ids) if !ids.is_empty() => ids,
            _ => {
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        // BRPOP with 1s timeout
        let job_result = state.redis.clone().brpop_ready(&tenant_ids, 1.0).await;

        match job_result {
            Ok(Some((_, job_json))) => match serde_json::from_str::<WhatsAppJob>(&job_json) {
                Ok(job) => {
                    process_job(worker_id, job, Arc::clone(&state)).await;
                }
                Err(e) => {
                    error!(worker_id, "Failed to deserialize job: {}", e);
                }
            },
            Ok(None) => {
                // Timeout — no jobs, continue
            }
            Err(e) => {
                error!(worker_id, "BRPOP error: {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn process_job(worker_id: usize, job: WhatsAppJob, state: Arc<WorkerState>) {
    let job_id = job.job_id;
    let instance = &job.instance_name.clone();
    let tenant_id = &job.tenant_id.to_string();

    info!(
        worker_id,
        job_id = %job_id,
        instance = %instance,
        "Processing job"
    );

    let redis = state.redis.clone();

    // ── Step 1: Instance health check ─────────────────────────────────────
    let health = redis
        .get_instance_health(instance)
        .await
        .unwrap_or(InstanceHealth::Disconnected);

    match health {
        InstanceHealth::Active | InstanceHealth::Connecting => {} // proceed
        InstanceHealth::QrRequired => {
            warn!(job_id = %job_id, instance = %instance, "QR_REQUIRED — moving to DLQ");
            move_to_dlq(&redis, &job, "instance_needs_auth").await;
            return;
        }
        InstanceHealth::Banned => {
            error!(job_id = %job_id, instance = %instance, "BANNED instance — moving to DLQ");
            move_to_dlq(&redis, &job, "instance_banned").await;
            return;
        }
        _ => {
            warn!(job_id = %job_id, instance = %instance, health = %health, "Instance not active — requeuing with delay");
            requeue_with_delay(&redis, &job, 300).await;
            return;
        }
    }

    // ── Step 2: Acquire per-instance send lock (prevents concurrent sends) ─
    let lock_key = format!("send_lock:{}", instance);
    let got_lock = redis
        .set_nx_ex(&lock_key, &job_id.to_string(), 30)
        .await
        .unwrap_or(false);

    if !got_lock {
        // Another worker is sending on this instance — requeue after brief wait
        sleep(Duration::from_millis(500)).await;
        let job_json = serde_json::to_string(&job).unwrap_or_default();
        let _ = redis.lpush_ready(tenant_id, &job_json).await;
        return;
    }

    // ── Step 3: Rate limit delay ───────────────────────────────────────────
    rate_limiter::enforce_delay(
        &redis,
        instance,
        state.config.min_send_delay_secs,
        state.config.max_send_delay_secs,
    )
    .await;

    // ── Step 4: Spam guard double-check ───────────────────────────────────
    let phone_hash = hash_phone(&job.recipient_phone);
    let today_count = redis.spam_guard_get_today(&phone_hash).await.unwrap_or(0);
    if today_count >= 5 {
        warn!(job_id = %job_id, "Spam guard blocked at worker — deferring");
        defer_to_tomorrow(&redis, &job).await;
        // Release lock
        let _ = redis.del(&lock_key).await;
        return;
    }

    // ── Step 5: Opt-out check ─────────────────────────────────────────────
    match state.db.is_opted_out_platform(&phone_hash).await {
        Ok(true) => {
            info!(job_id = %job_id, "Recipient opted out — blocking permanently");
            persist_status(&state.db, &job, JobStatus::BlockedOptOut, None, None).await;
            let _ = redis.del(&lock_key).await;
            return;
        }
        Err(e) => {
            warn!("Opt-out check error: {} — proceeding", e);
        }
        _ => {}
    }

    // ── Step 6: Idempotency check ─────────────────────────────────────────
    if let Ok(true) = redis.is_already_sent(&job.idempotency_key).await {
        info!(job_id = %job_id, "Duplicate send detected — skipping");
        persist_status(&state.db, &job, JobStatus::Duplicate, None, None).await;
        let _ = redis.del(&lock_key).await;
        return;
    }

    // ── Step 7: Send via Evolution API ────────────────────────────────────
    let text = match &job.payload {
        MessagePayload::Text { body } => body.clone(),
        MessagePayload::Template { body, .. } => body.clone(),
    };

    let send_result = state
        .evolution
        .send_text(instance, &job.recipient_phone, &text)
        .await;

    // ── Step 8: Update rate limit timestamp ───────────────────────────────
    let _ = redis.set_last_sent(instance, now_unix_i64()).await;
    // Release lock
    let _ = redis.del(&lock_key).await;

    // ── Step 9: Handle result ─────────────────────────────────────────────
    match send_result {
        Ok(msg_id) => {
            info!(job_id = %job_id, msg_id = %msg_id, "Message sent successfully");

            // Mark idempotency
            let _ = redis.mark_sent(&job.idempotency_key).await;

            // Increment spam counters
            let _ = redis.spam_guard_incr_today(&phone_hash).await;
            let _ = redis.spam_guard_incr_week(&phone_hash).await;
            let _ = redis.incr_partner_daily(tenant_id, &phone_hash).await;
            let _ = redis
                .spam_guard_add_partner_today(&phone_hash, &shared::utils::hash_id(&job.partner_id))
                .await;

            persist_status(&state.db, &job, JobStatus::Sent, Some(msg_id), None).await;
        }

        Err(e @ (EvolutionError::Transient(_) | EvolutionError::RateLimit { .. })) => {
            let retry_after = match &e {
                EvolutionError::RateLimit { retry_after_secs } => *retry_after_secs,
                _ => RETRY_DELAYS
                    .get(job.retry_count as usize)
                    .copied()
                    .unwrap_or(600),
            };

            if job.retry_count >= 3 {
                error!(job_id = %job_id, "Max retries exceeded — DLQ");
                move_to_dlq(&redis, &job, &e.to_string()).await;
            } else {
                warn!(job_id = %job_id, retry_count = job.retry_count, retry_after, "Retrying job");
                retry_with_delay(&redis, &job, retry_after).await;
            }
        }

        Err(EvolutionError::InstanceDisconnected(_msg)) => {
            warn!(instance = %instance, "Instance disconnected — pausing jobs");
            let _ = redis
                .set_instance_health(instance, &InstanceHealth::Disconnected)
                .await;
            requeue_with_delay(&redis, &job, 300).await;
        }

        Err(EvolutionError::AuthRequired(_msg)) => {
            warn!(instance = %instance, "Auth required — moving to DLQ");
            let _ = redis
                .set_instance_health(instance, &InstanceHealth::QrRequired)
                .await;
            move_to_dlq(&redis, &job, "instance_needs_auth").await;
        }

        Err(EvolutionError::InvalidRecipient(msg)) => {
            warn!(job_id = %job_id, "Invalid recipient — permanent failure");
            persist_status(
                &state.db,
                &job,
                JobStatus::Failed,
                None,
                Some(format!("invalid_recipient: {}", msg)),
            )
            .await;
        }

        Err(EvolutionError::Banned(msg)) => {
            error!(instance = %instance, "INSTANCE BANNED — escalating");
            let _ = redis
                .set_instance_health(instance, &InstanceHealth::Banned)
                .await;
            move_to_dlq(&redis, &job, &format!("banned: {}", msg)).await;
        }
    }
}

async fn move_to_dlq(redis: &RedisClient, job: &WhatsAppJob, reason: &str) {
    let mut job_copy = job.clone();
    job_copy.status = JobStatus::Failed;
    let json = serde_json::to_string(&json!({
        "job": job_copy,
        "dlq_reason": reason,
        "dlq_at": Utc::now(),
    }))
    .unwrap_or_default();
    let _ = redis.lpush_dlq(&job.tenant_id.to_string(), &json).await;
}

async fn requeue_with_delay(redis: &RedisClient, job: &WhatsAppJob, delay_secs: u64) {
    let scheduled_at = Utc::now() + chrono::Duration::seconds(delay_secs as i64);
    let mut job_copy = job.clone();
    job_copy.scheduled_at = scheduled_at;
    let _job_json = serde_json::to_string(&job_copy).unwrap_or_default();
    let _ = redis
        .zadd_scheduled(
            &job.tenant_id.to_string(),
            scheduled_at.timestamp() as f64,
            &job.job_id.to_string(),
        )
        .await;
    let _ = redis.save_job(&job_copy).await;
}

async fn retry_with_delay(redis: &RedisClient, job: &WhatsAppJob, delay_secs: u64) {
    let mut job_copy = job.clone();
    job_copy.retry_count += 1;
    job_copy.scheduled_at = Utc::now() + chrono::Duration::seconds(delay_secs as i64);
    requeue_with_delay(redis, &job_copy, delay_secs).await;
}

async fn defer_to_tomorrow(redis: &RedisClient, job: &WhatsAppJob) {
    // Defer to next calendar day (midnight IST ≈ 18:30 UTC)
    let tomorrow = Utc::now() + chrono::Duration::days(1);
    let midnight = tomorrow
        .date_naive()
        .and_hms_opt(18, 30, 0)
        .map(|dt| chrono::DateTime::from_naive_utc_and_offset(dt, Utc))
        .unwrap_or(tomorrow);

    let mut job_copy = job.clone();
    job_copy.scheduled_at = midnight;
    job_copy.status = JobStatus::DeferredSpamGuard;

    let _ = redis
        .zadd_scheduled(
            &job.tenant_id.to_string(),
            midnight.timestamp() as f64,
            &job.job_id.to_string(),
        )
        .await;
    let _ = redis.save_job(&job_copy).await;
}

async fn persist_status(
    db: &DbClient,
    job: &WhatsAppJob,
    status: JobStatus,
    msg_id: Option<String>,
    error_reason: Option<String>,
) {
    let phone_hash = hash_phone(&job.recipient_phone);

    // Pool sends show "leaex_pool" — never the actual pool number
    let instance_used = if job.message_type == shared::types::MessageType::Campaign {
        "leaex_pool".to_string()
    } else {
        job.instance_name.clone()
    };

    let log = InteractionLogInsert {
        tenant_id: job.tenant_id,
        campaign_id: job.campaign_id,
        message_type: job.message_type.as_str().to_string(),
        recipient_phone_hash: phone_hash,
        recipient_phone: job.recipient_phone.clone(),
        recipient_name: job.recipient_name.clone(),
        instance_used,
        status: status.as_str().to_string(),
        evolution_msg_id: msg_id,
        error_reason,
        retry_count: job.retry_count as i16,
        scheduled_at: job.scheduled_at,
        sent_at: if status == JobStatus::Sent {
            Some(Utc::now())
        } else {
            None
        },
        idempotency_key: job.idempotency_key.clone(),
    };

    if let Err(e) = db.insert_interaction(&log).await {
        error!("Failed to persist interaction log: {}", e);
    }
}
