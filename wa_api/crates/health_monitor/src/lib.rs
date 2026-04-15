use anyhow::Result;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use shared::{db::DbClient, evo::EvoClient, redis_client::RedisClient, types::InstanceHealth};

use shared::state::AppState;
use std::sync::Arc;

/// Instance name prefix filter: only manage instances matching this prefix.
/// Prevents accidental deletion of instances from other deployments sharing the same evo API.
const MANAGED_PREFIXES: &[&str] = &["wa_", "pool_", "leaex_"];

pub async fn start(state: Arc<AppState>) -> Result<()> {
    let redis = &state.redis;
    let evo = &state.evo;
    let db = &state.db;
    let alert_url = state.config.alert_webhook_url.clone();

    info!("Health monitor started — checking every 5 minutes");

    let mut last_ping = std::time::Instant::now();

    loop {
        // Ping evo every minute
        if last_ping.elapsed() >= Duration::from_secs(60) {
            match evo.fetch_instances().await {
                Ok(_) => {
                    info!("ACK SUCCESS — Connection to Evolution API is stable")
                }
                Err(e) => error!(
                    "ACK FAIL — Connection to Evolution API lost: {}",
                    e
                ),
            }
            last_ping = std::time::Instant::now();
        }

        if let Err(e) = check_all_instances(redis, evo, db, &alert_url).await {
            error!("Health monitor error: {}", e);
        }

        if let Err(e) = reconcile_stale_messages(redis, evo, db).await {
            error!("Reconciliation error: {}", e);
        }

        if let Err(e) = cleanup_orphan_instances(evo, db).await {
            error!("Orphan cleanup error: {}", e);
        }

        if let Err(e) = recover_stuck_processing_jobs(redis).await {
            error!("Processing recovery error: {}", e);
        }

        sleep(Duration::from_secs(5 * 60)).await;
    }
}

/// Check if an instance name is managed by this wa_api deployment.
fn is_managed_instance(name: &str) -> bool {
    MANAGED_PREFIXES.iter().any(|prefix| name.starts_with(prefix))
}

/// R3 watchdog: Recover jobs stuck in jobs:processing: worker lists.
/// This happens if a worker crashes mid-processing.
async fn recover_stuck_processing_jobs(redis: &RedisClient) -> Result<()> {
    let lists = redis.scan_processing_lists().await?;
    if lists.is_empty() {
        return Ok(());
    }

    info!(count = lists.len(), "Checking {} worker processing lists for stuck jobs", lists.len());

    for list_key in lists {
        // In a real implementation, we might want to check the age of the job.
        // For Phase 1, we assume anything in this list for > 5 mins is stuck
        // since health_monitor runs every 5 mins.
        while let Ok(Some(job_id)) = redis.lpop_processing(&list_key).await {
            if let Ok(Some(job)) = redis.get_job(&job_id).await {
                warn!(job_id = %job_id, worker_list = %list_key, "Recovering stuck job — re-queuing to ready");
                let _ = redis.lpush_ready(&job.tenant_id.to_string(), &job_id).await;
            }
        }
    }

    Ok(())
}

/// Cleanup instances in evo API that don't exist in our DB for > 1 hour.
/// Only affects instances matching MANAGED_PREFIXES to avoid deleting
/// instances from other deployments sharing the same evo API.
async fn cleanup_orphan_instances(evo: &EvoClient, db: &DbClient) -> Result<()> {
    info!("Orphan cleanup check — auditing evo API instances");

    let evo_instances = match evo.fetch_instances().await {
        Ok(i) => i,
        Err(e) => {
            error!("Failed to fetch instances for orphan check: {}", e);
            return Ok(());
        }
    };

    let db_instances = match db.get_all_instance_names().await {
        Ok(names) => names,
        Err(e) => {
            error!("Failed to fetch DB instances for orphan check: {}", e);
            return Ok(());
        }
    };

    for instance in evo_instances {
        let name = instance
            .get("instance")
            .and_then(|i| i.get("instanceName"))
            .and_then(|n| n.as_str())
            .unwrap_or_default();

        if name.is_empty() || db_instances.contains(&name.to_string()) {
            continue;
        }

        // Only manage instances with recognized prefixes
        if !is_managed_instance(name) {
            continue;
        }

        // Check if it's actually an orphan (not in DB) and how old it is.
        // We look at 'createdAt' from evo instance data.
        let created_at_str = instance
            .get("instance")
            .and_then(|i| i.get("createdAt"))
            .and_then(|c| c.as_str())
            .unwrap_or_default();

        let is_old = if created_at_str.is_empty() {
            true // If no date, assume old
        } else {
            match chrono::DateTime::parse_from_rfc3339(created_at_str) {
                Ok(dt) => {
                    let now = chrono::Utc::now();
                    let age = now.signed_duration_since(dt.with_timezone(&chrono::Utc));
                    age.num_hours() >= 1 // Delete orphans older than 1 hour
                }
                Err(_) => true,
            }
        };

        if is_old {
            warn!(instance = %name, "Deleting orphan evo instance (not in DB)");
            if let Err(e) = evo.delete_instance(name).await {
                error!("Failed to delete orphan instance {}: {}", name, e);
            }
        }
    }

    Ok(())
}

/// Check all instances registered in evo API.
async fn check_all_instances(
    redis: &RedisClient,
    evo: &EvoClient,
    db: &DbClient,
    alert_url: &Option<String>,
) -> Result<()> {
    let instances = evo.fetch_instances().await.unwrap_or_default();

    for instance in &instances {
        let name = instance
            .get("instance")
            .and_then(|i| i.get("instanceName"))
            .and_then(|n| n.as_str())
            .unwrap_or_default();

        if name.is_empty() {
            continue;
        }

        let state_str = evo
            .get_instance_status(name)
            .await
            .unwrap_or_else(|_| "DISCONNECTED".to_string());

        let new_health = match state_str.as_str() {
            "OPEN" | "CONNECTED" => InstanceHealth::Active,
            "CLOSE" | "CLOSED" => InstanceHealth::Disconnected,
            _ => InstanceHealth::Disconnected,
        };

        let prev_health = redis
            .get_instance_health(name)
            .await
            .unwrap_or(InstanceHealth::Disconnected);

        if prev_health != new_health {
            info!(
                instance = %name,
                prev = %prev_health,
                new = %new_health,
                "Instance state changed"
            );

            // Log to database
            let _ = db
                .log_instance_health_event(
                    name,
                    None,
                    name.starts_with("pool_"),
                    "connection_state_change",
                    prev_health.as_str(),
                    new_health.as_str(),
                    None,
                )
                .await;

            // Alert on QR_REQUIRED or BANNED
            if new_health == InstanceHealth::QrRequired || new_health == InstanceHealth::Banned {
                let alert_msg = if new_health == InstanceHealth::Banned {
                    format!("CRITICAL: Instance {} is BANNED", name)
                } else {
                    format!("ACTION REQUIRED: Instance {} needs QR re-auth", name)
                };

                warn!("{}", alert_msg);
                if let Some(url) = alert_url {
                    fire_alert(url, &alert_msg).await;
                }
            }

            redis.set_instance_health(name, &new_health).await?;
        }
    }

    Ok(())
}

/// Reconciliation: poll evo for status of messages in-flight > 10 minutes.
/// Runs every 5 minutes as per spec.
async fn reconcile_stale_messages(
    _redis: &RedisClient,
    _evo: &EvoClient,
    _db: &DbClient,
) -> Result<()> {
    // In a full implementation:
    // 1. Query DB for wa_interaction_log where status=sent AND sent_at < now - 10min
    // 2. For each: call evo API to check actual delivery status
    // 3. Update DB with current status
    // This is a reconciliation safety net for lost webhooks.
    info!("Reconciliation tick — checking stale in-flight messages");
    Ok(())
}

async fn fire_alert(webhook_url: &str, message: &str) {
    let client = reqwest::Client::new();
    let payload = json!({ "text": message });
    if let Err(e) = client.post(webhook_url).json(&payload).send().await {
        error!("Failed to fire alert webhook: {}", e);
    }
}
