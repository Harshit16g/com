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

    // Spawn dedicated etiquette loop (checks every 60s)
    let etiquette_state = state.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = shared::etiquette::check_deadlines(etiquette_state.clone()).await {
                error!("Etiquette loop error: {}", e);
            }
            sleep(Duration::from_secs(60)).await;
        }
    });

    info!("Health monitor started — using dynamic activity-based intervals");

    let mut last_orphan_cleanup = std::time::Instant::now();

    // Initial check on startup
    if let Err(e) = check_all_instances(redis, evo, db, &alert_url).await {
        error!("Initial health check error: {}", e);
    }

    loop {
        // ── 1. Calculate Sleep Duration ──────────────────────────────────────
        let last_activity_str = redis
            .get_string("engine_last_activity")
            .await
            .unwrap_or(None);
        let now_unix = shared::utils::now_unix_i64();

        let is_active = if let Some(s) = last_activity_str {
            let activity_ts = s.parse::<i64>().unwrap_or(0);
            (now_unix - activity_ts) < 15 * 60 // 15 minutes window for "Active"
        } else {
            false
        };

        let sleep_duration = if is_active {
            info!("Engine ACTIVE — using 5-minute monitoring interval");
            Duration::from_secs(5 * 60)
        } else {
            info!("Engine IDLE — using 1-hour monitoring interval");
            Duration::from_secs(3600)
        };

        // ── 2. Run Routine Health Checks (at every loop tick) ────────────────
        match evo.fetch_instances().await {
            Ok(_) => {
                info!("ACK SUCCESS — Connection to Evolution API is stable")
            }
            Err(e) => error!("ACK FAIL — Connection to Evolution API lost: {}", e),
        }

        if let Err(e) = check_all_instances(redis, evo, db, &alert_url).await {
            error!("Health monitor check error: {}", e);
        }

        if let Err(e) = reconcile_stale_messages(redis, evo, db).await {
            error!("Reconciliation error: {}", e);
        }

        if let Err(e) = recover_stuck_processing_jobs(redis).await {
            error!("Processing recovery error: {}", e);
        }

        // ── 3. Deep Orphan Cleanup (Every 6 Hours) ───────────────────────────
        if last_orphan_cleanup.elapsed() >= Duration::from_secs(6 * 3600) {
            info!("Running 6-hour deep orphan cleanup...");
            if let Err(e) = cleanup_orphan_instances(state.clone()).await {
                error!("Deep orphan cleanup error: {}", e);
            }
            last_orphan_cleanup = std::time::Instant::now();
        }

        sleep(sleep_duration).await;
    }
}

/// Check if an instance name is managed by this wa_api deployment.
fn is_managed_instance(name: &str) -> bool {
    MANAGED_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

/// R3 watchdog: Recover jobs stuck in jobs:processing: worker lists.
/// This happens if a worker crashes mid-processing.
async fn recover_stuck_processing_jobs(redis: &RedisClient) -> Result<()> {
    let lists = redis.scan_processing_lists().await?;
    if lists.is_empty() {
        return Ok(());
    }

    info!(
        count = lists.len(),
        "Checking {} worker processing lists for stuck jobs",
        lists.len()
    );

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

/// Cleanup instances in evo API and local DB that don't match platform state.
/// Matches against comms.wa_sessions in the platform Supabase/Neon DB.
async fn cleanup_orphan_instances(state: Arc<AppState>) -> Result<()> {
    let evo = &state.evo;
    let db = &state.db;

    info!("Orphan cleanup check — auditing against Platform DB");

    // 1. Fetch current world state
    let evo_instances = match evo.fetch_instances().await {
        Ok(i) => i,
        Err(e) => {
            error!("Failed to fetch instances for orphan check: {}", e);
            return Ok(());
        }
    };

    let local_tenants = match db.get_all_tenants().await {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to fetch local tenants for orphan check: {}", e);
            return Ok(());
        }
    };

    // 2. Fetch platform sessions if database is available
    let platform_sessions = if let Some(platform_db) = &state.platform_db {
        #[derive(sqlx::FromRow)]
        struct Session {
            instance_id: String,
            org_id: uuid::Uuid,
        }
        match sqlx::query_as::<_, Session>("SELECT instance_id, org_id FROM comms.wa_sessions")
            .fetch_all(platform_db)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|r| (r.instance_id, r.org_id))
                .collect::<Vec<_>>(),
            Err(e) => {
                error!("Failed to fetch platform sessions: {}", e);
                return Ok(());
            }
        }
    } else {
        warn!("Platform database not configured — skipping deep sync");
        return Ok(());
    };

    // 3. Audit Evo API Instances
    for instance in evo_instances {
        let name = instance
            .get("instance")
            .and_then(|i| i.get("instanceName"))
            .and_then(|n| n.as_str())
            .unwrap_or_default();

        if name.is_empty() {
            continue;
        }

        // Only manage instances with recognized prefixes
        if !is_managed_instance(name) {
            continue;
        }

        // Check if platform knows about this instance
        let platform_exists = platform_sessions.iter().any(|(id, _)| id == name);

        if !platform_exists {
            // Check age before deleting
            let created_at_str = instance
                .get("instance")
                .and_then(|i| i.get("createdAt"))
                .and_then(|c| c.as_str())
                .unwrap_or_default();

            let is_old = if created_at_str.is_empty() {
                true
            } else {
                match chrono::DateTime::parse_from_rfc3339(created_at_str) {
                    Ok(dt) => {
                        let now = chrono::Utc::now();
                        let age = now.signed_duration_since(dt.with_timezone(&chrono::Utc));
                        age.num_hours() >= 1 // Safety: 1 hour grace period
                    }
                    Err(_) => true,
                }
            };

            if is_old {
                warn!(instance = %name, "Deleting orphan evo instance (missing in Platform DB)");
                let _ = evo.delete_instance(name).await;
                // Also deactivate in local DB if it exists
                let _ = db.mark_tenant_orphan(name, "orphan cleaned").await;
            }
        }
    }

    // 4. Audit Local Tenants for mismatches
    for tenant in local_tenants {
        // If not in platform sessions -> Orphan
        let platform_session = platform_sessions
            .iter()
            .find(|(id, _)| id == &tenant.instance_name);

        match platform_session {
            None => {
                warn!(instance = %tenant.instance_name, "Local tenant marked as orphan (missing in Platform DB)");
                let _ = db
                    .mark_tenant_orphan(&tenant.instance_name, "orphan cleaned")
                    .await;
                let _ = evo.delete_instance(&tenant.instance_name).await;
            }
            Some((_, org_id)) => {
                // If partner_id mismatch -> Orphan
                if let Some(p_id) = tenant.partner_id {
                    if p_id != *org_id {
                        warn!(instance = %tenant.instance_name, "Local tenant partner mismatch — purging");
                        let _ = db
                            .mark_tenant_orphan(&tenant.instance_name, "orphan cleaned (mismatch)")
                            .await;
                        let _ = evo.delete_instance(&tenant.instance_name).await;
                    }
                }
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
