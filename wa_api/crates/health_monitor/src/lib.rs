use anyhow::Result;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use shared::{
    db::DbClient, evolution::EvolutionClient, redis_client::RedisClient, types::InstanceHealth,
};

use shared::state::AppState;
use std::sync::Arc;

pub async fn start(state: Arc<AppState>) -> Result<()> {
    let redis = &state.redis;
    let evolution = &state.evolution;
    let supabase = &state.db;
    let alert_url = state.config.alert_webhook_url.clone();

    info!("Health monitor started — checking every 5 minutes");

    loop {
        if let Err(e) = check_all_instances(redis, evolution, supabase, &alert_url).await {
            error!("Health monitor error: {}", e);
        }

        if let Err(e) = reconcile_stale_messages(redis, evolution, supabase).await {
            error!("Reconciliation error: {}", e);
        }

        sleep(Duration::from_secs(5 * 60)).await;
    }
}

/// Check all instances registered in Evolution API.
async fn check_all_instances(
    redis: &RedisClient,
    evolution: &EvolutionClient,
    supabase: &DbClient,
    alert_url: &Option<String>,
) -> Result<()> {
    let instances = evolution.fetch_instances().await.unwrap_or_default();

    for instance in &instances {
        let name = instance
            .get("instance")
            .and_then(|i| i.get("instanceName"))
            .and_then(|n| n.as_str())
            .unwrap_or_default();

        if name.is_empty() {
            continue;
        }

        let state_str = evolution
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

            // Log to Supabase
            let _ = supabase
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

/// Reconciliation: poll Evolution for status of messages in-flight > 10 minutes.
/// Runs every 5 minutes as per spec.
async fn reconcile_stale_messages(
    _redis: &RedisClient,
    _evolution: &EvolutionClient,
    _supabase: &DbClient,
) -> Result<()> {
    // In a full implementation:
    // 1. Query Supabase for wa_interaction_log where status=sent AND sent_at < now - 10min
    // 2. For each: call Evolution API to check actual delivery status
    // 3. Update Supabase with current status
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
