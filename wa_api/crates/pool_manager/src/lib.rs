use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use shared::{
    evolution::EvolutionClient,
    redis_client::RedisClient,
    types::{InstanceHealth, PoolNumberState},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PoolInstance {
    pub name: String,
    pub state: String,
    pub daily_sent: u32,
    pub daily_limit: u32,
    pub warmup_day: u32,
    pub last_used: Option<i64>,
    pub consecutive_failures: u32,
}

impl PoolInstance {
    pub fn is_active(&self) -> bool {
        self.state == PoolNumberState::Active.to_string() && self.daily_sent < self.daily_limit
    }
}

use shared::state::AppState;
use std::sync::Arc;

pub async fn start(state: Arc<AppState>) -> Result<()> {
    let redis = &state.redis;
    let evolution = &state.evolution;

    info!("Pool manager started — health check every 15 minutes");

    loop {
        if let Err(e) = health_check_all(&redis, &evolution).await {
            error!("Pool health check error: {}", e);
        }

        // Update available set in Redis (used by api_gateway for routing)
        if let Err(e) = refresh_available_set(&redis).await {
            error!("Pool available set refresh error: {}", e);
        }

        // Advance warmup counters (daily)
        if let Err(e) = advance_warmup(&redis).await {
            error!("Warmup advance error: {}", e);
        }

        // 15 minute interval
        sleep(Duration::from_secs(15 * 60)).await;
    }
}

/// Ping each pool instance and update health state.
async fn health_check_all(redis: &RedisClient, evolution: &EvolutionClient) -> Result<()> {
    let instances = get_all_pool_instances(redis).await?;

    for mut instance in instances {
        let _prev_state = instance.state.clone();

        match evolution.get_instance_status(&instance.name).await {
            Ok(state) => {
                let health = match state.as_str() {
                    "OPEN" | "CONNECTED" => InstanceHealth::Active,
                    "CLOSE" | "CLOSED" | "DISCONNECTED" => InstanceHealth::Disconnected,
                    _ => InstanceHealth::Disconnected,
                };

                redis.set_instance_health(&instance.name, &health).await?;

                // Reset failure counter on success
                instance.consecutive_failures = 0;

                if health == InstanceHealth::Active && instance.daily_sent < instance.daily_limit {
                    instance.state = PoolNumberState::Active.to_string();
                }

                info!(
                    pool = %instance.name,
                    state = %instance.state,
                    daily_sent = instance.daily_sent,
                    "Pool health OK"
                );
            }
            Err(e) => {
                instance.consecutive_failures += 1;
                warn!(
                    pool = %instance.name,
                    failures = instance.consecutive_failures,
                    "Pool health check failed: {}",
                    e
                );

                // > 3 failures in 1h → FLAGGED
                if instance.consecutive_failures >= 3 {
                    instance.state = PoolNumberState::Flagged.to_string();
                    redis
                        .set_instance_health(&instance.name, &InstanceHealth::Flagged)
                        .await?;
                    error!(pool = %instance.name, "Pool number FLAGGED — removed from rotation");
                }
            }
        }

        save_pool_instance(redis, &instance).await?;
    }

    Ok(())
}

/// Refresh the pool:available set in Redis.
async fn refresh_available_set(redis: &RedisClient) -> Result<()> {
    let instances = get_all_pool_instances(redis).await?;

    // Clear and rebuild
    redis.del("pool:available").await?;

    for instance in &instances {
        if instance.is_active() {
            redis.pool_add_available(&instance.name).await?;
        }
    }

    let active_count = instances.iter().filter(|i| i.is_active()).count();
    info!(
        "Pool available set refreshed: {}/{} active",
        active_count,
        instances.len()
    );

    Ok(())
}

/// Advance warmup: increment warmup_day and graduate to ACTIVE at day 25.
async fn advance_warmup(redis: &RedisClient) -> Result<()> {
    let instances = get_all_pool_instances(redis).await?;

    for mut instance in instances {
        if instance.state == PoolNumberState::Warming.to_string() {
            instance.warmup_day += 1;
            // Increase daily limit by 20 each day
            instance.daily_limit = (20 * instance.warmup_day).min(500);

            if instance.warmup_day >= 25 {
                instance.state = PoolNumberState::Active.to_string();
                instance.daily_limit = 500;
                info!(pool = %instance.name, "Pool number graduated from WARMING to ACTIVE");
            }

            save_pool_instance(redis, &instance).await?;
        }
    }

    Ok(())
}

/// Get all pool instances from Redis.
async fn get_all_pool_instances(redis: &RedisClient) -> Result<Vec<PoolInstance>> {
    // Pool instances are stored as pool:instance:{name} keys
    let _keys: Vec<String> = {
        // We use Redis KEYS (fine for small pool, use SCAN for large sets)
        let pattern = "pool:instance:*";
        redis.get_string(pattern).await?;
        // Using scan logic
        vec![] // TODO: implement KEYS scan properly
    };

    // For Phase 1: read from a known list stored in pool:instances
    let instances_json = redis.get_string("pool:instances").await?;
    match instances_json {
        Some(json) => Ok(serde_json::from_str(&json).unwrap_or_default()),
        None => Ok(vec![]),
    }
}

async fn save_pool_instance(redis: &RedisClient, instance: &PoolInstance) -> Result<()> {
    // Load all, update this one, save back
    let mut all = get_all_pool_instances(redis).await?;
    if let Some(pos) = all.iter().position(|i| i.name == instance.name) {
        all[pos] = instance.clone();
    } else {
        all.push(instance.clone());
    }
    let json = serde_json::to_string(&all)?;
    redis.set_string("pool:instances", &json).await?;
    Ok(())
}

/// Register a new pool number (admin operation).
pub async fn register_pool_number(redis: &RedisClient, name: &str) -> Result<()> {
    let instance = PoolInstance {
        name: name.to_string(),
        state: PoolNumberState::Warming.to_string(),
        daily_sent: 0,
        daily_limit: 20, // Start at 20 msgs/day for warm-up
        warmup_day: 0,
        last_used: None,
        consecutive_failures: 0,
    };
    save_pool_instance(redis, &instance).await?;
    info!(pool = %name, "New pool number registered for warm-up");
    Ok(())
}
