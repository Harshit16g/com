use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

use shared::{
    evo::EvoClient,
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
    let evo = &state.evo;

    info!("Pool manager started — health check every 15 minutes");

    loop {
        if let Err(e) = health_check_all(redis, evo).await {
            error!("Pool health check error: {}", e);
        }

        // Update available set in Redis
        if let Err(e) = refresh_available_set(redis).await {
            error!("Pool available set refresh error: {}", e);
        }

        // Advance warmup counters (daily)
        if let Err(e) = advance_warmup(redis).await {
            error!("Warmup advance error: {}", e);
        }

        sleep(Duration::from_secs(15 * 60)).await;
    }
}

async fn health_check_all(redis: &RedisClient, evo: &EvoClient) -> Result<()> {
    let instances = get_all_pool_instances(redis).await?;

    for mut instance in instances {
        match evo.get_instance_status(&instance.name).await {
            Ok(state) => {
                let health = match state.as_str() {
                    "OPEN" | "CONNECTED" => InstanceHealth::Active,
                    _ => InstanceHealth::Disconnected,
                };

                let _ = redis.set_instance_health(&instance.name, &health).await;
                instance.consecutive_failures = 0;

                if health == InstanceHealth::Active && instance.daily_sent < instance.daily_limit {
                    if instance.state == PoolNumberState::Flagged.to_string() {
                        instance.state = PoolNumberState::Active.to_string();
                    }
                }
            }
            Err(_) => {
                instance.consecutive_failures += 1;
                if instance.consecutive_failures >= 3 {
                    instance.state = PoolNumberState::Flagged.to_string();
                    let _ = redis
                        .set_instance_health(&instance.name, &InstanceHealth::Flagged)
                        .await;
                }
            }
        }
        save_pool_instance(redis, &instance).await?;
    }
    Ok(())
}

async fn refresh_available_set(redis: &RedisClient) -> Result<()> {
    let instances = get_all_pool_instances(redis).await?;
    let _ = redis.del("pool:available").await;

    for instance in &instances {
        if instance.is_active() {
            let _ = redis.pool_add_available(&instance.name).await;
        }
    }
    Ok(())
}

async fn advance_warmup(redis: &RedisClient) -> Result<()> {
    let instances = get_all_pool_instances(redis).await?;
    for mut instance in instances {
        if instance.state == PoolNumberState::Warming.to_string() {
            instance.warmup_day += 1;
            instance.daily_limit = (20 * instance.warmup_day).min(500);
            if instance.warmup_day >= 25 {
                instance.state = PoolNumberState::Active.to_string();
                instance.daily_limit = 500;
            }
            save_pool_instance(redis, &instance).await?;
        }
    }
    Ok(())
}

/// Get all pool instances — each stored as a separate Redis key (pool:instance:{name}).
/// This avoids read-modify-write races on a single JSON blob.
pub async fn get_all_pool_instances(redis: &RedisClient) -> Result<Vec<PoolInstance>> {
    // Get the list of all pool instance names from the tracking set
    let names = redis.pool_get_instance_names().await?;
    let mut instances = Vec::new();
    for name in names {
        if let Ok(Some(instance)) = get_pool_instance(redis, &name).await {
            instances.push(instance);
        }
    }
    Ok(instances)
}

/// Get a single pool instance from Redis.
async fn get_pool_instance(redis: &RedisClient, name: &str) -> Result<Option<PoolInstance>> {
    let key = format!("pool:instance:{}", name);
    let raw: Option<String> = redis.get_string(&key).await?;
    match raw {
        Some(json) => Ok(serde_json::from_str(&json).ok()),
        None => Ok(None),
    }
}

/// Save a single pool instance to its own Redis key.
/// No read-modify-write race: each instance key is independent.
async fn save_pool_instance(redis: &RedisClient, instance: &PoolInstance) -> Result<()> {
    let key = format!("pool:instance:{}", instance.name);
    let json = serde_json::to_string(instance)?;
    let _ = redis.set_string(&key, &json).await;
    // Ensure the instance name is in the tracking set
    let _ = redis.pool_add_instance_name(&instance.name).await;
    Ok(())
}

pub async fn register_pool_number(redis: &RedisClient, name: &str) -> Result<()> {
    let instance = PoolInstance {
        name: name.to_string(),
        state: PoolNumberState::Warming.to_string(),
        daily_sent: 0,
        daily_limit: 20,
        warmup_day: 0,
        last_used: None,
        consecutive_failures: 0,
    };
    save_pool_instance(redis, &instance).await?;
    Ok(())
}
