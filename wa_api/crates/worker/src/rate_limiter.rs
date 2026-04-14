use shared::{redis_client::RedisClient, utils::random_delay_secs};
use std::time::Duration;
use tokio::time::sleep;
use tracing::debug;

/// Enforce inter-message delay for an evo API instance.
/// Randomizes between min and max to avoid detection patterns.
pub async fn enforce_delay(redis: &RedisClient, instance_name: &str, min_secs: u64, max_secs: u64) {
    let last_sent = redis
        .get_last_sent(instance_name)
        .await
        .unwrap_or(None)
        .unwrap_or(0);

    let required_gap = random_delay_secs(min_secs, max_secs);
    let now = shared::utils::now_unix_i64();
    let elapsed = (now - last_sent).max(0) as u64;

    if elapsed < required_gap {
        let wait = required_gap - elapsed;
        debug!(
            instance = %instance_name,
            wait_secs = wait,
            "Rate limit delay enforced"
        );
        sleep(Duration::from_secs(wait)).await;
    }
}

/// Adaptive delay: if delivery rate drops below 70%, increase to 15-25s range.
#[allow(dead_code)]
pub fn adaptive_delay_range(delivery_rate: f64, base_min: u64, base_max: u64) -> (u64, u64) {
    if delivery_rate < 0.70 {
        // Slow down
        (15, 25)
    } else if delivery_rate > 0.85 {
        // Back to normal
        (base_min, base_max)
    } else {
        // Intermediate
        (base_min, base_max)
    }
}
