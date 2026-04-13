use shared::{redis_client::RedisClient, utils::{hash_id, hash_phone}};
use uuid::Uuid;

/// Platform-wide daily limit (across ALL partners to same number)
pub const PLATFORM_DAILY_LIMIT: i64 = 5;
/// Per-partner daily limit to same number
pub const PARTNER_DAILY_LIMIT: i64 = 3;
/// Weekly limit per recipient across all partners
pub const WEEKLY_LIMIT: i64 = 15;
/// Max unique partners per recipient per day before blocking all
pub const MAX_PARTNERS_PER_DAY: i64 = 3;

pub struct SpamGuardResult {
    pub allowed: bool,
    pub reason: Option<String>,
}

/// Check all spam guard rules before sending/enqueuing.
/// Returns true if the message is allowed to proceed.
pub async fn check(
    redis: &RedisClient,
    phone: &str,
    tenant_id: &Uuid,
    partner_id: &Uuid,
) -> anyhow::Result<SpamGuardResult> {
    let phone_hash = hash_phone(phone);
    let _partner_hash = hash_id(partner_id);
    let tenant_id_str = tenant_id.to_string();

    // Rule 1: Global daily platform limit (5 msgs/day to same phone from any partner)
    let today_count = redis.spam_guard_get_today(&phone_hash).await?;
    if today_count >= PLATFORM_DAILY_LIMIT {
        return Ok(SpamGuardResult {
            allowed: false,
            reason: Some("platform_daily_limit".to_string()),
        });
    }

    // Rule 2: Multi-partner cap (if already received from 3+ different partners today, block all)
    let partner_count = redis.spam_guard_partner_count_today(&phone_hash).await?;
    if partner_count >= MAX_PARTNERS_PER_DAY {
        return Ok(SpamGuardResult {
            allowed: false,
            reason: Some("multi_partner_cap".to_string()),
        });
    }

    // Rule 3: Per-partner daily limit (3 msgs/day from same partner to same number)
    let partner_today = redis.get_partner_daily(&tenant_id_str, &phone_hash).await?;
    if partner_today >= PARTNER_DAILY_LIMIT {
        return Ok(SpamGuardResult {
            allowed: false,
            reason: Some("partner_daily_limit".to_string()),
        });
    }

    // Rule 4: Weekly volume cap (15 platform messages to any single phone in 7 days)
    let week_count = redis.spam_guard_get_week(&phone_hash).await?;
    if week_count >= WEEKLY_LIMIT {
        return Ok(SpamGuardResult {
            allowed: false,
            reason: Some("weekly_limit".to_string()),
        });
    }

    Ok(SpamGuardResult { allowed: true, reason: None })
}

/// Increment spam guard counters after a successful send.
#[allow(dead_code)]
pub async fn record_send(
    redis: &RedisClient,
    phone: &str,
    tenant_id: &Uuid,
    partner_id: &Uuid,
) -> anyhow::Result<()> {
    let phone_hash = hash_phone(phone);
    let partner_hash = hash_id(partner_id);
    let tenant_id_str = tenant_id.to_string();

    redis.spam_guard_incr_today(&phone_hash).await?;
    redis.spam_guard_incr_week(&phone_hash).await?;
    redis.incr_partner_daily(&tenant_id_str, &phone_hash).await?;
    redis.spam_guard_add_partner_today(&phone_hash, &partner_hash).await?;

    Ok(())
}
