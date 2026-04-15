use chrono::{Timelike, Utc};
use rand::Rng;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::types::MessagePayload;

/// R7: Calculate seconds until the next IST midnight (UTC+5:30).
pub fn seconds_until_ist_midnight() -> u64 {
    let now = Utc::now();
    // IST is UTC + 5:30
    let ist_offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    let ist_now = now.with_timezone(&ist_offset);

    let tomorrow_midnight = (ist_now + chrono::Duration::days(1))
        .with_hour(0)
        .and_then(|t| t.with_minute(0))
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
        .unwrap();

    let diff = tomorrow_midnight - ist_now;
    diff.num_seconds().max(1) as u64
}

/// SHA-256 hash returning hex string.
pub fn sha256(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

/// Hash a phone number for use in spam guard (no PII stored in plaintext).
pub fn hash_phone(phone: &str) -> String {
    sha256(phone)
}

/// Hash a partner/tenant ID.
pub fn hash_id(id: &Uuid) -> String {
    sha256(&id.to_string())
}

/// Mask a phone number: preserves first 3 chars and last 4, e.g. "+91XXXXXXXXXX" → "+91 XXXXX X1234"
pub fn mask_phone(phone: &str) -> String {
    let len = phone.len();
    if len >= 8 {
        let prefix = &phone[..3];   // e.g. "+91" or "+44"
        let last4 = &phone[len - 4..];
        format!("{} XXXXX X{}", prefix, last4)
    } else if len >= 4 {
        let last4 = &phone[len - 4..];
        format!("XX{}", last4)
    } else {
        "XXXXXX".to_string()
    }
}

/// Compute idempotency key for a job.
/// sha256(tenant_id + phone + template_hash + campaign_id)
pub fn idempotency_key(
    tenant_id: &Uuid,
    phone: &str,
    template_hash: &str,
    campaign_id: Option<&Uuid>,
) -> String {
    let campaign_str = campaign_id.map(|id| id.to_string()).unwrap_or_default();
    sha256(&format!(
        "{}{}{}{}",
        tenant_id, phone, template_hash, campaign_str
    ))
}

/// Compute a template hash from message payload.
pub fn template_hash(payload: &MessagePayload) -> String {
    match payload {
        MessagePayload::Text { body } => sha256(body),
        MessagePayload::Template { template_name, .. } => sha256(template_name),
    }
}

/// Random delay between min and max seconds (inclusive).
pub fn random_delay_secs(min: u64, max: u64) -> u64 {
    rand::thread_rng().gen_range(min..=max)
}

/// Current unix timestamp as f64 (for Redis ZSET scores).
pub fn now_unix() -> f64 {
    Utc::now().timestamp() as f64
}

/// Current unix timestamp as i64.
pub fn now_unix_i64() -> i64 {
    Utc::now().timestamp()
}

/// Validate phone number format: must be +91XXXXXXXXXX (10 digits after +91).
pub fn validate_phone(phone: &str) -> bool {
    if !phone.starts_with("+91") {
        return false;
    }
    let digits: String = phone[3..].chars().filter(|c| c.is_ascii_digit()).collect();
    digits.len() == 10
}

/// Validate message length.
pub fn validate_message_length(body: &str) -> bool {
    body.len() <= 4096
}
