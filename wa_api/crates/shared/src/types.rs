use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── Plan Tier ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PlanTier {
    Basic,
    Pro,
    Enterprise,
}

// ─── Tenant Context ─────────────────────────────────────────────────────────

/// Constructed ONCE at API Gateway auth layer and passed immutably through
/// the entire request pipeline. No service downstream may modify instance_name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantContext {
    pub agency_id: Uuid,
    pub tenant_id: Uuid,
    pub partner_id: Uuid,
    /// Evolution API instance identifier (e.g. "wa_glamour_studio_01")
    pub instance_name: String,
    /// +91XXXXXXXXXX of the connected WhatsApp number
    pub wa_number: String,
    pub plan_tier: PlanTier,
    /// Max messages per day from plan
    pub daily_limit: u32,
    /// Campaign features (Pro+ only)
    pub campaign_allowed: bool,
}

// ─── Message Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    Campaign,
    BookingConfirm,
    Reminder,
    Birthday,
    Anniversary,
    #[serde(rename = "manual_crm")]
    ManualCrm,
    ReEngagement,
    Inbound,
    Outbound,
    Feedback,
    Complaint,
}

impl MessageType {
    pub fn is_campaign(&self) -> bool {
        *self == MessageType::Campaign
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            MessageType::Campaign => "campaign",
            MessageType::BookingConfirm => "booking_confirm",
            MessageType::Reminder => "reminder",
            MessageType::Birthday => "birthday",
            MessageType::Anniversary => "anniversary",
            MessageType::ManualCrm => "manual_crm",
            MessageType::ReEngagement => "re_engagement",
            MessageType::Inbound => "inbound",
            MessageType::Outbound => "outbound",
            MessageType::Feedback => "feedback",
            MessageType::Complaint => "complaint",
        }
    }
}

// ─── Message Payload ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessagePayload {
    Text {
        body: String,
    },
    Template {
        template_name: String,
        variables: Vec<String>,
        body: String, // rendered body for Evolution API
    },
}

// ─── Job Status ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Sent,
    Delivered,
    Read,
    Failed,
    DeferredSpamGuard,
    BlockedOptOut,
    Duplicate,
    ExpiredDlq,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Sent => "sent",
            JobStatus::Delivered => "delivered",
            JobStatus::Read => "read",
            JobStatus::Failed => "failed",
            JobStatus::DeferredSpamGuard => "deferred_spam",
            JobStatus::BlockedOptOut => "blocked_optout",
            JobStatus::Duplicate => "duplicate",
            JobStatus::ExpiredDlq => "expired_dlq",
        }
    }
}

// ─── WhatsApp Job ────────────────────────────────────────────────────────────

/// The core job schema stored in Redis and processed by workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhatsAppJob {
    pub job_id: Uuid,
    pub tenant_id: Uuid,
    pub partner_id: Uuid,
    /// None for CRM direct messages
    pub campaign_id: Option<Uuid>,
    /// Which Evolution API instance to use
    pub instance_name: String,
    pub message_type: MessageType,
    /// +91XXXXXXXXXX
    pub recipient_phone: String,
    pub recipient_name: Option<String>,
    pub payload: MessagePayload,
    /// 0-3, then DLQ
    pub retry_count: u8,
    pub scheduled_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    /// sha256(tenant_id + phone + template_hash + campaign_id)
    pub idempotency_key: String,
    pub status: JobStatus,
}

// ─── Instance Health State ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InstanceHealth {
    Active,
    Connecting,
    QrRequired,
    Disconnected,
    Flagged,
    Banned,
}

impl InstanceHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceHealth::Active => "ACTIVE",
            InstanceHealth::Connecting => "CONNECTING",
            InstanceHealth::QrRequired => "QR_REQUIRED",
            InstanceHealth::Disconnected => "DISCONNECTED",
            InstanceHealth::Flagged => "FLAGGED",
            InstanceHealth::Banned => "BANNED",
        }
    }
}

impl std::fmt::Display for InstanceHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for InstanceHealth {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ACTIVE" => Ok(InstanceHealth::Active),
            "CONNECTING" => Ok(InstanceHealth::Connecting),
            "QR_REQUIRED" => Ok(InstanceHealth::QrRequired),
            "DISCONNECTED" => Ok(InstanceHealth::Disconnected),
            "FLAGGED" => Ok(InstanceHealth::Flagged),
            "BANNED" => Ok(InstanceHealth::Banned),
            _ => Err(format!("Unknown instance health: {}", s)),
        }
    }
}

// ─── Pool Number State ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PoolNumberState {
    Warming,
    Active,
    Cooling,
    Flagged,
    Resting,
    Retired,
}

impl std::fmt::Display for PoolNumberState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PoolNumberState::Warming => "WARMING",
            PoolNumberState::Active => "ACTIVE",
            PoolNumberState::Cooling => "COOLING",
            PoolNumberState::Flagged => "FLAGGED",
            PoolNumberState::Resting => "RESTING",
            PoolNumberState::Retired => "RETIRED",
        };
        write!(f, "{}", s)
    }
}

// ─── Campaign Status ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CampaignStatus {
    Draft,
    Running,
    Paused,
    Completed,
    Cancelled,
}

// ─── Defer Reason ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeferReason {
    SpamGuard,
    DailyLimitReached,
    WeeklyLimitReached,
    MultiPartnerCap,
}
