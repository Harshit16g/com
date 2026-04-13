use axum::{
    extract::State, http::StatusCode, response::IntoResponse, routing::post, Extension, Json,
    Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared::{
    types::{JobStatus, MessagePayload, MessageType, WhatsAppJob},
    utils::{idempotency_key, template_hash, validate_message_length, validate_phone},
};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::services::spam_guard;
use shared::state::AppState;
use shared::types::TenantContext;

// ─── Request / Response ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    /// Recipient phone in +91XXXXXXXXXX format
    pub phone: String,
    /// Message text (max 4096 chars)
    pub message: String,
    /// Optional customer name for interaction log
    pub recipient_name: Option<String>,
    /// Optional message type (defaults to manual_crm)
    pub message_type: Option<String>,
    /// Optional scheduling: ISO8601 datetime to send at
    pub scheduled_at: Option<chrono::DateTime<Utc>>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct SendMessageResponse {
    pub job_id: String,
    pub status: String,
    pub scheduled_at: chrono::DateTime<Utc>,
}

// ─── Handler ─────────────────────────────────────────────────────────────────

/// POST /message/send
/// Send a 1:1 CRM message through the partner's personal WA instance.
/// Rate limit: 60 req/min per tenant.
async fn send_message(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Json(req): Json<SendMessageRequest>,
) -> impl IntoResponse {
    // ── 1. Payload validation ──────────────────────────────────────────────
    if !validate_phone(&req.phone) {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "Invalid phone format. Expected +91XXXXXXXXXX (10 digits after +91)",
                "field": "phone"
            })),
        )
            .into_response();
    }

    if req.message.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": "Message cannot be empty", "field": "message"})),
        )
            .into_response();
    }

    if !validate_message_length(&req.message) {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": "Message exceeds 4096 character limit", "field": "message"})),
        )
            .into_response();
    }

    let msg_type = match req.message_type.as_deref().unwrap_or("manual_crm") {
        "booking_confirm" => MessageType::BookingConfirm,
        "reminder" => MessageType::Reminder,
        "birthday" => MessageType::Birthday,
        "anniversary" => MessageType::Anniversary,
        "re_engagement" => MessageType::ReEngagement,
        _ => MessageType::ManualCrm,
    };

    // ── 2. Check opt-out ───────────────────────────────────────────────────
    let phone_hash = shared::utils::hash_phone(&req.phone);

    if let Ok(true) = state.db.is_opted_out_platform(&phone_hash).await {
        return (
            StatusCode::OK,
            Json(json!({
                "status": "blocked_optout",
                "message": "Recipient has opted out platform-wide"
            })),
        )
            .into_response();
    }

    // ── 3. Spam guard ──────────────────────────────────────────────────────
    let redis = state.redis.clone();
    if let Ok(result) = spam_guard::check(&redis, &req.phone, &ctx.tenant_id, &ctx.partner_id).await
    {
        if !result.allowed {
            return (
                StatusCode::OK,
                Json(json!({
                    "status": "deferred_spam_guard",
                    "reason": result.reason
                })),
            )
                .into_response();
        }
    }

    // ── 4. Build job ───────────────────────────────────────────────────────
    let payload = MessagePayload::Text {
        body: req.message.clone(),
    };
    let t_hash = template_hash(&payload);
    let idem_key = idempotency_key(&ctx.tenant_id, &req.phone, &t_hash, None);

    // Check idempotency — don't re-enqueue duplicate
    if let Ok(true) = redis.is_already_sent(&idem_key).await {
        return (
            StatusCode::OK,
            Json(json!({
                "status": "duplicate",
                "message": "Message already sent or enqueued"
            })),
        )
            .into_response();
    }

    let job_id = Uuid::new_v4();
    let scheduled_at = req.scheduled_at.unwrap_or_else(Utc::now);

    let job = WhatsAppJob {
        job_id,
        tenant_id: ctx.tenant_id,
        partner_id: ctx.partner_id,
        campaign_id: None,
        instance_name: ctx.instance_name.clone(),
        message_type: msg_type,
        recipient_phone: req.phone.clone(),
        recipient_name: req.recipient_name.clone(),
        payload,
        retry_count: 0,
        scheduled_at,
        created_at: Utc::now(),
        idempotency_key: idem_key,
        status: JobStatus::Pending,
    };

    // ── 5. Save job + enqueue ──────────────────────────────────────────────
    let job_json = match serde_json::to_string(&job) {
        Ok(j) => j,
        Err(e) => {
            error!("Failed to serialize job: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Internal error"})),
            )
                .into_response();
        }
    };

    if let Err(e) = redis.save_job(&job).await {
        error!("Failed to save job: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "Failed to enqueue message"})),
        )
            .into_response();
    }

    // If scheduled in the future, put in ZSET; else put directly in ready queue
    let now = Utc::now();
    let result = if scheduled_at <= now {
        redis
            .lpush_ready(&ctx.tenant_id.to_string(), &job_json)
            .await
    } else {
        redis
            .zadd_scheduled(
                &ctx.tenant_id.to_string(),
                scheduled_at.timestamp() as f64,
                &job_id.to_string(),
            )
            .await
    };

    if let Err(e) = result {
        error!("Failed to enqueue job: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "Failed to enqueue message"})),
        )
            .into_response();
    }

    info!(
        job_id = %job_id,
        tenant_id = %ctx.tenant_id,
        instance = %ctx.instance_name,
        "CRM message enqueued"
    );

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "job_id": job_id,
            "status": "queued",
            "scheduled_at": scheduled_at,
        })),
    )
        .into_response()
}

// ─── Router ───────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/message/send", post(send_message))
}
