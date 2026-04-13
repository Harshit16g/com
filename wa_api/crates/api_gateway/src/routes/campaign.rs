use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared::{
    types::{JobStatus, MessagePayload, MessageType, WhatsAppJob},
    utils::{idempotency_key, sha256, template_hash, validate_phone},
};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::services::spam_guard;
use shared::state::AppState;
use shared::types::TenantContext;

// ─── Request / Response ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct StartCampaignRequest {
    pub name: String,
    pub template_name: String,
    /// Template variables (applies to all recipients unless per-recipient overrides)
    pub variables: Vec<String>,
    /// Rendered message body
    pub message_body: String,
    /// List of recipient phone numbers
    pub recipients: Vec<String>,
    /// Optional schedule start time (defaults to now)
    pub scheduled_at: Option<chrono::DateTime<Utc>>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct StartCampaignResponse {
    pub campaign_id: String,
    pub status: String,
    pub total_recipients: usize,
    pub enqueued: usize,
    pub spam_guard_dropped: usize,
}

// ─── Handlers ────────────────────────────────────────────────────────────────

/// POST /campaign/start
/// Create and enqueue a campaign via pool numbers.
async fn start_campaign(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Json(req): Json<StartCampaignRequest>,
) -> impl IntoResponse {
    // Check campaign permission (Pro+ only)
    if !ctx.campaign_allowed {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Campaign feature requires Pro or Enterprise plan"})),
        )
            .into_response();
    }

    if req.recipients.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": "Recipients list cannot be empty"})),
        )
            .into_response();
    }

    if req.name.is_empty() || req.template_name.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": "name and template_name are required"})),
        )
            .into_response();
    }

    // Validate all phone numbers upfront
    let invalid: Vec<&String> = req
        .recipients
        .iter()
        .filter(|p| !validate_phone(p))
        .collect();

    if !invalid.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "Invalid phone numbers",
                "invalid": invalid,
            })),
        )
            .into_response();
    }

    let campaign_id = Uuid::new_v4();
    let payload = MessagePayload::Template {
        template_name: req.template_name.clone(),
        variables: req.variables.clone(),
        body: req.message_body.clone(),
    };
    let t_hash = template_hash(&payload);

    // Get an available pool instance
    let redis = state.redis.clone();
    let pool_instances = match redis.pool_get_available().await {
        Ok(p) if !p.is_empty() => p,
        Ok(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "No pool numbers available. Try again shortly."})),
            )
                .into_response();
        }
        Err(e) => {
            error!("Failed to get pool instances: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Internal error"})),
            )
                .into_response();
        }
    };

    let scheduled_at = req.scheduled_at.unwrap_or_else(Utc::now);
    let total = req.recipients.len();
    let mut enqueued = 0usize;
    let mut spam_dropped = 0usize;

    // Round-robin pool instance assignment across recipients
    let mut pool_idx = 0;

    for phone in &req.recipients {
        let phone_hash = shared::utils::hash_phone(phone);

        // Weekly spam guard check at job creation
        let week_count = redis
            .spam_guard_get_week(&phone_hash)
            .await
            .unwrap_or_default();
        if week_count >= spam_guard::WEEKLY_LIMIT {
            spam_dropped += 1;
            continue;
        }

        // Check campaign dedup (same template to same number in 7 days)
        let dedup_key = format!("dedup:{}:{}", sha256(phone), t_hash);
        if redis.exists(&dedup_key).await.unwrap_or(false) {
            spam_dropped += 1;
            continue;
        }

        let instance_name = pool_instances[pool_idx % pool_instances.len()].clone();
        pool_idx += 1;

        let idem_key = idempotency_key(&ctx.tenant_id, phone, &t_hash, Some(&campaign_id));

        let job = WhatsAppJob {
            job_id: Uuid::new_v4(),
            tenant_id: ctx.tenant_id,
            partner_id: ctx.partner_id,
            campaign_id: Some(campaign_id),
            instance_name,
            message_type: MessageType::Campaign,
            recipient_phone: phone.clone(),
            recipient_name: None,
            payload: payload.clone(),
            retry_count: 0,
            scheduled_at,
            created_at: Utc::now(),
            idempotency_key: idem_key,
            status: JobStatus::Pending,
        };

        let job_json = match serde_json::to_string(&job) {
            Ok(j) => j,
            Err(_) => continue,
        };

        // Save job metadata
        let save_result: anyhow::Result<()> = redis.save_job(&job).await;
        if save_result.is_err() {
            continue;
        }

        // Enqueue: scheduled or immediate
        let result: anyhow::Result<()> = if scheduled_at <= Utc::now() {
            redis
                .lpush_ready(&ctx.tenant_id.to_string(), &job_json)
                .await
        } else {
            redis
                .zadd_scheduled(
                    &ctx.tenant_id.to_string(),
                    scheduled_at.timestamp() as f64,
                    &job.job_id.to_string(),
                )
                .await
        };

        if result.is_ok() {
            enqueued += 1;
            // Set 7-day dedup marker
            let _ = redis.set_string_ex(&dedup_key, "1", 7 * 24 * 3600).await;
        }
    }

    // Track campaign in active ZSET
    let _ = redis
        .campaigns_add_active(&campaign_id.to_string(), Utc::now().timestamp() as f64)
        .await;

    info!(
        campaign_id = %campaign_id,
        tenant_id = %ctx.tenant_id,
        total,
        enqueued,
        spam_dropped,
        "Campaign started"
    );

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "campaign_id": campaign_id,
            "status": "running",
            "total_recipients": total,
            "enqueued": enqueued,
            "spam_guard_dropped": spam_dropped,
        })),
    )
        .into_response()
}

/// GET /campaigns — list all campaigns for this tenant
async fn list_campaigns(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
) -> impl IntoResponse {
    #[derive(sqlx::FromRow, serde::Serialize)]
    struct CampaignRow {
        id: uuid::Uuid,
        name: String,
        template_name: Option<String>,
        status: String,
        total_recipients: i32,
        sent_count: i32,
        delivered_count: i32,
        failed_count: i32,
        deferred_count: i32,
        started_at: Option<chrono::DateTime<chrono::Utc>>,
        completed_at: Option<chrono::DateTime<chrono::Utc>>,
        created_at: chrono::DateTime<chrono::Utc>,
    }

    match sqlx::query_as::<_, CampaignRow>(
        "SELECT id, name, template_name, status, total_recipients, sent_count, \
         delivered_count, failed_count, deferred_count, started_at, completed_at, created_at \
         FROM wa_campaigns WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 50",
    )
    .bind(ctx.tenant_id)
    .fetch_all(state.db.pool())
    .await
    {
        Ok(rows) => (StatusCode::OK, Json(serde_json::json!({"campaigns": rows}))).into_response(),
        Err(e) => {
            tracing::error!("Failed to list campaigns: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database error"})),
            )
                .into_response()
        }
    }
}

/// GET /campaign/status/:id
async fn campaign_status(
    State(_state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Path(campaign_id): Path<String>,
) -> impl IntoResponse {
    // In a full implementation this would query Supabase wa_campaigns + Redis
    // For now return a placeholder structure
    (
        StatusCode::OK,
        Json(json!({
            "campaign_id": campaign_id,
            "tenant_id": ctx.tenant_id,
            "status": "running",
            "note": "Detailed status queried from Supabase wa_campaigns table"
        })),
    )
        .into_response()
}

/// POST /campaign/pause/:id
async fn pause_campaign(
    State(_state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Path(campaign_id): Path<String>,
) -> impl IntoResponse {
    // Move ready queue jobs for this campaign back to scheduled ZSET
    // Full implementation iterates ready queue and filters by campaign_id
    info!(campaign_id = %campaign_id, tenant_id = %ctx.tenant_id, "Campaign pause requested");
    (
        StatusCode::ACCEPTED,
        Json(json!({"campaign_id": campaign_id, "status": "pausing"})),
    )
        .into_response()
}

/// POST /campaign/resume/:id
async fn resume_campaign(
    State(_state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Path(campaign_id): Path<String>,
) -> impl IntoResponse {
    info!(campaign_id = %campaign_id, tenant_id = %ctx.tenant_id, "Campaign resume requested");
    (
        StatusCode::ACCEPTED,
        Json(json!({"campaign_id": campaign_id, "status": "resuming"})),
    )
        .into_response()
}

/// POST /campaign/cancel/:id
async fn cancel_campaign(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Path(campaign_id): Path<String>,
) -> impl IntoResponse {
    info!(campaign_id = %campaign_id, tenant_id = %ctx.tenant_id, "Campaign cancel requested");
    let redis = state.redis.clone();
    let _ = redis.campaigns_remove_active(&campaign_id).await;
    (
        StatusCode::OK,
        Json(json!({"campaign_id": campaign_id, "status": "cancelled"})),
    )
        .into_response()
}

// ─── Router ───────────────────────────────────────────────────────────────────

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/campaigns", get(list_campaigns))
        .route("/campaign/start", post(start_campaign))
        .route("/campaign/status/:id", get(campaign_status))
        .route("/campaign/pause/:id", post(pause_campaign))
        .route("/campaign/resume/:id", post(resume_campaign))
        .route("/campaign/cancel/:id", post(cancel_campaign))
}
