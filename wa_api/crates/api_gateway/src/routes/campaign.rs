use axum::{
    extract::{Path, Query, State},
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
    utils::{idempotency_key, template_hash, validate_phone},
};
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::services::spam_guard;
use shared::state::AppState;

/// Maximum recipients per campaign start request.
const MAX_CAMPAIGN_RECIPIENTS: usize = 10_000;

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
    /// Tenant ID — required for admin route (admin doesn't have TenantContext)
    pub tenant_id: Uuid,
    /// Partner ID — required for admin route
    pub partner_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct CampaignQuery {
    pub tenant_id: Option<Uuid>,
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

// ─── Admin Handlers (require x-admin-key) ────────────────────────────────────

/// POST /admin/campaign/start
/// Admin-only: Start a campaign on behalf of a tenant.
/// Partners cannot directly start campaigns — they request via the Leaex platform.
async fn start_campaign(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StartCampaignRequest>,
) -> impl IntoResponse {
    // Validate tenant exists
    let tenant = match state.db.get_tenant_by_id(&req.tenant_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Tenant not found"})),
            )
                .into_response()
        }
        Err(e) => {
            error!("Tenant lookup error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "DB error"})),
            )
                .into_response();
        }
    };

    if !tenant.campaign_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Campaign feature is not enabled for this tenant"})),
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

    if req.recipients.len() > MAX_CAMPAIGN_RECIPIENTS {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": format!("Too many recipients. Maximum {} per request. Use batching for larger campaigns.", MAX_CAMPAIGN_RECIPIENTS),
                "max": MAX_CAMPAIGN_RECIPIENTS,
                "received": req.recipients.len()
            })),
        )
            .into_response();
    }

    // Backpressure check
    if let Ok(len) = state
        .redis
        .queue_len_ready(&req.tenant_id.to_string())
        .await
    {
        if len > 5000 {
            warn!(tenant_id = %req.tenant_id, queue_len = len, "Backpressure triggered");
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "Queue overloaded",
                    "code": "BACKPRESSURE_TRIGGERED"
                })),
            )
                .into_response();
        }
    }

    let campaign_id = Uuid::new_v4();

    // Save campaign to DB
    let _ = sqlx::query(
        "INSERT INTO wa_campaigns (id, tenant_id, name, template_name, status, total_recipients) \
         VALUES ($1, $2, $3, $4, 'running', $5)",
    )
    .bind(campaign_id)
    .bind(req.tenant_id)
    .bind(&req.name)
    .bind(&req.template_name)
    .bind(req.recipients.len() as i32)
    .execute(state.db.pool())
    .await;

    let payload = MessagePayload::Template {
        template_name: req.template_name.clone(),
        variables: req.variables.clone(),
        body: req.message_body.clone(),
    };
    let t_hash = template_hash(&payload);

    let redis = state.redis.clone();
    let pool_instances = redis.pool_get_available().await.unwrap_or_default();
    if pool_instances.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "No pool numbers available"})),
        )
            .into_response();
    }

    let scheduled_at = req.scheduled_at.unwrap_or_else(Utc::now);
    let mut enqueued = 0usize;
    let mut spam_dropped = 0usize;
    let mut pool_idx = 0;

    for phone in &req.recipients {
        if !validate_phone(phone) {
            continue;
        }

        let week_count = redis.spam_guard_get_week(phone).await.unwrap_or_default();
        if week_count >= spam_guard::WEEKLY_LIMIT {
            spam_dropped += 1;
            continue;
        }

        let instance_name = pool_instances[pool_idx % pool_instances.len()].clone();
        pool_idx += 1;

        let idem_key = idempotency_key(&req.tenant_id, phone, &t_hash, Some(&campaign_id));

        let job = WhatsAppJob {
            job_id: Uuid::new_v4(),
            tenant_id: req.tenant_id,
            partner_id: req.partner_id,
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

        if redis.save_job(&job).await.is_ok() {
            let job_id_str = job.job_id.to_string();
            let res = if scheduled_at <= Utc::now() {
                redis
                    .lpush_ready(&req.tenant_id.to_string(), &job_id_str)
                    .await
            } else {
                redis
                    .zadd_scheduled(
                        &req.tenant_id.to_string(),
                        scheduled_at.timestamp() as f64,
                        &job_id_str,
                    )
                    .await
            };
            if res.is_ok() {
                enqueued += 1;
            }
        }
    }

    let _ = redis
        .campaigns_add_active(&campaign_id.to_string(), Utc::now().timestamp() as f64)
        .await;

    let _ = redis
        .set_string(
            "engine_last_activity",
            &shared::utils::now_unix().to_string(),
        )
        .await;

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "campaign_id": campaign_id,
            "status": "running",
            "total": req.recipients.len(),
            "enqueued": enqueued,
            "spam_dropped": spam_dropped
        })),
    )
        .into_response()
}

/// POST /admin/campaign/pause/:id
async fn pause_campaign(
    State(state): State<Arc<AppState>>,
    Path(campaign_id): Path<Uuid>,
    Query(params): Query<CampaignQuery>,
) -> impl IntoResponse {
    let tenant_id = match params.tenant_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "tenant_id query param required"})),
            )
                .into_response()
        }
    };
    let tenant_id_str = tenant_id.to_string();
    let campaign_id_str = campaign_id.to_string();

    match state
        .redis
        .move_jobs_to_paused(&tenant_id_str, &campaign_id_str)
        .await
    {
        Ok(count) => {
            let _ = sqlx::query(
                "UPDATE wa_campaigns SET status = 'paused' WHERE id = $1 AND tenant_id = $2",
            )
            .bind(campaign_id)
            .bind(tenant_id)
            .execute(state.db.pool())
            .await;

            info!(campaign_id = %campaign_id, moved = count, "Campaign paused");
            (
                StatusCode::OK,
                Json(json!({"status": "paused", "moved_to_paused": count})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /admin/campaign/resume/:id
async fn resume_campaign(
    State(state): State<Arc<AppState>>,
    Path(campaign_id): Path<Uuid>,
    Query(params): Query<CampaignQuery>,
) -> impl IntoResponse {
    let tenant_id = match params.tenant_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "tenant_id query param required"})),
            )
                .into_response()
        }
    };

    // Backpressure check
    if let Ok(len) = state.redis.queue_len_ready(&tenant_id.to_string()).await {
        if len > 5000 {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"error": "Queue overloaded, cannot resume yet"})),
            )
                .into_response();
        }
    }

    let tenant_id_str = tenant_id.to_string();
    let campaign_id_str = campaign_id.to_string();

    match state
        .redis
        .move_jobs_to_ready(&tenant_id_str, &campaign_id_str)
        .await
    {
        Ok(count) => {
            let _ = sqlx::query(
                "UPDATE wa_campaigns SET status = 'running' WHERE id = $1 AND tenant_id = $2",
            )
            .bind(campaign_id)
            .bind(tenant_id)
            .execute(state.db.pool())
            .await;

            info!(campaign_id = %campaign_id, moved = count, "Campaign resumed");
            (
                StatusCode::OK,
                Json(json!({"status": "running", "moved_to_ready": count})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /admin/campaign/cancel/:id
async fn cancel_campaign(
    State(state): State<Arc<AppState>>,
    Path(campaign_id): Path<Uuid>,
    Query(params): Query<CampaignQuery>,
) -> impl IntoResponse {
    let tenant_id = match params.tenant_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "tenant_id query param required"})),
            )
                .into_response()
        }
    };
    let tenant_id_str = tenant_id.to_string();
    let campaign_id_str = campaign_id.to_string();

    match state
        .redis
        .purge_campaign_jobs(&tenant_id_str, &campaign_id_str)
        .await
    {
        Ok(count) => {
            let _ = sqlx::query(
                "UPDATE wa_campaigns SET status = 'cancelled' WHERE id = $1 AND tenant_id = $2",
            )
            .bind(campaign_id)
            .bind(tenant_id)
            .execute(state.db.pool())
            .await;

            let _ = state.redis.campaigns_remove_active(&campaign_id_str).await;

            info!(campaign_id = %campaign_id, purged = count, "Campaign cancelled");
            (
                StatusCode::OK,
                Json(json!({"status": "cancelled", "purged": count})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Partner Handlers (require x-api-key + x-tenant-id) ──────────────────────

/// GET /campaign/status/:id — Partner reads their own campaign status
async fn campaign_status(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<shared::types::TenantContext>,
    Path(campaign_id): Path<Uuid>,
) -> impl IntoResponse {
    #[derive(sqlx::FromRow, Serialize)]
    struct StatusRow {
        name: String,
        status: String,
        total_recipients: i32,
        sent_count: i32,
        delivered_count: i32,
        failed_count: i32,
        deferred_count: i32,
    }

    let row = sqlx::query_as::<_, StatusRow>(
        "SELECT name, status, total_recipients, sent_count, delivered_count, failed_count, deferred_count \
         FROM wa_campaigns WHERE id = $1 AND tenant_id = $2"
    )
    .bind(campaign_id)
    .bind(ctx.tenant_id)
    .fetch_optional(state.db.pool())
    .await;

    match row {
        Ok(Some(r)) => (StatusCode::OK, Json(json!(r))).into_response(),
        _ => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Campaign not found"})),
        )
            .into_response(),
    }
}

/// GET /campaigns — Partner lists their own campaigns
async fn list_campaigns(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<shared::types::TenantContext>,
) -> impl IntoResponse {
    #[derive(sqlx::FromRow, Serialize)]
    struct CampaignRow {
        id: Uuid,
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
        Ok(rows) => (StatusCode::OK, Json(json!({"campaigns": rows}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Routers ──────────────────────────────────────────────────────────────────

/// Partner-facing read-only campaign routes (require x-api-key + x-tenant-id)
pub fn partner_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/campaigns", get(list_campaigns))
        .route("/campaign/status/:id", get(campaign_status))
}

/// Admin-facing campaign write routes (require x-admin-key)
pub fn admin_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin/campaign/start", post(start_campaign))
        .route("/admin/campaign/pause/:id", post(pause_campaign))
        .route("/admin/campaign/resume/:id", post(resume_campaign))
        .route("/admin/campaign/cancel/:id", post(cancel_campaign))
}
