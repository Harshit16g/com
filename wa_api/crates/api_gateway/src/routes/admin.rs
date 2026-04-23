use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use pool_manager::{get_all_pool_instances, register_pool_number};
use shared::state::AppState;

// ─── GET /admin/instances ─────────────────────────────────────────────────────

#[derive(sqlx::FromRow, Serialize)]
struct TenantListRow {
    id: Uuid,
    partner_id: Option<Uuid>,
    instance_name: String,
    wa_number: Option<String>,
    instance_status: String,
    daily_crm_limit: i32,
    campaign_enabled: bool,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// GET /admin/instances — full list of all provisioned tenants with live status from Redis.
async fn admin_list_instances(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rows = sqlx::query_as::<_, TenantListRow>(
        "SELECT id, partner_id, instance_name, wa_number, \
         instance_status, daily_crm_limit, campaign_enabled, created_at \
         FROM tenants ORDER BY created_at DESC",
    )
    .fetch_all(state.db.pool())
    .await;

    match rows {
        Ok(tenants) => {
            // Enrich with cached Redis health status
            let redis = state.redis.clone();
            let mut enriched = Vec::with_capacity(tenants.len());
            for t in tenants {
                let live_status = redis
                    .get_instance_health(&t.instance_name)
                    .await
                    .map(|h| h.to_string())
                    .unwrap_or_else(|_| t.instance_status.clone());

                enriched.push(json!({
                    "id": t.id,
                    "partner_id": t.partner_id,
                    "instance_name": t.instance_name,
                    "wa_number": t.wa_number,
                    "db_status": t.instance_status,
                    "live_status": live_status,
                    "daily_crm_limit": t.daily_crm_limit,
                    "campaign_enabled": t.campaign_enabled,
                    "created_at": t.created_at,
                }));
            }
            (StatusCode::OK, Json(json!({ "instances": enriched }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("DB error: {}", e) })),
        )
            .into_response(),
    }
}

// ─── Pool Management ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct AddPoolNumberRequest {
    /// Suffix for the pool instance, e.g. "leaex_01". "pool_" prefix added automatically.
    suffix: String,
}

/// POST /admin/pool/add
/// Registers and provisions a new WhatsApp number into the campaign pool.
async fn admin_add_pool_number(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddPoolNumberRequest>,
) -> impl IntoResponse {
    let instance_name = format!("pool_{}", req.suffix);

    info!(instance = %instance_name, "Registering new pool instance");

    // 1. Provision in Evo
    match state.evo.create_instance(&instance_name).await {
        Ok(_) => {
            // 2. Register in pool_manager (persists to Redis)
            if let Err(e) = register_pool_number(&state.redis, &instance_name).await {
                error!(instance = %instance_name, "Failed to register pool number in Redis: {}", e);
                let err_msg = e.to_string();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": err_msg })),
                )
                    .into_response();
            }

            (
                StatusCode::CREATED,
                Json(json!({
                    "status": "SUCCESS",
                    "instance_name": instance_name,
                    "message": "Pool instance provisioned. Scan QR to activate."
                })),
            )
                .into_response()
        }
        Err(e) => {
            error!(instance = %instance_name, "Failed to provision pool instance in Evo: {}", e);
            let err_msg = e.to_string();
            (StatusCode::BAD_GATEWAY, Json(json!({ "error": err_msg }))).into_response()
        }
    }
}

/// GET /admin/pool
/// Lists all numbers in the campaign pool with their live status and stats.
async fn admin_list_pool(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let pool_members = match get_all_pool_instances(&state.redis).await {
        Ok(m) => m,
        Err(e) => {
            let err_msg = e.to_string();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": err_msg})),
            )
                .into_response();
        }
    };

    let mut results = Vec::new();
    for p in pool_members {
        let health = state
            .redis
            .get_instance_health(&p.name)
            .await
            .map(|h| h.to_string())
            .unwrap_or_else(|_| "UNKNOWN".into());

        results.push(json!({
            "instance_name": p.name,
            "state": p.state,
            "status": health,
            "daily_sent": p.daily_sent,
            "daily_limit": p.daily_limit,
            "warmup_day": p.warmup_day
        }));
    }

    (StatusCode::OK, Json(json!({ "pool": results }))).into_response()
}

// ─── Interactions ────────────────────────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct AdminInteractionsQuery {
    pub tenant_id: Option<String>,
    pub page: Option<u32>,
    pub limit: Option<u32>,
    pub status: Option<String>,
    pub message_type: Option<String>,
}

/// GET /admin/interactions
async fn admin_interactions(
    State(_state): State<Arc<AppState>>,
    Query(params): Query<AdminInteractionsQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    let limit = params.limit.unwrap_or(100).min(500);

    (
        StatusCode::OK,
        Json(json!({
            "page": page,
            "limit": limit,
            "tenant_filter": params.tenant_id,
            "note": "Admin cross-tenant view."
        })),
    )
        .into_response()
}

// ─── Provisioning ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CreateInstanceRequest {
    /// Must match the partner's organisation slug, e.g. "leaex_partner_01"
    instance_name: String,
    /// Optional: link to Leaex partner_id for cross-system joins
    partner_id: Uuid,
    /// Daily CRM message quota for this partner
    daily_crm_limit: Option<i32>,
    /// Whether campaign sends are allowed (Pro+ plan)
    campaign_enabled: Option<bool>,
}

/// Validate instance_name: only alphanumeric + underscore to prevent path traversal in evo API URLs.
fn is_valid_instance_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[derive(Debug, Deserialize)]
struct UpdateTenantLimitsRequest {
    tenant_id: Uuid,
    /// New daily CRM message limit (if provided)
    daily_crm_limit: Option<i32>,
    /// Enable/disable campaign feature (if provided)
    campaign_enabled: Option<bool>,
}

/// POST /admin/tenant/update-limits — Admin-configurable tenant limits
async fn admin_update_tenant_limits(
    State(state): State<Arc<AppState>>,
    Json(req): Json<UpdateTenantLimitsRequest>,
) -> impl IntoResponse {
    let mut updates = Vec::new();
    let mut bind_idx = 2;

    if req.daily_crm_limit.is_some() {
        updates.push(format!("daily_crm_limit = ${}", bind_idx));
        bind_idx += 1;
    }
    if req.campaign_enabled.is_some() {
        updates.push(format!("campaign_enabled = ${}", bind_idx));
    }

    if updates.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "No fields to update"})),
        )
            .into_response();
    }

    let sql = format!(
        "UPDATE tenants SET {}, updated_at = NOW() WHERE id = $1",
        updates.join(", ")
    );

    let mut query = sqlx::query(&sql).bind(req.tenant_id);
    if let Some(limit) = req.daily_crm_limit {
        query = query.bind(limit);
    }
    if let Some(enabled) = req.campaign_enabled {
        query = query.bind(enabled);
    }

    match query.execute(state.db.pool()).await {
        Ok(result) if result.rows_affected() > 0 => {
            info!(tenant_id = %req.tenant_id, "Tenant limits updated");
            (
                StatusCode::OK,
                Json(json!({
                    "status": "updated",
                    "tenant_id": req.tenant_id,
                    "daily_crm_limit": req.daily_crm_limit,
                    "campaign_enabled": req.campaign_enabled,
                })),
            )
                .into_response()
        }
        Ok(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Tenant not found"})),
        )
            .into_response(),
        Err(e) => {
            error!("Failed to update tenant limits: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database error"})),
            )
                .into_response()
        }
    }
}

async fn admin_create_instance(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateInstanceRequest>,
) -> impl IntoResponse {
    // Validate instance name (prevent path traversal in evo API URLs)
    if !is_valid_instance_name(&req.instance_name) {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "Invalid instance_name. Only alphanumeric characters and underscores allowed (max 64 chars).",
                "field": "instance_name"
            })),
        ).into_response();
    }

    // 1. Step 1: Resolve metadata from Platform DB (if available)
    let mut business_name = "our business".to_string();
    let mut partner_name = "manager".to_string();

    if let Some(platform_db) = &state.platform_db {
        match state.db.fetch_platform_partner_info(platform_db, &req.partner_id).await {
            Ok((biz, part)) => {
                business_name = biz;
                partner_name = part;
                info!(instance_name = %req.instance_name, business_name = %business_name, "Fetched partner metadata from platform");
            }
            Err(e) => {
                tracing::warn!("Failed to fetch partner metadata for {}: {}. Using defaults.", req.partner_id, e);
            }
        }
    }

    // 2. Step 2: Database Pre-flight (Local Validation)
    let tenant_id = Uuid::new_v4();
    let insert_result = sqlx::query(
        "INSERT INTO tenants \
         (id, partner_id, instance_name, instance_status, daily_crm_limit, campaign_enabled, business_name, partner_name) \
         VALUES ($1, $2, $3, 'disconnected', $4, $5, $6, $7)",
    )
    .bind(tenant_id)
    .bind(req.partner_id)
    .bind(&req.instance_name)
    .bind(req.daily_crm_limit.unwrap_or(200))
    .bind(req.campaign_enabled.unwrap_or(false))
    .bind(business_name)
    .bind(partner_name)
    .execute(state.db.pool())
    .await;

    if let Err(e) = insert_result {
        let err_msg = e.to_string();
        if err_msg.contains("unique constraint") || err_msg.contains("already exists") {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "An instance with this name already exists",
                    "instance_name": req.instance_name,
                })),
            )
                .into_response();
        }
        tracing::error!("DB pre-flight insert failed: {}", e);
        let err_msg = e.to_string();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Database error: {}", err_msg) })),
        )
            .into_response();
    }

    info!(instance_name = %req.instance_name, "Instance reserved in DB (Phase 1)");

    // 2. Step 2: External Provisioning in evo API
    match state.evo.create_instance(&req.instance_name).await {
        Ok(evo_resp) => {
            info!(instance_name = %req.instance_name, "Instance created in evo API (Phase 2)");

            // 3. Step 3: Confirmation - Update status to 'qr_required'
            let update = sqlx::query(
                "UPDATE tenants SET instance_status = 'qr_required', updated_at = NOW() WHERE id = $1"
            )
            .bind(tenant_id)
            .execute(state.db.pool())
            .await;

            match update {
                Ok(_) => (
                    StatusCode::CREATED,
                    Json(json!({
                        "tenant_id": tenant_id,
                        "partner_id": req.partner_id,
                        "instance_name": req.instance_name,
                        "status": "qr_required",
                        "daily_crm_limit": req.daily_crm_limit.unwrap_or(200),
                        "campaign_enabled": req.campaign_enabled.unwrap_or(false),
                        "evo_response": evo_resp,
                        "next_step": "Partner should scan QR via /instance/qr",
                    })),
                )
                    .into_response(),
                Err(e) => {
                    tracing::error!("CRITICAL: Phase 3 update failed: {}", e);
                    let err_msg = e.to_string();
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": err_msg })),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            // 4. Compensation: Rollback the DB reservation
            tracing::error!(
                "evo create_instance failed (Phase 2): {}. Rolling back DB reservation.",
                e
            );
            let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
                .bind(tenant_id)
                .execute(state.db.pool())
                .await;

            let err_msg = e.to_string();
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("evo API error: {}", err_msg) })),
            )
                .into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct AdminForceQrRequest {
    tenant_id: Uuid,
    instance_name: String,
    reason: String,
}

/// POST /admin/instance/force-qr
async fn admin_force_qr(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AdminForceQrRequest>,
) -> impl IntoResponse {
    // 1. Validate tenant exists and matches instance_name
    let _tenant = match state
        .db
        .get_tenant_by_instance_name(&req.instance_name)
        .await
    {
        Ok(Some(t)) if t.id == req.tenant_id => t,
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Tenant not found or instance_name mismatch"})),
            )
                .into_response()
        }
    };

    if req.reason.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "reason is required for admin force-qr"})),
        )
            .into_response();
    }

    // 2. Audit Log
    info!(
        admin_action = "force_qr",
        tenant_id = %req.tenant_id,
        instance = %req.instance_name,
        reason = %req.reason,
        "Admin initiated forced QR regeneration"
    );

    let _ = state
        .db
        .log_instance_health_event(
            &req.instance_name,
            Some(&req.tenant_id),
            false,
            "admin_force_qr",
            "N/A",
            "QR_REQUIRED",
            Some(json!({ "reason": req.reason, "triggered_by": "admin_api" })),
        )
        .await;

    // 3. Call evo API
    match state.evo.get_instance_qr(&req.instance_name).await {
        Ok((base64, code)) => (
            StatusCode::OK,
            Json(json!({ "base64": base64, "code": code, "message": "Forced QR generated successfully" })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Admin force-QR error for {}: {}", req.instance_name, e);
            let err_msg = e.to_string();
            (StatusCode::BAD_GATEWAY, Json(json!({ "error": err_msg }))).into_response()
        }
    }
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin/interactions", get(admin_interactions))
        .route("/admin/instances", get(admin_list_instances))
        .route("/admin/instance/create", post(admin_create_instance))
        .route("/admin/instance/force-qr", post(admin_force_qr))
        .route(
            "/admin/tenant/update-limits",
            post(admin_update_tenant_limits),
        )
        .route("/admin/instance/:id", delete(admin_delete_instance))
        .route("/admin/pool", get(admin_list_pool))
        .route("/admin/pool/add", post(admin_add_pool_number))
}

/// DELETE /admin/instance/:id
async fn admin_delete_instance(
    State(state): State<Arc<AppState>>,
    Path(tenant_id): Path<Uuid>,
) -> impl IntoResponse {
    // 1. Get instance name before deleting from DB
    let tenant = match state.db.get_tenant(&tenant_id).await {
        Ok(Some(t)) => t,
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Tenant not found"})),
            )
                .into_response()
        }
    };

    // 2. Delete from Evolution API (WhatsApp Engine)
    if let Err(e) = state.evo.delete_instance(&tenant.instance_name).await {
        tracing::error!("Failed to delete instance from Evo: {}", e);
        // We continue anyway to ensure DB is cleaned up, but log the error
    }

    // 3. Delete from Rust Database
    if let Err(e) = state.db.delete_tenant(&tenant_id).await {
        tracing::error!("Failed to delete tenant from DB: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }

    // 4. Clear Redis health status
    let redis_key = format!("instance_health:{}", tenant.instance_name);
    if let Err(e) = state.redis.del(&redis_key).await {
        tracing::error!("Failed to delete instance health from Redis: {}", e);
        return (
            StatusCode::OK,
            Json(json!({"status": "deleted", "warning": "Redis cleanup failed"})),
        )
            .into_response();
    }

    (StatusCode::OK, Json(json!({"status": "deleted"}))).into_response()
}
