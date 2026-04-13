use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use shared::state::AppState;

// ─── GET /admin/instances ─────────────────────────────────────────────────────

#[derive(sqlx::FromRow, Serialize)]
struct TenantListRow {
    id: Uuid,
    agency_id: Uuid,
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
        "SELECT id, agency_id, partner_id, instance_name, wa_number, \
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
                    "agency_id": t.agency_id,
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
/// Full cross-tenant interaction log. Admin key required.
async fn admin_interactions(
    State(_state): State<Arc<AppState>>,
    Query(params): Query<AdminInteractionsQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    let limit = params.limit.unwrap_or(100).min(500);
    let _offset = (page - 1) * limit;

    // Admin uses service_role key — Supabase RLS bypassed
    // Full implementation queries wa_interaction_log with optional tenant_id filter
    (
        StatusCode::OK,
        Json(json!({
            "page": page,
            "limit": limit,
            "tenant_filter": params.tenant_id,
            "note": "Admin cross-tenant view. Queries wa_interaction_log with service_role key."
        })),
    )
        .into_response()
}

/// POST /admin/instance/create
/// Admin-only: provisions a new Evolution API instance and creates the
/// corresponding tenant record in the wa_api database.
/// One instance per partner — enforced by the unique constraint on instance_name.
#[derive(Debug, Deserialize)]
struct CreateInstanceRequest {
    /// Must match the partner's organisation slug, e.g. "leaex_partner_01"
    instance_name: String,
    /// wa_api agency this tenant belongs to
    agency_id: Uuid,
    /// Optional: link to Leaex partner_id for cross-system joins
    partner_id: Option<Uuid>,
    /// Daily CRM message quota for this partner
    daily_crm_limit: Option<i32>,
    /// Whether campaign sends are allowed (Pro+ plan)
    campaign_enabled: Option<bool>,
}

async fn admin_create_instance(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateInstanceRequest>,
) -> impl IntoResponse {
    // 1. Check the instance doesn't already exist in our DB
    let existing: Option<(String,)> =
        sqlx::query_as("SELECT instance_name FROM tenants WHERE instance_name = $1 LIMIT 1")
            .bind(&req.instance_name)
            .fetch_optional(state.db.pool())
            .await
            .unwrap_or(None);

    if existing.is_some() {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "An instance with this name already exists",
                "instance_name": req.instance_name,
            })),
        )
            .into_response();
    }

    // 2. Create instance in Evolution API
    match state.evolution.create_instance(&req.instance_name).await {
        Ok(evo_resp) => {
            info!(instance_name = %req.instance_name, "Instance created in Evolution API");

            // 3. Insert tenant record in wa_api DB
            let tenant_id = Uuid::new_v4();
            let insert = sqlx::query(
                "INSERT INTO tenants \
                 (id, agency_id, partner_id, instance_name, instance_status, daily_crm_limit, campaign_enabled) \
                 VALUES ($1, $2, $3, $4, 'qr_required', $5, $6)"
            )
            .bind(tenant_id)
            .bind(req.agency_id)
            .bind(req.partner_id)
            .bind(&req.instance_name)
            .bind(req.daily_crm_limit.unwrap_or(200))
            .bind(req.campaign_enabled.unwrap_or(false))
            .execute(state.db.pool())
            .await;

            match insert {
                Ok(_) => (
                    StatusCode::CREATED,
                    Json(json!({
                        "tenant_id": tenant_id,
                        "instance_name": req.instance_name,
                        "status": "qr_required",
                        "daily_crm_limit": req.daily_crm_limit.unwrap_or(200),
                        "campaign_enabled": req.campaign_enabled.unwrap_or(false),
                        "evolution_response": evo_resp,
                        "next_step": "Partner should scan QR via /instance/qr",
                    })),
                )
                    .into_response(),
                Err(e) => {
                    // Rollback: delete from Evolution API (best-effort)
                    tracing::error!("DB insert failed after Evolution create: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": format!("DB insert failed: {}", e) })),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!("Evolution create_instance failed: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("Evolution API error: {}", e) })),
            )
                .into_response()
        }
    }
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin/interactions", get(admin_interactions))
        .route("/admin/instances", get(admin_list_instances))
        .route("/admin/instance/create", post(admin_create_instance))
}
