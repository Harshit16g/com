use axum::{
    extract::State, http::StatusCode, response::IntoResponse, routing::{get, post}, Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use shared::types::{InstanceHealth, TenantContext};
use std::sync::Arc;
use tracing::{error, info, warn};

use shared::state::AppState;

#[derive(Debug, Deserialize)]
pub struct QrRegenerateRequest {
    /// Explicit confirmation of instance name to prevent accidental disconnection.
    pub confirm_instance: String,
}

/// POST /instance/qr/regenerate
/// Strictly owned QR regeneration. Only allowed for the owning tenant
/// when the instance is in a disconnected or QR_REQUIRED state.
async fn instance_qr_regenerate(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Json(req): Json<QrRegenerateRequest>,
) -> impl IntoResponse {
    // 1. Ownership Check: confirm_instance must match the resolved tenant's instance.
    if req.confirm_instance != ctx.instance_name {
        warn!(
            tenant_id = %ctx.tenant_id,
            claimed = %req.confirm_instance,
            actual = %ctx.instance_name,
            "QR regeneration ownership mismatch blocked"
        );
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Instance name does not match tenant ownership"})),
        )
            .into_response();
    }

    // 2. State Check: only allow if not ACTIVE.
    let redis = state.redis.clone();
    let health = redis.get_instance_health(&ctx.instance_name).await
        .unwrap_or(InstanceHealth::Disconnected);

    if health == InstanceHealth::Active {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "Instance is already ACTIVE. QR regeneration would cause unnecessary disconnection.",
                "status": health.as_str()
            })),
        )
            .into_response();
    }

    // 3. Rate Limit: max 3 per hour per tenant.
    let hour_bucket = chrono::Utc::now().format("%Y%m%d%H").to_string();
    let regen_key = format!("qr_regen_count:{}:{}", ctx.tenant_id, hour_bucket);
    let count = redis.incr_ex(&regen_key, 3600).await.unwrap_or(0);
    if count > 3 {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "QR regeneration rate limit exceeded: 3 per hour"})),
        )
            .into_response();
    }

    info!(instance = %ctx.instance_name, attempt = count, "Initiating QR regeneration");

    // 4. Call evo API
    match state.evo.get_instance_qr(&ctx.instance_name).await {
        Ok((base64, code)) => (
            StatusCode::OK,
            Json(json!({ "base64": base64, "code": code })),
        )
            .into_response(),
        Err(e) => {
            error!("QR regeneration error for {}: {}", ctx.instance_name, e);
            (StatusCode::BAD_GATEWAY, Json(json!({ "error": e.to_string() }))).into_response()
        }
    }
}

/// GET /instance/qr
/// Returns QR base64 for the tenant's instance.
/// Returns 404 if the instance hasn't been provisioned by admin yet.
/// Returns 409 if the instance is already connected.
async fn instance_qr(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
) -> impl IntoResponse {
    match state.evo.get_instance_qr(&ctx.instance_name).await {
        Ok((base64, code)) => (
            StatusCode::OK,
            Json(json!({ "base64": base64, "code": code })),
        )
            .into_response(),
        Err(e) => {
            let msg = e.to_string();
            if msg.starts_with("instance_not_found") {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({
                        "error": "Instance not provisioned. Contact admin to activate WhatsApp for your account.",
                        "instance_name": ctx.instance_name,
                    })),
                )
                    .into_response();
            }
            if msg.contains("already be connected") || msg.contains("No base64") {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({ "error": "Instance is already connected" })),
                )
                    .into_response();
            }
            error!("QR fetch error for {}: {}", ctx.instance_name, msg);
            (StatusCode::BAD_GATEWAY, Json(json!({ "error": msg }))).into_response()
        }
    }
}

/// GET /instance/health
/// Check partner's WA instance connection status.
async fn instance_health(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
) -> impl IntoResponse {
    let redis = state.redis.clone();

    // Check Redis health state first (fast path)
    let health = match redis.get_instance_health(&ctx.instance_name).await {
        Ok(h) => h,
        Err(e) => {
            error!("Redis health check error: {}", e);
            shared::types::InstanceHealth::Disconnected
        }
    };

    // If ACTIVE from cache, also verify with evo API
    let evo_state = if health == shared::types::InstanceHealth::Active {
        match state.evo.get_instance_status(&ctx.instance_name).await {
            Ok(s) => s,
            Err(_) => "UNKNOWN".to_string(),
        }
    } else {
        health.to_string()
    };

    (
        StatusCode::OK,
        Json(json!({
            "instance_name": ctx.instance_name,
            "wa_number": ctx.wa_number,
            "status": evo_state,
            "cached_status": health.as_str(),
        })),
    )
        .into_response()
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/instance/health", get(instance_health))
        .route("/instance/qr", get(instance_qr))
        .route("/instance/qr/regenerate", post(instance_qr_regenerate))
}
