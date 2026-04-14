use axum::{
    extract::State, http::StatusCode, response::IntoResponse, routing::get, Extension, Json, Router,
};
use serde_json::json;
use shared::types::TenantContext;
use std::sync::Arc;
use tracing::error;

use shared::state::AppState;

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
}
