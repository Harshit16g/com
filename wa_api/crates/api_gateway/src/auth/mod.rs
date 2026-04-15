use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use shared::types::{InstanceHealth, PlanTier, TenantContext};
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;

use shared::state::AppState;

/// Constant-time string comparison via SHA-256 hash.
/// Prevents timing attacks by always comparing fixed-length hashes
/// regardless of input length or content.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let hash_a = Sha256::digest(a.as_bytes());
    let hash_b = Sha256::digest(b.as_bytes());
    hash_a == hash_b
}

/// Simple static authentication for the Platform (Leaex v2).
/// Verifies x-api-key against PAUTH_API_KEY from config.
/// Resolves tenant context using x-tenant-id (partner_id).
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    let provided_key = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok());

    let platform_key = &state.config.pauth_api_key;

    if provided_key.is_none()
        || platform_key.is_empty()
        || !constant_time_eq(provided_key.unwrap(), platform_key)
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "Invalid or missing API key"})),
        ));
    }

    let partner_id = req
        .headers()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Uuid::parse_str(v).ok())
        .ok_or((
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "x-tenant-id (partner_id) header missing or invalid"})),
        ))?;

    // Load partner config from local DB
    let tenant = match state.db.get_tenant_by_partner_id(&partner_id).await {
        Ok(Some(t)) => t,
        Ok(None) => return Err((
            StatusCode::FORBIDDEN,
            axum::Json(json!({"error": "Partner not found in this deployment"})),
        )),
        Err(e) => {
            error!("Tenant DB lookup error: {}", e);
            return Err((StatusCode::INTERNAL_SERVER_ERROR, axum::Json(json!({"error": "auth service unavailable"}))));
        }
    };

    // Check instance health from Redis (Real-time gate)
    let health = state.redis.get_instance_health(&tenant.instance_name).await
        .unwrap_or(InstanceHealth::Disconnected);

    match health {
        InstanceHealth::Banned => return Err((StatusCode::FORBIDDEN, axum::Json(json!({"error": "WhatsApp instance is banned"})))),
        InstanceHealth::QrRequired => {
            let path = req.uri().path();
            let is_setup_route = path == "/instance/qr" || path == "/instance/health" || path == "/instance/qr/regenerate";
            if !is_setup_route {
                return Err((StatusCode::CONFLICT, axum::Json(json!({
                    "error": "WhatsApp instance needs re-authentication (QR scan required)",
                    "code": "QR_REQUIRED"
                }))));
            }
        }
        _ => {}
    }

    // Build context
    let ctx = TenantContext {
        tenant_id: tenant.id,   // Local DB primary key
        partner_id,             // Platform's partner ID
        owner_id: Uuid::nil(),  // Detailed owner_id removed for simple deployment
        instance_name: tenant.instance_name.clone(),
        wa_number: tenant.wa_number.unwrap_or_default(),
        plan_tier: PlanTier::Enterprise, // Default to Enterprise for dedicated deployments
        daily_limit: tenant.daily_crm_limit as u32,
        campaign_allowed: tenant.campaign_enabled,
        key_scopes: vec!["all".to_string()], // Simplified for platform access
    };

    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}

/// Admin-only auth middleware — requires x-admin-key header matching ADMIN_API_KEY from config.
/// Uses constant-time comparison to prevent timing attacks.
pub async fn admin_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    let provided_key = req
        .headers()
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok());

    let admin_key = &state.config.admin_api_key;

    if provided_key.is_none()
        || admin_key.is_empty()
        || !constant_time_eq(provided_key.unwrap(), admin_key)
    {
        return Err((
            StatusCode::FORBIDDEN,
            axum::Json(json!({"error": "Invalid or missing admin key"})),
        ));
    }

    Ok(next.run(req).await)
}

/// Webhook auth middleware — verifies x-webhook-secret header from evo API instances.
/// Supports multiple evo instances sharing the same secret.
pub async fn webhook_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    let provided_secret = req
        .headers()
        .get("x-webhook-secret")
        .and_then(|v| v.to_str().ok());

    let expected_secret = &state.config.webhook_shared_secret;

    if provided_secret.is_none()
        || expected_secret.is_empty()
        || !constant_time_eq(provided_secret.unwrap(), expected_secret)
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "Invalid or missing webhook secret"})),
        ));
    }

    Ok(next.run(req).await)
}
