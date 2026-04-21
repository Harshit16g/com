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
use tracing::{error, warn};
use uuid::Uuid;

use shared::state::AppState;
use shared::jwt::{verify_supabase_jwt, verify_admin_jwt};

/// Constant-time string comparison via SHA-256 hash.
/// Prevents timing attacks by always comparing fixed-length hashes
/// regardless of input length or content.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let hash_a = Sha256::digest(a.as_bytes());
    let hash_b = Sha256::digest(b.as_bytes());
    hash_a == hash_b
}

/// Supabase Authentication Middleware.
/// Verifies Bearer token from Supabase and extracts org_id from app_metadata.
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    let auth_header = req.headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));

    if auth_header.is_none() {
        // Fallback for legacy calls during transition (optional, but safer)
        let legacy_key = req.headers().get("x-api-key").and_then(|v| v.to_str().ok());
        if let Some(key) = legacy_key {
            if constant_time_eq(key, &state.config.pauth_api_key) {
                warn!("Legacy x-api-key used. Please migrate to Bearer token.");
                return legacy_auth_middleware(state, req, next).await;
            }
        }
        
        return Err((
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "Missing or malformed Authorization header"})),
        ));
    }

    let token = auth_header.unwrap();
    let claims = match verify_supabase_jwt(token, &state.config.supabase_jwt_secret) {
        Ok(c) => c,
        Err(e) => {
            warn!("JWT verification failed: {}", e);
            return Err((
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({"error": "Invalid token"})),
            ));
        }
    };

    let partner_id = claims.app_metadata.org_id.ok_or((
        StatusCode::FORBIDDEN,
        axum::Json(json!({"error": "No org_id found in token metadata"})),
    ))?;

    // Load partner config from local DB
    let tenant = match state.db.get_tenant_by_partner_id(&partner_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return Err((
                StatusCode::FORBIDDEN,
                axum::Json(json!({"error": "Partner organization not found or not initialized"})),
            ))
        }
        Err(e) => {
            error!("Tenant DB lookup error: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "auth service unavailable"})),
            ));
        }
    };

    // Check instance health from Redis
    let health = state
        .redis
        .get_instance_health(&tenant.instance_name)
        .await
        .unwrap_or(InstanceHealth::Disconnected);

    match health {
        InstanceHealth::Banned => {
            return Err((
                StatusCode::FORBIDDEN,
                axum::Json(json!({"error": "WhatsApp instance is banned"})),
            ))
        }
        InstanceHealth::QrRequired => {
            let path = req.uri().path();
            let is_setup_route = path == "/instance/qr"
                || path == "/instance/health"
                || path == "/instance/qr/regenerate";
            if !is_setup_route {
                return Err((
                    StatusCode::CONFLICT,
                    axum::Json(json!({
                        "error": "WhatsApp instance needs re-authentication (QR scan required)",
                        "code": "QR_REQUIRED"
                    })),
                ));
            }
        }
        _ => {}
    }

    let ctx = TenantContext {
        tenant_id: tenant.id,
        partner_id,
        owner_id: claims.sub,
        instance_name: tenant.instance_name.clone(),
        wa_number: tenant.wa_number.unwrap_or_default(),
        plan_tier: PlanTier::Enterprise,
        daily_limit: tenant.daily_crm_limit as u32,
        campaign_allowed: tenant.campaign_enabled,
        key_scopes: vec!["all".to_string()],
    };

    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}

/// Admin Authentication Middleware.
/// Supports both Supabase JWT (for dashboard users) and static secret (for machine calls).
pub async fn admin_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    // 1. Try static x-admin-key (Machine-to-Machine)
    if let Some(key) = req.headers().get("x-admin-key").and_then(|v| v.to_str().ok()) {
        if !state.config.admin_api_key.is_empty() && constant_time_eq(key, &state.config.admin_api_key) {
            return Ok(next.run(req).await);
        }
    }

    // 2. Try Supabase JWT (Dashboard Admins)
    if let Some(auth_header) = req.headers().get("Authorization").and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth_header.strip_prefix("Bearer ") {
            if let Ok(claims) = verify_supabase_jwt(token, &state.config.supabase_jwt_secret) {
                // Check role in app_metadata or user_metadata
                let role = claims.app_metadata.role.as_deref()
                    .or_else(|| claims.user_metadata.as_ref().and_then(|m| m.role.as_deref()))
                    .unwrap_or("");
                
                if role == "admin" || role == "core_admin" {
                    return Ok(next.run(req).await);
                }
            }
        }
    }

    // 3. Try internal Admin JWT (Optional fallback for specific wa_api scripts)
    if let Some(auth_header) = req.headers().get("x-admin-token").and_then(|v| v.to_str().ok()) {
        if verify_admin_jwt(auth_header, &state.config.admin_jwt_secret).is_ok() {
            return Ok(next.run(req).await);
        }
    }

    Err((
        StatusCode::FORBIDDEN,
        axum::Json(json!({"error": "Insufficient permissions or missing admin key"})),
    ))
}

/// Webhook Authentication Middleware.
/// Verifies x-evo-api-key header from evo API instances.
pub async fn webhook_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    let provided_secret = req
        .headers()
        .get("x-evo-api-key")
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.headers().get("x-webhook-secret").and_then(|v| v.to_str().ok())); // Backward compat

    let expected_secret = &state.config.evo_internal_api_key;

    if provided_secret.is_none()
        || expected_secret.is_empty()
        || !constant_time_eq(provided_secret.unwrap(), expected_secret)
    {
        return Err((
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "Invalid or missing internal API key"})),
        ));
    }

    Ok(next.run(req).await)
}

/// Legacy auth support during transition
async fn legacy_auth_middleware(
    state: Arc<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    let partner_id = req
        .headers()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Uuid::parse_str(v).ok())
        .ok_or((
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "x-tenant-id (partner_id) header missing or invalid"})),
        ))?;

    let tenant = match state.db.get_tenant_by_partner_id(&partner_id).await {
        Ok(Some(t)) => t,
        Ok(None) => return Err((StatusCode::FORBIDDEN, axum::Json(json!({"error": "Partner not found"})))),
        Err(_) => return Err((StatusCode::INTERNAL_SERVER_ERROR, axum::Json(json!({"error": "DB error"})))),
    };

    let ctx = TenantContext {
        tenant_id: tenant.id,
        partner_id,
        owner_id: Uuid::nil(),
        instance_name: tenant.instance_name.clone(),
        wa_number: tenant.wa_number.unwrap_or_default(),
        plan_tier: PlanTier::Enterprise,
        daily_limit: tenant.daily_crm_limit as u32,
        campaign_allowed: tenant.campaign_enabled,
        key_scopes: vec!["all".to_string()],
    };

    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}
