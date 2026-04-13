use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use serde_json::json;
use shared::{types::TenantContext, utils::sha256};
use std::sync::Arc;
use tracing::{error, warn};
use uuid::Uuid;

use crate::state::AppState;

/// Extract x-api-key from request, resolve to TenantContext, inject into extensions.
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    let api_key = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let api_key = match api_key {
        Some(k) => k,
        None => {
            return Err((
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({"error": "Missing x-api-key header"})),
            ))
        }
    };

    // Try cache first (5 min TTL)
    let key_hash = sha256(&api_key);
    let redis = state.redis.clone();

    if let Ok(Some(ctx)) = redis.get_cached_api_key::<TenantContext>(&key_hash).await {
        req.extensions_mut().insert(ctx);
        return Ok(next.run(req).await);
    }

    // Cache miss — query DB
    let agency = match state.db.get_agency_by_api_key_hash(&key_hash).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            warn!("Unknown API key attempted");
            return Err((
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({"error": "Invalid API key"})),
            ));
        }
        Err(e) => {
            error!("DB lookup error: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "Auth service unavailable"})),
            ));
        }
    };

    // Validate subscription
    if agency.subscription_status != "active" {
        return Err((
            StatusCode::FORBIDDEN,
            axum::Json(json!({"error": "Account suspended or not active"})),
        ));
    }

    // Extract tenant_id from request extension or header
    // For single-tenant API keys the tenant = the agency's default tenant
    // For multi-tenant the caller passes X-Tenant-Id
    let tenant_id_str = req
        .headers()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let tenant_id = match tenant_id_str
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok())
    {
        Some(id) => id,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": "Missing X-Tenant-Id header"})),
            ));
        }
    };

    // Get tenant row
    let tenant = match state.db.get_tenant(&tenant_id, &agency.id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return Err((
                StatusCode::FORBIDDEN,
                axum::Json(json!({"error": "Tenant not found or not owned by this agency"})),
            ));
        }
        Err(e) => {
            error!("Tenant DB lookup error: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "Auth service unavailable"})),
            ));
        }
    };

    // Validate instance state
    if tenant.instance_status == "banned" || tenant.instance_status == "suspended" {
        return Err((
            StatusCode::FORBIDDEN,
            axum::Json(json!({"error": "WhatsApp instance is banned or suspended"})),
        ));
    }

    // Allow /instance/qr and /instance/health through even when QR scan is pending —
    // those are the routes the partner uses TO complete the setup.
    let path = req.uri().path();
    let is_setup_route = path == "/instance/qr" || path == "/instance/health";

    if tenant.instance_status == "qr_required" && !is_setup_route {
        return Err((
            StatusCode::CONFLICT,
            axum::Json(
                json!({"error": "WhatsApp instance needs re-authentication (QR scan required)", "code": "QR_REQUIRED"}),
            ),
        ));
    }

    let plan_tier = match agency.plan_tier.as_str() {
        "basic" => shared::types::PlanTier::Basic,
        "pro" => shared::types::PlanTier::Pro,
        "enterprise" => shared::types::PlanTier::Enterprise,
        _ => shared::types::PlanTier::Basic,
    };

    let ctx = TenantContext {
        agency_id: agency.id,
        tenant_id,
        partner_id: tenant.partner_id.unwrap_or(Uuid::nil()),
        instance_name: tenant.instance_name.clone(),
        wa_number: tenant.wa_number.unwrap_or_default(),
        plan_tier,
        daily_limit: tenant.daily_crm_limit as u32,
        campaign_allowed: tenant.campaign_enabled,
    };

    // Cache for 5 minutes
    let _ = redis.cache_api_key(&key_hash, &ctx).await;

    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}

/// Admin-only auth middleware — requires x-admin-key header.
pub async fn admin_auth_middleware(
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, axum::Json<serde_json::Value>)> {
    let admin_key = req
        .headers()
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let expected = std::env::var("ADMIN_API_KEY").unwrap_or_default();

    match admin_key {
        Some(k) if !expected.is_empty() && k == expected => Ok(next.run(req).await),
        Some(_) => Err((
            StatusCode::FORBIDDEN,
            axum::Json(json!({"error": "Invalid admin key"})),
        )),
        None => Err((
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "Missing x-admin-key header"})),
        )),
    }
}
