use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use shared::state::AppState;
use shared::types::TenantContext;

#[derive(Debug, Deserialize)]
pub struct ContactSearchQuery {
    /// Search term (name or phone number)
    pub q: String,
}

/// GET /contacts
/// Perform fuzzy name search or phone search within the partner's existing contact list.
async fn search_contacts(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Query(query): Query<ContactSearchQuery>,
) -> impl IntoResponse {
    let q = query.q.trim();
    if q.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Search query 'q' is required" })),
        )
            .into_response();
    }

    match state.db.search_contacts_fuzzy(&ctx.tenant_id, q).await {
        Ok(contacts) => (StatusCode::OK, Json(json!({ "contacts": contacts }))).into_response(),
        Err(e) => {
            tracing::error!("Contact search error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Database error" })),
            )
                .into_response()
        }
    }
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/contacts", axum::routing::get(search_contacts))
}
