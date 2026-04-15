use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use shared::types::TenantContext;
use std::sync::Arc;
use tracing::error;

use shared::state::AppState;

#[derive(Debug, Deserialize)]
pub struct AnalyticsQuery {
    pub page: Option<u32>,
    pub limit: Option<u32>,
    pub status: Option<String>,
    pub message_type: Option<String>,
}

#[derive(sqlx::FromRow)]
struct InteractionRowRaw {
    id: uuid::Uuid,
    campaign_id: Option<uuid::Uuid>,
    message_type: String,
    recipient_phone: String,
    recipient_name: Option<String>,
    instance_used: String,
    status: String,
    error_reason: Option<String>,
    retry_count: i16,
    scheduled_at: chrono::DateTime<chrono::Utc>,
    sent_at: Option<chrono::DateTime<chrono::Utc>>,
    delivered_at: Option<chrono::DateTime<chrono::Utc>>,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(serde::Serialize)]
struct InteractionRow {
    id: uuid::Uuid,
    campaign_id: Option<uuid::Uuid>,
    message_type: String,
    /// Masked phone number — never expose full PII in analytics
    recipient_phone_masked: String,
    recipient_name: Option<String>,
    instance_used: String,
    status: String,
    error_reason: Option<String>,
    retry_count: i16,
    scheduled_at: chrono::DateTime<chrono::Utc>,
    sent_at: Option<chrono::DateTime<chrono::Utc>>,
    delivered_at: Option<chrono::DateTime<chrono::Utc>>,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow, serde::Serialize)]
struct DailySummary {
    total_today: i64,
    sent_today: i64,
    delivered_today: i64,
    failed_today: i64,
    deferred_today: i64,
    inbound_today: i64,
}

/// GET /analytics/messages
async fn messages_analytics(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<TenantContext>,
    Query(params): Query<AnalyticsQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1).max(1);
    let limit = params.limit.unwrap_or(50).min(200) as i64;
    let offset = ((page - 1) * limit as u32) as i64;

    // Build optional filters
    let status_filter = params.status.as_deref().unwrap_or("");
    let mut type_filter = params.message_type.as_deref().unwrap_or("").to_string();

    // UI might send 'crm' but DB uses 'manual_crm'
    if type_filter == "crm" {
        type_filter = "manual_crm".to_string();
    }

    let rows = sqlx::query_as::<_, InteractionRowRaw>(
        "SELECT id, campaign_id, message_type, recipient_phone, recipient_name, instance_used, \
         status, error_reason, retry_count, scheduled_at, sent_at, delivered_at, created_at \
         FROM wa_interaction_log \
         WHERE tenant_id = $1 \
           AND ($2 = '' OR status = $2) \
           AND ($3 = '' OR message_type = $3) \
         ORDER BY created_at DESC \
         LIMIT $4 OFFSET $5",
    )
    .bind(ctx.tenant_id)
    .bind(status_filter)
    .bind(&type_filter)
    .bind(limit)
    .bind(offset)
    .fetch_all(state.db.pool())
    .await;

    // Daily summary (today UTC)
    let summary = sqlx::query_as::<_, DailySummary>(
        "SELECT \
          COUNT(*) as total_today, \
          COUNT(*) FILTER (WHERE status IN ('sent','delivered','read')) as sent_today, \
          COUNT(*) FILTER (WHERE status IN ('delivered','read')) as delivered_today, \
          COUNT(*) FILTER (WHERE status = 'failed') as failed_today, \
          COUNT(*) FILTER (WHERE status = 'deferred_spam') as deferred_today, \
          COUNT(*) FILTER (WHERE message_type = 'inbound') as inbound_today \
         FROM wa_interaction_log \
         WHERE tenant_id = $1 AND created_at >= CURRENT_DATE",
    )
    .bind(ctx.tenant_id)
    .fetch_optional(state.db.pool())
    .await;

    match (rows, summary) {
        (Ok(raw_messages), Ok(s)) => {
            // Mask phone numbers before returning to client
            let messages: Vec<InteractionRow> = raw_messages
                .into_iter()
                .map(|r| InteractionRow {
                    id: r.id,
                    campaign_id: r.campaign_id,
                    message_type: r.message_type,
                    recipient_phone_masked: shared::utils::mask_phone(&r.recipient_phone),
                    recipient_name: r.recipient_name,
                    instance_used: r.instance_used,
                    status: r.status,
                    error_reason: r.error_reason,
                    retry_count: r.retry_count,
                    scheduled_at: r.scheduled_at,
                    sent_at: r.sent_at,
                    delivered_at: r.delivered_at,
                    created_at: r.created_at,
                })
                .collect();

            let s_ref = s.as_ref();
            let sent = s_ref.map(|d| d.sent_today).unwrap_or(0);
            let delivered = s_ref.map(|d| d.delivered_today).unwrap_or(0);
            let inbound = s_ref.map(|d| d.inbound_today).unwrap_or(0);

            let delivery_rate = if sent > 0 {
                format!("{:.1}%", (delivered as f64 / sent as f64) * 100.0)
            } else {
                "—".to_string()
            };

            // Ack Rate: (Delivered / Sent) * 100
            let ack_rate = delivery_rate.clone();

            // Response Rate: (Inbound / Sent) * 100 (approximate proxy for engagement)
            let response_rate = if sent > 0 {
                format!("{:.1}%", (inbound as f64 / sent as f64) * 100.0)
            } else {
                "—".to_string()
            };

            // Follow-up Rate: % of inbound that got a reply (complex to calculate exactly, using proxy for now)
            let followup_rate = if inbound > 0 {
                format!("{:.1}%", (sent as f64 / inbound as f64).min(1.0) * 100.0)
            } else {
                "—".to_string()
            };

            (
                StatusCode::OK,
                Json(json!({
                    "tenant_id": ctx.tenant_id,
                    "page": page,
                    "limit": limit,
                    "messages": messages,
                    "summary": {
                        "total_today": s_ref.map(|d| d.total_today).unwrap_or(0),
                        "sent_today": sent,
                        "delivered_today": delivered,
                        "failed_today": s_ref.map(|d| d.failed_today).unwrap_or(0),
                        "deferred_today": s_ref.map(|d| d.deferred_today).unwrap_or(0),
                        "inbound_today": inbound,
                        "delivery_rate": delivery_rate,
                        "ack_rate": ack_rate,
                        "response_rate": response_rate,
                        "followup_rate": followup_rate,
                    }
                })),
            )
                .into_response()
        }
        (Err(e), _) | (_, Err(e)) => {
            error!("Analytics query error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database error"})),
            )
                .into_response()
        }
    }
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/analytics/messages", get(messages_analytics))
}
