use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info};

use shared::state::AppState;

/// evo API webhook payload (simplified).
#[derive(Debug, Deserialize)]
pub struct EvoWebhook {
    pub event: String,
    pub instance: Option<String>,
    pub data: Option<serde_json::Value>,
}

/// POST /webhook/evo
/// Receives delivery receipts, incoming messages, and presence updates from evo API.
async fn evo_webhook(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EvoWebhook>,
) -> impl IntoResponse {
    match payload.event.as_str() {
        // Delivery receipt
        "messages.update" => {
            if let Some(data) = &payload.data {
                let msg_id = data
                    .get("key")
                    .and_then(|k| k.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();

                let status_str = data
                    .get("update")
                    .and_then(|u| u.get("status"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown");

                let delivery_status = match status_str {
                    "DELIVERY_ACK" | "DELIVERED" => shared::types::JobStatus::Delivered,
                    "READ" => shared::types::JobStatus::Read,
                    "FAILED" | "ERROR" => shared::types::JobStatus::Failed,
                    _ => return (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
                };

                let delivered_at = if delivery_status == shared::types::JobStatus::Delivered
                    || delivery_status == shared::types::JobStatus::Read
                {
                    Some(chrono::Utc::now())
                } else {
                    None
                };

                if !msg_id.is_empty() {
                    if let Ok(Some(campaign_id)) = state
                        .db
                        .update_interaction_status(msg_id, delivery_status.as_str(), delivered_at)
                        .await
                    {
                        // R10: Atomic counter increment via RPC
                        let (delivered, failed) = match delivery_status {
                            shared::types::JobStatus::Delivered
                            | shared::types::JobStatus::Read => (1, 0),
                            shared::types::JobStatus::Failed => (0, 1),
                            _ => (0, 0),
                        };

                        if delivered > 0 || failed > 0 {
                            let _ = state
                                .db
                                .increment_campaign_counters(&campaign_id, 0, delivered, failed)
                                .await;
                        }
                    }
                }
            }
        }

        // Message Upsert (Inbound messages and STOP keywords)
        "messages.upsert" => {
            if let (Some(instance), Some(data)) = (&payload.instance, &payload.data) {
                let messages = data.as_array().cloned().unwrap_or_default();
                for msg in &messages {
                    let from_me = msg
                        .get("key")
                        .and_then(|k| k.get("fromMe"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);

                    // Extract phone from JID (format: 919XXXXXXXXXX@s.whatsapp.net)
                    let from_jid = msg
                        .get("key")
                        .and_then(|k| k.get("remoteJid"))
                        .and_then(|j| j.as_str())
                        .unwrap_or("");
                    let phone = from_jid
                        .split('@')
                        .next()
                        .map(|s| format!("+{}", s))
                        .unwrap_or_default();
                    let msg_id = msg
                        .get("key")
                        .and_then(|k| k.get("id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    if from_me || phone.is_empty() || msg_id.is_empty() {
                        continue;
                    }

                    let body = msg
                        .get("message")
                        .and_then(|m| {
                            m.get("conversation")
                                .or_else(|| m.pointer("/extendedTextMessage/text"))
                        })
                        .and_then(|c| c.as_str())
                        .unwrap_or("")
                        .trim();

                    if let Ok(Some(tenant)) = state.db.get_tenant_by_instance_name(instance).await {
                        let tenant_id = tenant.id;
                        let push_name = msg.get("pushName").and_then(|v| v.as_str());
                        let log = shared::db::InteractionLogInsert {
                            tenant_id,
                            campaign_id: None,
                            message_type: "inbound".to_string(),
                            recipient_phone: phone.clone(),
                            recipient_name: push_name.map(|s| s.to_string()),
                            instance_used: instance.clone(),
                            status: "delivered".to_string(),
                            evo_msg_id: Some(msg_id.to_string()),
                            error_reason: None,
                            retry_count: 0,
                            scheduled_at: chrono::Utc::now(),
                            sent_at: Some(chrono::Utc::now()),
                            idempotency_key: format!("inbound:{}", msg_id),
                        };
                        let _ = state.db.insert_interaction(&log).await;

                        // ─── 4. Lora Etiquette Bot & RPC Sync ───────────────
                        let push_name_str = push_name.unwrap_or("Customer");

                        // a) Apply Lora logic
                        if let Ok(Some(bot_msg)) = shared::etiquette::process_inbound(
                            state.clone(),
                            &tenant,
                            &phone,
                            push_name_str,
                            body,
                        )
                        .await
                        {
                            let _ = state.evo.send_text(instance, &phone, &bot_msg).await;
                        }

                        // b) Platform RPC Sync
                        if let (Some(platform_db), Some(org_id)) =
                            (&state.platform_db, &tenant.partner_id)
                        {
                            let payload = json!({
                                "phone": phone,
                                "body": body,
                                "push_name": push_name_str,
                                "msg_id": msg_id,
                                "direction": "inbound",
                                "timestamp": chrono::Utc::now()
                            });
                            let _ = state
                                .db
                                .sync_to_platform_rpc(
                                    platform_db,
                                    org_id,
                                    instance,
                                    "inbound",
                                    payload,
                                )
                                .await;
                        }
                    }

                    // 2. Handle STOP keywords for opt-out
                    let upper_body = body.to_uppercase();
                    if upper_body == "STOP" || upper_body == "UNSUBSCRIBE" {
                        info!(phone = %phone, "STOP keyword received — opting out platform-wide");
                        let _ = state.db.upsert_opt_out(&phone, None, "stop_keyword").await;
                    }
                }
            }
        }

        "connection.update" => {
            // Instance connection state changed
            if let (Some(instance), Some(data)) = (&payload.instance, &payload.data) {
                let state_str = data
                    .get("state")
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown");

                info!(instance = %instance, state = %state_str, "Instance connection update");

                let health = match state_str {
                    "open" => shared::types::InstanceHealth::Active,
                    "connecting" => shared::types::InstanceHealth::Connecting,
                    "close" | "closed" => shared::types::InstanceHealth::Disconnected,
                    _ => shared::types::InstanceHealth::Disconnected,
                };

                let redis = state.redis.clone();
                let _ = redis.set_instance_health(instance, &health).await;

                // Sync to Platform (Leaex v2)
                if let Ok(Some(tenant)) = state.db.get_tenant_by_instance_name(instance).await {
                    if let Some(partner_id) = tenant.partner_id {
                        sync_to_platform(&state, partner_id, instance, health.as_str()).await;
                    }
                }
            }
        }

        "contacts.update" | "contacts.upsert" => {
            if let (Some(instance), Some(data)) = (&payload.instance, &payload.data) {
                if let Ok(Some(tenant)) = state.db.get_tenant_by_instance_name(instance).await {
                    let contacts = if data.is_array() {
                        data.as_array().cloned().unwrap_or_default()
                    } else {
                        vec![data.clone()]
                    };

                    for contact in contacts {
                        let jid = contact
                            .get("id")
                            .and_then(|v| v.as_str())
                            .or_else(|| contact.get("remoteJid").and_then(|v| v.as_str()))
                            .unwrap_or("");
                        let name = contact
                            .get("name")
                            .and_then(|v| v.as_str())
                            .or_else(|| contact.get("pushName").and_then(|v| v.as_str()))
                            .or_else(|| contact.get("verifiedName").and_then(|v| v.as_str()));
                        let pic = contact.get("profilePicUrl").and_then(|v| v.as_str());

                        if !jid.is_empty() {
                            let phone = jid
                                .split('@')
                                .next()
                                .map(|s| format!("+{}", s))
                                .unwrap_or_default();
                            if !phone.is_empty() {
                                // 1. Backfill name in interaction log
                                if let Some(n) = name {
                                    let _ = state.db.update_interaction_name(&phone, n).await;
                                }
                                // 2. Upsert to wa_contacts for presence/profile pic support
                                let _ =
                                    state.db.upsert_contact(&tenant.id, &phone, name, pic).await;
                            }
                        }
                    }
                }
            }
        }

        "send.message" => {
            if let Some(data) = &payload.data {
                let msg_id = data
                    .get("key")
                    .and_then(|k| k.get("id"))
                    .and_then(|v| v.as_str())
                    .or_else(|| data.get("messageId").and_then(|v| v.as_str()))
                    .unwrap_or("");

                if !msg_id.is_empty() {
                    let _ = state
                        .db
                        .update_interaction_status(msg_id, "sent", None)
                        .await;
                }
            }
        }

        "presence.update" => {
            if let (Some(instance), Some(data)) = (&payload.instance, &payload.data) {
                if let Ok(Some(tenant)) = state.db.get_tenant_by_instance_name(instance).await {
                    let jid = data.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let presences = data.get("presences").and_then(|p| p.as_object());

                    if let Some(p_map) = presences {
                        for (p_jid, p_data) in p_map {
                            let status = p_data
                                .get("lastKnownPresence")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unavailable");
                            let phone = p_jid
                                .split('@')
                                .next()
                                .map(|s| format!("+{}", s))
                                .unwrap_or_default();
                            let _ = state
                                .db
                                .update_contact_presence(&tenant.id, &phone, status)
                                .await;
                        }
                    } else if !jid.is_empty() {
                        let phone = jid
                            .split('@')
                            .next()
                            .map(|s| format!("+{}", s))
                            .unwrap_or_default();
                        let _ = state
                            .db
                            .update_contact_presence(&tenant.id, &phone, "composing")
                            .await;
                    }
                }
            }
        }

        "qrcode.updated" => {
            if let (Some(instance), Some(data)) = (&payload.instance, &payload.data) {
                let qrcode = data
                    .get("qrcode")
                    .and_then(|q| q.get("base64"))
                    .and_then(|v| v.as_str());
                if let Some(base64) = qrcode {
                    let redis = state.redis.clone();
                    let key = format!("instance_qr:{}", instance);
                    let _ = redis.set_string_ex(&key, base64, 40).await;
                    info!(instance = %instance, "Stored refreshed QR code in cache");

                    // Notify platform that QR is pending scan
                    if let Ok(Some(tenant)) = state.db.get_tenant_by_instance_name(instance).await {
                        if let Some(partner_id) = tenant.partner_id {
                            sync_to_platform(&state, partner_id, instance, "qr_pending").await;
                        }
                    }
                }
            }
        }

        "chats.upsert" | "chats.update" => {
            if let Some(data) = &payload.data {
                let count = if data.is_array() {
                    data.as_array().map(|a| a.len()).unwrap_or(0)
                } else {
                    1
                };
                info!(instance = %payload.instance.as_deref().unwrap_or("unknown"), event = %payload.event, count, "WhatsApp chat list updated");
            }
        }

        event => {
            let noise = ["labels.edit", "labels.association"];
            if !noise.contains(&event) {
                info!(
                    instance = %payload.instance.as_deref().unwrap_or("unknown"),
                    event = %event,
                    "evo webhook event received"
                );
            }
        }
    }

    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

/// Notify the main platform (Leaex v2) of status changes.
async fn sync_to_platform(
    state: &AppState,
    partner_id: uuid::Uuid,
    instance_name: &str,
    status: &str,
) {
    let url = match &state.config.platform_webhook_url {
        Some(u) => u,
        None => return,
    };

    let platform_status = match status {
        "active" | "open" | "connected" => "connected",
        "qr_required" | "qr_pending" => "qr_pending",
        "banned" => "banned",
        "connecting" => "connecting",
        _ => "disconnected",
    };

    let client = reqwest::Client::new();
    let payload = json!({
        "partner_id": partner_id,
        "instance_id": instance_name,
        "status": platform_status,
    });

    let mut request = client.post(url).json(&payload);
    if let Some(key) = &state.config.platform_api_key {
        request = request.header("x-platform-api-key", key);
    }

    if let Err(e) = request.send().await {
        error!(partner_id = %partner_id, "Failed to sync status to platform: {}", e);
    } else {
        info!(partner_id = %partner_id, status = %platform_status, "Synced status to platform");
    }
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/webhook/evo",
            post(evo_webhook).get(|| async { "Webhook endpoint is alive. Use POST to send data." }),
        )
        .route(
            "/webhook/evo/:action",
            post(evo_webhook).get(|| async { "Webhook endpoint is alive. Use POST to send data." }),
        )
}
