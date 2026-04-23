use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use crate::db::TenantRow;
use crate::state::AppState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EtiquetteState {
    New,
    Greeted,
    AwaitingConcern,
    Notified,
    Apologized,
    PartnerActive,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConvoState {
    pub state: EtiquetteState,
    pub last_inbound_at: DateTime<Utc>,
    pub last_outbound_at: Option<DateTime<Utc>>,
    pub apology_scheduled_at: Option<DateTime<Utc>>,
}

pub struct LoraTemplates;

impl LoraTemplates {
    pub fn greeting(customer_name: &str, business_name: &str) -> String {
        format!(
            "Hi {}, I'm Lora from Leaex on behalf of {}. How can I assist you today?",
            customer_name, business_name
        )
    }

    pub fn ask_concern() -> String {
        "Thank you! Could you please share your concern or the issue you're facing?".to_string()
    }

    pub fn notify_wait(partner_name: &str) -> String {
        format!(
            "Got it. Please wait a few minutes while I notify {} and coordinate.",
            partner_name
        )
    }

    pub fn partner_busy(business_name: &str) -> String {
        format!(
            "I apologize for the delay. It seems our partner at {} is quite busy at the moment. Thank you for your patience.",
            business_name
        )
    }

    pub fn closing() -> String {
        "Since we haven't heard back, I'll be closing this chat for now. Feel free to reach out again if you need further assistance!".to_string()
    }
}

/// Core logic for Lora's etiquette flow.
/// Processes an inbound message and returns an optional message to send back.
pub async fn process_inbound(
    state: Arc<AppState>,
    tenant: &TenantRow,
    customer_phone: &str,
    customer_name: &str,
    _message_body: &str,
) -> Result<Option<String>> {
    let redis = &state.redis;
    let key = format!("etiquette:state:{}:{}", tenant.id, customer_phone);

    // 1. Get or Init state
    let mut convo: ConvoState = match redis.get::<ConvoState>(&key).await? {
        Some(c) => {
            // Reset to New if more than 24 hours since last activity
            if Utc::now()
                .signed_duration_since(c.last_inbound_at)
                .num_hours()
                >= 24
            {
                ConvoState {
                    state: EtiquetteState::New,
                    last_inbound_at: Utc::now(),
                    last_outbound_at: None,
                    apology_scheduled_at: None,
                }
            } else {
                c
            }
        }
        None => ConvoState {
            state: EtiquetteState::New,
            last_inbound_at: Utc::now(),
            last_outbound_at: None,
            apology_scheduled_at: None,
        },
    };

    convo.last_inbound_at = Utc::now();
    let biz_name = tenant.business_name.as_deref().unwrap_or("our team");
    let part_name = tenant.partner_name.as_deref().unwrap_or(biz_name);

    let response = match convo.state {
        EtiquetteState::New => {
            convo.state = EtiquetteState::Greeted;
            Some(LoraTemplates::greeting(customer_name, biz_name))
        }
        EtiquetteState::Greeted => {
            convo.state = EtiquetteState::AwaitingConcern;
            Some(LoraTemplates::ask_concern())
        }
        EtiquetteState::AwaitingConcern => {
            convo.state = EtiquetteState::Notified;
            // Schedule apology between 5-10 minutes from now
            let delay_mins = crate::utils::random_delay_secs(5 * 60, 10 * 60);
            let deadline = Utc::now() + chrono::Duration::seconds(delay_mins as i64);
            convo.apology_scheduled_at = Some(deadline);

            // Add to ZSET queue for processing
            let _ = redis
                .zadd_etiquette_delay(&tenant.id, customer_phone, deadline.timestamp() as f64)
                .await;

            Some(LoraTemplates::notify_wait(part_name))
        }
        _ => None, // Already notified or partner is active
    };

    redis.set_ex(&key, &convo, 48 * 3600).await?;
    Ok(response)
}

/// Processes an outbound message from a partner.
/// This typically suppresses Lora's etiquette logic for the duration of the conversation.
pub async fn process_outbound(
    state: Arc<AppState>,
    tenant_id: &Uuid,
    customer_phone: &str,
) -> Result<()> {
    let redis = &state.redis;
    let key = format!("etiquette:state:{}:{}", tenant_id, customer_phone);

    if let Some(mut convo) = redis.get::<ConvoState>(&key).await? {
        convo.state = EtiquetteState::PartnerActive;
        convo.last_outbound_at = Some(Utc::now());
        convo.apology_scheduled_at = None; // Cancel scheduled apology

        // Remove from ZSET queue
        let _ = redis.zrem_etiquette_delay(tenant_id, customer_phone).await;

        redis.set_ex(&key, &convo, 48 * 3600).await?;
    }

    Ok(())
}

pub async fn check_deadlines(state: Arc<AppState>) -> Result<()> {
    let redis = &state.redis;
    let now_score = Utc::now().timestamp() as f64;

    // 1. Get all expired apology deadlines
    let expired = redis.zrange_etiquette_deadlines(now_score).await?;

    for entry in expired {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 2 {
            continue;
        }
        let tenant_id_str = parts[0];
        let phone = parts[1];

        if let Ok(tenant_id) = Uuid::parse_str(tenant_id_str) {
            let key = format!("etiquette:state:{}:{}", tenant_id, phone);
            if let Some(mut convo) = redis.get::<ConvoState>(&key).await? {
                if convo.state == EtiquetteState::Notified {
                    if let Ok(Some(tenant)) = state.db.get_tenant(&tenant_id).await {
                        let biz_name = tenant.business_name.as_deref().unwrap_or("our team");
                        let apology = LoraTemplates::partner_busy(biz_name);

                        info!(phone = %phone, "Lora sending apology for partner delay");
                        if let Err(e) = state
                            .evo
                            .send_text(&tenant.instance_name, phone, &apology)
                            .await
                        {
                            tracing::error!("Failed to send Lora apology to {}: {}", phone, e);
                        } else {
                            convo.state = EtiquetteState::Apologized;
                            convo.apology_scheduled_at = None;

                            // Remove from ZSET and update state
                            let _ = redis.zrem_etiquette_delay(&tenant_id, phone).await;
                            redis.set_ex(&key, &convo, 48 * 3600).await?;

                            // Sync to Platform
                            if let (Some(platform_db), Some(org_id)) =
                                (&state.platform_db, &tenant.partner_id)
                            {
                                let sync_payload = serde_json::json!({
                                    "phone": phone,
                                    "body": apology,
                                    "push_name": "Lora (System)",
                                    "msg_id": format!("lora-apology-{}", Utc::now().timestamp()),
                                    "direction": "outbound",
                                    "timestamp": Utc::now()
                                });
                                let _ = state
                                    .db
                                    .sync_to_platform_rpc(
                                        platform_db,
                                        org_id,
                                        &tenant.instance_name,
                                        "system_outbound",
                                        sync_payload,
                                    )
                                    .await;
                            }
                        }
                    }
                }
            }
        }
    }

    // 2. Idle closings (Scan-based but slower frequency, or skip for now)
    // For now, let's just use the ZSET for apologies which are the time-critical part.

    Ok(())
}
