use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// Evolution API HTTP client.
/// Wraps calls to the Evolution API instance running in `evo/`.
#[derive(Clone)]
pub struct EvolutionClient {
    client: Client,
    base_url: String,
    api_key: String,
}

#[derive(Debug, Serialize)]
struct SendTextRequest {
    number: String,
    text: String,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
struct SendTemplateRequest {
    number: String,
    text: String, // rendered body
}

#[derive(Debug, Deserialize)]
pub struct SendResponse {
    pub key: Option<MessageKey>,
    pub status: Option<String>,
    pub message: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MessageKey {
    pub id: Option<String>,
    #[serde(rename = "remoteJid")]
    pub remote_jid: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct InstanceStatusResponse {
    pub instance: Option<InstanceInfo>,
    pub state: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct InstanceInfo {
    pub instance_name: Option<String>,
    pub status: Option<String>,
    pub owner: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum EvolutionError {
    /// Connection timeout / 503 / network issue — retryable
    Transient(String),
    /// HTTP 429 — respect retry-after
    RateLimit { retry_after_secs: u64 },
    /// Instance not found or session closed — pause all jobs for instance
    InstanceDisconnected(String),
    /// qr_required / auth_failed — move to DLQ
    AuthRequired(String),
    /// invalid_number / not_on_whatsapp — permanent failure
    InvalidRecipient(String),
    /// banned / account_restricted — escalate immediately
    Banned(String),
}

impl std::fmt::Display for EvolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvolutionError::Transient(m) => write!(f, "transient: {}", m),
            EvolutionError::RateLimit { retry_after_secs } => {
                write!(f, "rate_limit: retry after {}s", retry_after_secs)
            }
            EvolutionError::InstanceDisconnected(m) => write!(f, "instance_disconnected: {}", m),
            EvolutionError::AuthRequired(m) => write!(f, "auth_required: {}", m),
            EvolutionError::InvalidRecipient(m) => write!(f, "invalid_recipient: {}", m),
            EvolutionError::Banned(m) => write!(f, "banned: {}", m),
        }
    }
}

impl EvolutionClient {
    pub fn new(base_url: &str, api_key: &str) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        EvolutionClient {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
        }
    }

    /// Send a text message through a specific Evolution API instance.
    /// Returns the Evolution message ID on success.
    pub async fn send_text(
        &self,
        instance_name: &str,
        phone: &str,
        text: &str,
    ) -> Result<String, EvolutionError> {
        let url = format!(
            "{}/message/sendText/{}",
            self.base_url, instance_name
        );

        let body = SendTextRequest {
            number: phone.to_string(),
            text: text.to_string(),
        };

        debug!("Sending text via instance={} to={}", instance_name, &phone[..4]);

        let resp = self
            .client
            .post(&url)
            .header("apikey", &self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() || e.is_connect() {
                    EvolutionError::Transient(e.to_string())
                } else {
                    EvolutionError::Transient(e.to_string())
                }
            })?;

        let status = resp.status();

        if status == 429 {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(60);
            return Err(EvolutionError::RateLimit { retry_after_secs: retry_after });
        }

        if status.is_server_error() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(EvolutionError::Transient(format!(
                "HTTP {}: {}",
                status, body_text
            )));
        }

        let body_text = resp.text().await.map_err(|e| EvolutionError::Transient(e.to_string()))?;

        // Check for known error strings in response body
        let lower = body_text.to_lowercase();
        if lower.contains("qr_required") || lower.contains("auth_failed") || lower.contains("unauthorized") {
            return Err(EvolutionError::AuthRequired(body_text));
        }
        if lower.contains("banned") || lower.contains("account_restricted") {
            return Err(EvolutionError::Banned(body_text));
        }
        if lower.contains("invalid_number") || lower.contains("not_on_whatsapp") || lower.contains("not registered") {
            return Err(EvolutionError::InvalidRecipient(body_text));
        }
        if lower.contains("instance_not_found") || lower.contains("session_closed") || lower.contains("disconnected") {
            return Err(EvolutionError::InstanceDisconnected(body_text));
        }

        // Parse message ID from response
        let send_resp: SendResponse = serde_json::from_str(&body_text).map_err(|e| {
            EvolutionError::Transient(format!("Failed to parse response: {} body={}", e, body_text))
        })?;

        let msg_id = send_resp
            .key
            .and_then(|k| k.id)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        Ok(msg_id)
    }

    /// Check the connection status of an Evolution API instance.
    pub async fn get_instance_status(&self, instance_name: &str) -> Result<String> {
        let url = format!(
            "{}/instance/connectionState/{}",
            self.base_url, instance_name
        );

        let resp = self
            .client
            .get(&url)
            .header("apikey", &self.api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("HTTP {} from Evolution health check", resp.status()));
        }

        let body: serde_json::Value = resp.json().await?;
        let state = body
            .get("instance")
            .and_then(|i| i.get("state"))
            .and_then(|s| s.as_str())
            .unwrap_or("disconnected")
            .to_uppercase();

        Ok(state)
    }

    /// Fetch all instances from Evolution API.
    pub async fn fetch_instances(&self) -> Result<Vec<serde_json::Value>> {
        let url = format!("{}/instance/fetchInstances", self.base_url);
        let resp = self
            .client
            .get(&url)
            .header("apikey", &self.api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("HTTP {} from Evolution fetchInstances", resp.status()));
        }

        Ok(resp.json().await?)
    }

    /// Create a new instance in Evolution API (admin only, 1 per partner).
    pub async fn create_instance(&self, instance_name: &str) -> Result<serde_json::Value> {
        let url = format!("{}/instance/create", self.base_url);
        let body = serde_json::json!({
            "instanceName": instance_name,
            "qrcode": true,
            "integration": "WHATSAPP-BAILEYS",
            "phoneClient": "Leaex CRM"
        });
        let resp = self
            .client
            .post(&url)
            .header("apikey", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("Evolution create_instance HTTP {}: {}", status, body));
        }
        Ok(body)
    }

    /// Get QR code base64 for an instance (for partner connect flow).
    /// Returns Err if instance doesn't exist — admin must create it first.
    pub async fn get_instance_qr(&self, instance_name: &str) -> Result<(String, Option<String>)> {
        let url = format!("{}/instance/connect/{}", self.base_url, instance_name);
        let resp = self
            .client
            .get(&url)
            .header("apikey", &self.api_key)
            .send()
            .await?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();

        if status == 404 {
            return Err(anyhow!("instance_not_found: {}", instance_name));
        }
        if !status.is_success() {
            return Err(anyhow!("Evolution QR HTTP {}: {}", status, body));
        }

        // Evolution API v2 shape: { base64, code } or { qrcode: { base64, code } }
        let base64 = body.get("base64")
            .or_else(|| body.pointer("/qrcode/base64"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("No base64 in QR response — instance may already be connected"))?
            .to_string();

        let code = body.get("code")
            .or_else(|| body.pointer("/qrcode/code"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok((base64, code))
    }
}
