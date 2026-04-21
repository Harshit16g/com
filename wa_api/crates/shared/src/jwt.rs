use anyhow::{anyhow, Result};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct SupabaseClaims {
    #[serde(default)]
    pub aud: Option<serde_json::Value>,
    pub exp: usize,
    pub sub: Uuid,
    pub email: Option<String>,
    #[serde(default)]
    pub app_metadata: AppMetadata,
    #[serde(default)]
    pub user_metadata: Option<UserMetadata>,
    #[serde(default)]
    pub role: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AppMetadata {
    pub provider: Option<String>,
    pub providers: Option<Vec<String>>,
    #[serde(alias = "orgId")]
    pub org_id: Option<Uuid>,
    pub role: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserMetadata {
    pub role: Option<String>,
    pub full_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AdminClaims {
    pub sub: String,
    pub exp: usize,
    pub role: String, // must be "admin"
}

pub fn verify_supabase_jwt(token: &str, secret: &str) -> Result<SupabaseClaims> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_aud = false;

    // Supabase JWT secret is often b64 encoded in the dashboard, 
    // but the library needs the raw bytes.
    let decoding_key = if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(secret) {
        DecodingKey::from_secret(&bytes)
    } else {
        // Fallback to raw string if not b64
        DecodingKey::from_secret(secret.as_bytes())
    };

    let token_data = decode::<SupabaseClaims>(token, &decoding_key, &validation)
        .map_err(|e| anyhow!("JWT verification failed: {}", e))?;

    Ok(token_data.claims)
}

pub fn verify_admin_jwt(token: &str, secret: &str) -> Result<AdminClaims> {
    // Admin secret is usually a raw string, not base64.
    // If it's also base64, we'd decode it, but for now assuming it matches the .env direct value.
    let validation = Validation::new(Algorithm::HS256);

    let token_data = decode::<AdminClaims>(
        token,
        &DecodingKey::from_secret(secret.as_ref()),
        &validation,
    )
    .map_err(|e| anyhow!("Admin JWT verification failed: {}", e))?;

    if token_data.claims.role != "admin" {
        return Err(anyhow!("Invalid admin role in JWT"));
    }

    Ok(token_data.claims)
}
