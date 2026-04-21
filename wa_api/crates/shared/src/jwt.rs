use anyhow::{anyhow, Result};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct SupabaseClaims {
    pub aud: String,
    pub exp: usize,
    pub sub: Uuid,
    pub email: Option<String>,
    pub app_metadata: AppMetadata,
    pub user_metadata: Option<UserMetadata>,
    pub role: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppMetadata {
    pub provider: Option<String>,
    pub providers: Option<Vec<String>>,
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
    validation.set_audience(&["authenticated"]);

    let token_data = decode::<SupabaseClaims>(
        token,
        &DecodingKey::from_secret(secret.as_ref()),
        &validation,
    )
    .map_err(|e| anyhow!("JWT verification failed: {}", e))?;

    Ok(token_data.claims)
}

pub fn verify_admin_jwt(token: &str, secret: &str) -> Result<AdminClaims> {
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
