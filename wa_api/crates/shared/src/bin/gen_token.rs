use jsonwebtoken::{encode, EncodingKey, Header};
use shared::jwt::{AppMetadata, SupabaseClaims, UserMetadata};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn main() {
    dotenvy::dotenv().ok();
    let secret_raw = std::env::var("SUPABASE_JWT_SECRET").expect("SUPABASE_JWT_SECRET not set");
    
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine as _;
    let secret_bytes = BASE64_STANDARD.decode(&secret_raw).expect("Failed to decode secret from base64");
    
    let key = EncodingKey::from_secret(&secret_bytes);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize;
    let exp = now + 3600;

    // 1. Generate Admin Token
    let admin_claims = SupabaseClaims {
        aud: Some(serde_json::Value::String("authenticated".to_string())),
        exp,
        sub: Uuid::new_v4(),
        email: Some("leaex.core@gmail.com".to_string()),
        app_metadata: AppMetadata::default(),
        user_metadata: Some(UserMetadata {
            role: Some("admin".to_string()),
            full_name: Some("Platform Admin".to_string()),
        }),
        role: Some("authenticated".to_string()),
    };
    let admin_token = encode(&Header::default(), &admin_claims, &key).unwrap();

    // 2. Generate Partner Token
    let org_id = Uuid::parse_str("f52a3d0c-3ddc-427d-8222-326a960bebfe").unwrap();
    let partner_claims = SupabaseClaims {
        aud: Some(serde_json::Value::String("authenticated".to_string())),
        exp,
        sub: Uuid::new_v4(),
        email: Some("partner@example.com".to_string()),
        app_metadata: AppMetadata {
            org_id: Some(org_id),
            role: Some("partner".to_string()),
            ..Default::default()
        },
        user_metadata: Some(UserMetadata {
            role: Some("partner".to_string()),
            full_name: Some("Test Partner".to_string()),
        }),
        role: Some("authenticated".to_string()),
    };
    let partner_token = encode(&Header::default(), &partner_claims, &key).unwrap();

    println!("--- ADMIN TOKEN ---");
    println!("{}", admin_token);
    println!("\n--- PARTNER TOKEN ---");
    println!("{}", partner_token);
    println!("\nOrgID: {}", org_id);
}
