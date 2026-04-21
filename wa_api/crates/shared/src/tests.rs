use shared::jwt::{verify_supabase_jwt, SupabaseClaims};
use uuid::Uuid;
use serde_json::json;

#[test]
fn test_jwt_deserialization() {
    let raw_claims = json!({
        "aud": "authenticated",
        "exp": 1234567890,
        "sub": Uuid::new_v4(),
        "email": "test@example.com",
        "app_metadata": {
            "provider": "email",
            "role": "admin"
        },
        "user_metadata": {
            "role": "admin"
        },
        "role": "authenticated"
    });
    
    let claims_str = serde_json::to_string(&raw_claims).unwrap();
    let claims: SupabaseClaims = serde_json::from_str(&claims_str).expect("Should deserialize robustly");
    
    assert_eq!(claims.role.as_deref(), Some("authenticated"));
    assert_eq!(claims.app_metadata.role.as_deref(), Some("admin"));
}

#[test]
fn test_jwt_with_missing_fields() {
    // Test that missing fields (except sub/exp) don't break it
    let raw_claims = json!({
        "exp": 1234567890,
        "sub": Uuid::new_v4(),
    });
    
    let claims_str = serde_json::to_string(&raw_claims).unwrap();
    let _claims: SupabaseClaims = serde_json::from_str(&claims_str).expect("Should default missing fields");
}
