#these env variables should be set in your application
#  Platform Configuration (Production)

# ─── wa_api Connection ───────────────────────────────────────
# Points to the Rust Gateway
WA_API_URL= GATEWAY_URL

#Auth by the gateway is done using a environment variable named supabase_jwt if you are not using supabase or do not have a JWT create one using https://www.jwt.io/  

# ─── Integration Settings ─────────────────────────────────────
WA_API_DEFAULT_TENANT_ID=00000000-0000-0000-0000-000000000000
WA_API_TIMEOUT_MS=15000
WA_API_RETRY_COUNT=2
