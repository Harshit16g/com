-- ─────────────────────────────────────────────────────────────────────────────
-- Leaex WhatsApp Engine — v3.0 → v3.1 Migration
-- Run this on existing v3.0 deployments to evolve the schema.
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Remove agency dependency from tenants
ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_agency_id_fkey;
ALTER TABLE tenants DROP COLUMN IF EXISTS agency_id;

-- 2. Add direction + intent columns to wa_interaction_log
ALTER TABLE wa_interaction_log
  ADD COLUMN IF NOT EXISTS direction TEXT NOT NULL DEFAULT 'outbound'
    CHECK (direction IN ('inbound', 'outbound'));

ALTER TABLE wa_interaction_log
  ADD COLUMN IF NOT EXISTS intent TEXT
    CHECK (intent IS NULL OR intent IN (
      'support', 'sales', 'marketing', 'feedback',
      'transactional', 're_engagement', 'general'
    ));

CREATE INDEX IF NOT EXISTS idx_interaction_direction
  ON wa_interaction_log(direction);

-- 3. Expand message_type CHECK constraint
ALTER TABLE wa_interaction_log DROP CONSTRAINT IF EXISTS wa_interaction_log_message_type_check;
ALTER TABLE wa_interaction_log
  ADD CONSTRAINT wa_interaction_log_message_type_check
  CHECK (message_type IN (
    'campaign', 'booking_confirm', 'reminder', 'birthday',
    'anniversary', 'manual_crm', 're_engagement',
    'inbound', 'outbound', 'feedback', 'complaint'
  ));

-- 4. Expand instance_health_log event_type CHECK constraint
ALTER TABLE instance_health_log DROP CONSTRAINT IF EXISTS instance_health_log_event_type_check;
ALTER TABLE instance_health_log
  ADD CONSTRAINT instance_health_log_event_type_check
  CHECK (event_type IN (
    'connected', 'disconnected', 'qr_required', 'banned',
    'health_check_ok', 'health_check_fail',
    'connection_state_change', 'admin_force_qr'
  ));

-- 5. Create wa_contacts table if not exists
CREATE TABLE IF NOT EXISTS wa_contacts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    phone           TEXT NOT NULL,
    name            TEXT,
    profile_pic_url TEXT,
    last_presence   TEXT,
    last_seen_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, phone)
);
CREATE INDEX IF NOT EXISTS idx_contacts_tenant ON wa_contacts(tenant_id);

-- 6. Remove broken RLS policies (they depend on auth.jwt() which is Supabase-only)
-- NOTE: We enforce tenant isolation in application code, not via RLS.
DROP POLICY IF EXISTS "tenant_isolation_interactions" ON wa_interaction_log;
DROP POLICY IF EXISTS "tenant_isolation_campaigns" ON wa_campaigns;
DROP POLICY IF EXISTS "tenant_isolation_consent" ON wa_customer_consent;
DROP POLICY IF EXISTS "admin_only_pool_stats" ON pool_number_stats;

ALTER TABLE wa_interaction_log  DISABLE ROW LEVEL SECURITY;
ALTER TABLE wa_campaigns        DISABLE ROW LEVEL SECURITY;
ALTER TABLE wa_customer_consent DISABLE ROW LEVEL SECURITY;
ALTER TABLE pool_number_stats   DISABLE ROW LEVEL SECURITY;

-- 7. Remove unused api_keys table (auth is via env-var shared secrets)
DROP TABLE IF EXISTS api_keys;

-- 8. Remove agencies table (each agency gets its own wa_api deployment)
DROP TABLE IF EXISTS agencies CASCADE;
