-- ─────────────────────────────────────────────────────────────────────────────
-- Leaex WhatsApp Engine — Initial Schema (Supabase/Postgres)
-- ─────────────────────────────────────────────────────────────────────────────

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─── agencies ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agencies (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 TEXT NOT NULL,
    api_key              TEXT UNIQUE NOT NULL,  -- bcrypt hash of the real key
    subscription_status  TEXT NOT NULL DEFAULT 'active'
                             CHECK (subscription_status IN ('active','suspended','trial')),
    plan_tier            TEXT NOT NULL DEFAULT 'basic'
                             CHECK (plan_tier IN ('basic','pro','enterprise')),
    daily_msg_limit      INTEGER NOT NULL DEFAULT 200,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── tenants (salon partners) ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tenants (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agency_id         UUID NOT NULL REFERENCES agencies(id),
    partner_id        UUID,                     -- links to Leaex partners table
    instance_name     TEXT UNIQUE NOT NULL,     -- evo API instance identifier
    wa_number         TEXT,                     -- +91XXXXXXXXXX
    instance_status   TEXT NOT NULL DEFAULT 'disconnected'
                          CHECK (instance_status IN
                            ('active','qr_required','disconnected','banned','suspended')),
    daily_crm_limit   INTEGER NOT NULL DEFAULT 200,
    campaign_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tenants_agency ON tenants(agency_id);

-- ─── wa_campaigns ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wa_campaigns (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    name                TEXT NOT NULL,
    template_name       TEXT,
    template_hash       TEXT,
    status              TEXT NOT NULL DEFAULT 'draft'
                            CHECK (status IN ('draft','running','paused','completed','cancelled')),
    total_recipients    INTEGER NOT NULL DEFAULT 0,
    sent_count          INTEGER NOT NULL DEFAULT 0,
    delivered_count     INTEGER NOT NULL DEFAULT 0,
    failed_count        INTEGER NOT NULL DEFAULT 0,
    deferred_count      INTEGER NOT NULL DEFAULT 0,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    pool_rotation_ids   TEXT[],   -- ADMIN ONLY: not exposed in partner API
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_campaigns_tenant ON wa_campaigns(tenant_id);
CREATE INDEX IF NOT EXISTS idx_campaigns_status ON wa_campaigns(status);

-- ─── wa_interaction_log ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wa_interaction_log (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               UUID NOT NULL REFERENCES tenants(id),
    campaign_id             UUID REFERENCES wa_campaigns(id),
    message_type            TEXT NOT NULL
                                CHECK (message_type IN
                                  ('campaign','booking_confirm','reminder','birthday',
                                   'anniversary','manual_crm','re_engagement')),
    recipient_phone_hash    TEXT NOT NULL,   -- sha256(phone), for analytics/joins
    recipient_phone         TEXT NOT NULL,   -- Full PII number stored securely
    recipient_name          TEXT,            -- Full customer name
    instance_used           TEXT NOT NULL,   -- "leaex_pool" for campaigns
    status                  TEXT NOT NULL DEFAULT 'pending'
                                CHECK (status IN
                                  ('pending','sent','delivered','read','failed',
                                   'deferred_spam','blocked_optout','duplicate','expired_dlq')),
    evo_msg_id        TEXT,
    error_reason            TEXT,
    retry_count             SMALLINT NOT NULL DEFAULT 0,
    scheduled_at            TIMESTAMPTZ NOT NULL,
    sent_at                 TIMESTAMPTZ,
    delivered_at            TIMESTAMPTZ,
    idempotency_key         TEXT UNIQUE NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_interaction_tenant    ON wa_interaction_log(tenant_id);
CREATE INDEX IF NOT EXISTS idx_interaction_campaign  ON wa_interaction_log(campaign_id);
CREATE INDEX IF NOT EXISTS idx_interaction_status    ON wa_interaction_log(status);
CREATE INDEX IF NOT EXISTS idx_interaction_sent_at   ON wa_interaction_log(sent_at DESC);

-- ─── wa_customer_consent ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS wa_customer_consent (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    phone_hash          TEXT NOT NULL,           -- sha256(phone)
    tenant_id           UUID REFERENCES tenants(id),  -- NULL = platform-wide opt-out
    opted_out           BOOLEAN NOT NULL DEFAULT FALSE,
    opted_out_at        TIMESTAMPTZ,
    opted_out_source    TEXT CHECK (opted_out_source IN
                          ('stop_keyword','partner_crm','customer_request','admin')),
    opted_in_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (phone_hash, tenant_id)
);
CREATE INDEX IF NOT EXISTS idx_consent_phone ON wa_customer_consent(phone_hash);

-- ─── instance_health_log (admin-only) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS instance_health_log (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_name    TEXT NOT NULL,
    tenant_id        UUID REFERENCES tenants(id),
    is_pool          BOOLEAN NOT NULL DEFAULT FALSE,
    event_type       TEXT NOT NULL
                         CHECK (event_type IN
                           ('connected','disconnected','qr_required','banned',
                            'health_check_ok','health_check_fail','connection_state_change')),
    previous_status  TEXT NOT NULL,
    new_status       TEXT NOT NULL,
    detail           JSONB,
    logged_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_health_log_instance ON instance_health_log(instance_name, logged_at DESC);

-- ─── rate_limit_events (admin-only) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rate_limit_events (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID REFERENCES tenants(id),
    instance_name       TEXT,
    event_type          TEXT NOT NULL
                            CHECK (event_type IN
                              ('daily_limit_reached','spam_guard_triggered',
                               'adaptive_delay_increased','failure_spike')),
    msg_count_at_event  INTEGER,
    detail              TEXT,
    logged_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── pool_number_stats (admin-only) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pool_number_stats (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_name   TEXT UNIQUE NOT NULL,
    state           TEXT NOT NULL DEFAULT 'warming'
                        CHECK (state IN ('warming','active','cooling','flagged','resting','retired')),
    daily_sent      INTEGER NOT NULL DEFAULT 0,
    daily_limit     INTEGER NOT NULL DEFAULT 20,
    delivery_rate   NUMERIC(5,4) DEFAULT 0,
    last_used       TIMESTAMPTZ,
    warmup_day      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- RLS Policies
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE wa_interaction_log   ENABLE ROW LEVEL SECURITY;
ALTER TABLE wa_campaigns         ENABLE ROW LEVEL SECURITY;
ALTER TABLE wa_customer_consent  ENABLE ROW LEVEL SECURITY;
ALTER TABLE pool_number_stats    ENABLE ROW LEVEL SECURITY;

-- Partners see only their own interaction records
CREATE POLICY "tenant_isolation_interactions"
ON wa_interaction_log FOR SELECT
USING (tenant_id::TEXT = auth.jwt() ->> 'tenant_id');

-- Partners see only their own campaigns
CREATE POLICY "tenant_isolation_campaigns"
ON wa_campaigns FOR SELECT
USING (tenant_id::TEXT = auth.jwt() ->> 'tenant_id');

-- Partners see their own consent + platform-wide opt-outs (tenant_id IS NULL)
CREATE POLICY "tenant_isolation_consent"
ON wa_customer_consent FOR SELECT
USING (
    tenant_id::TEXT = auth.jwt() ->> 'tenant_id'
    OR tenant_id IS NULL
);

-- Pool stats: admin only (service_role bypasses all RLS)
CREATE POLICY "admin_only_pool_stats"
ON pool_number_stats FOR SELECT
USING (auth.role() = 'service_role');

-- ─────────────────────────────────────────────────────────────────────────────
-- Seed: Default admin agency (update api_key hash after deploy)
-- ─────────────────────────────────────────────────────────────────────────────
-- The api_key stored here is the bcrypt hash of the raw key.
-- Raw key for dev: "leaex-dev-api-key-change-in-prod"
-- Generate a real hash via: SELECT crypt('your-key', gen_salt('bf'));
INSERT INTO agencies (name, api_key, subscription_status, plan_tier, daily_msg_limit)
VALUES (
    'Leaex Platform',
    '$2a$10$placeholder.replace.with.real.bcrypt.hash.of.your.api.key',
    'active',
    'enterprise',
    10000
)
ON CONFLICT (api_key) DO NOTHING;
