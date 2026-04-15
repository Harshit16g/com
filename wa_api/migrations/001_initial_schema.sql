-- ─────────────────────────────────────────────────────────────────────────────
-- Leaex WhatsApp Engine — Schema v3.1 (April 2026)
-- Direct Postgres — no Supabase RLS dependency
-- Agency model removed: each agency gets its own wa_api deployment.
-- ─────────────────────────────────────────────────────────────────────────────

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─── tenants (partner instances) ──────────────────────────────────────────────
-- Each row = one WhatsApp instance for one Leaex partner.
-- partner_id links to the Leaex v2 platform's partner table.
CREATE TABLE IF NOT EXISTS tenants (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partner_id        UUID UNIQUE,                -- links to Leaex partners table (1:1 mapping)
    instance_name     TEXT UNIQUE NOT NULL,        -- evo API instance identifier (e.g. "wa_glamour_studio_01")
    wa_number         TEXT,                        -- +91XXXXXXXXXX of the connected WhatsApp number
    instance_status   TEXT NOT NULL DEFAULT 'disconnected'
                          CHECK (instance_status IN
                            ('active','qr_required','disconnected','banned','suspended')),
    daily_crm_limit   INTEGER NOT NULL DEFAULT 200,
    campaign_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
-- Central log for ALL WhatsApp interactions (inbound + outbound).
-- direction + intent columns provide the interaction taxonomy.
CREATE TABLE IF NOT EXISTS wa_interaction_log (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               UUID NOT NULL REFERENCES tenants(id),
    campaign_id             UUID REFERENCES wa_campaigns(id),

    -- Direction: who initiated the interaction
    direction               TEXT NOT NULL DEFAULT 'outbound'
                                CHECK (direction IN ('inbound', 'outbound')),

    -- Message type: specific automation/trigger type
    message_type            TEXT NOT NULL
                                CHECK (message_type IN (
                                    -- Outbound types
                                    'campaign', 'booking_confirm', 'reminder', 'birthday',
                                    'anniversary', 'manual_crm', 're_engagement',
                                    -- Inbound types
                                    'inbound',
                                    -- General
                                    'outbound', 'feedback', 'complaint'
                                )),

    -- Intent: business purpose of the interaction (nullable for legacy data)
    intent                  TEXT
                                CHECK (intent IS NULL OR intent IN (
                                    'support', 'sales', 'marketing', 'feedback',
                                    'transactional', 're_engagement', 'general'
                                )),

    recipient_phone_hash    TEXT NOT NULL,   -- sha256(phone), for analytics/joins
    recipient_phone         TEXT NOT NULL,   -- Full PII number (to be migrated to Supabase)
    recipient_name          TEXT,            -- Full customer name
    instance_used           TEXT NOT NULL,   -- "leaex_pool" for campaigns
    status                  TEXT NOT NULL DEFAULT 'pending'
                                CHECK (status IN (
                                    'pending', 'sent', 'delivered', 'read', 'failed',
                                    'deferred_spam', 'blocked_optout', 'duplicate', 'expired_dlq'
                                )),
    evo_msg_id              TEXT,
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
CREATE INDEX IF NOT EXISTS idx_interaction_direction ON wa_interaction_log(direction);

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
                         CHECK (event_type IN (
                             'connected', 'disconnected', 'qr_required', 'banned',
                             'health_check_ok', 'health_check_fail',
                             'connection_state_change', 'admin_force_qr'
                         )),
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
                            CHECK (event_type IN (
                                'daily_limit_reached', 'spam_guard_triggered',
                                'adaptive_delay_increased', 'failure_spike'
                            )),
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

-- ─── wa_contacts ─────────────────────────────────────────────────────────────
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

-- ─── RPC Functions ──────────────────────────────────────────────────────────

-- R10: Atomic campaign counter updates
CREATE OR REPLACE FUNCTION increment_campaign_counters(
  p_campaign_id UUID, p_sent INT, p_delivered INT, p_failed INT
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  UPDATE wa_campaigns SET
    sent_count      = sent_count + p_sent,
    delivered_count = delivered_count + p_delivered,
    failed_count    = failed_count + p_failed,
    updated_at      = NOW()
  WHERE id = p_campaign_id;
END;
$$;
