-- ─────────────────────────────────────────────────────────────────────────────
-- Campaign Requests — Migration for Leaex v2 (Supabase)
-- Partners request campaigns via the Leaex platform UI.
-- Admins approve/reject via the admin panel, then start campaigns in wa_api.
-- This migration is for the Leaex v2 Supabase database, NOT the wa_api DB.
-- ─────────────────────────────────────────────────────────────────────────────

-- NOTE: This table goes in the Leaex v2 Supabase DB, not wa_api.

CREATE TABLE IF NOT EXISTS campaign_requests (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partner_id          UUID NOT NULL REFERENCES partners(id),  -- FK to Leaex partners table
    name                TEXT NOT NULL,
    template_name       TEXT NOT NULL,
    message_body        TEXT NOT NULL,
    variables           JSONB DEFAULT '[]',
    recipient_count     INTEGER NOT NULL DEFAULT 0,
    recipient_data      JSONB,                                  -- Array of phone numbers (encrypted)
    scheduled_at        TIMESTAMPTZ,                            -- Requested start time
    status              TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN (
                                'pending',       -- Awaiting admin review
                                'approved',      -- Approved, waiting for admin to start in wa_api
                                'rejected',      -- Rejected by admin
                                'started',       -- Started in wa_api
                                'completed'      -- Completed
                            )),
    rejection_reason    TEXT,
    approved_by         UUID REFERENCES auth.users(id),
    approved_at         TIMESTAMPTZ,
    wa_campaign_id      UUID,                                   -- Links to wa_campaigns.id after started
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- RLS: Partners can read/create their own requests. Admins can read/update all.
ALTER TABLE campaign_requests ENABLE ROW LEVEL SECURITY;

CREATE POLICY "partners_own_requests" ON campaign_requests
    FOR SELECT USING (partner_id = auth.uid());

CREATE POLICY "partners_create_requests" ON campaign_requests
    FOR INSERT WITH CHECK (partner_id = auth.uid());

CREATE POLICY "admin_manage_requests" ON campaign_requests
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM auth.users u
            WHERE u.id = auth.uid()
            AND u.raw_user_meta_data->>'role' = 'admin'
        )
    );

-- Index for admin dashboard
CREATE INDEX IF NOT EXISTS idx_campaign_requests_status ON campaign_requests(status);
CREATE INDEX IF NOT EXISTS idx_campaign_requests_partner ON campaign_requests(partner_id);
