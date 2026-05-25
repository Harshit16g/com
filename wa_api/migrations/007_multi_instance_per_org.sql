-- ─────────────────────────────────────────────────────────────────────────────
-- Migration 007: Allow multiple tenants per partner (org)
-- One org can have multiple WhatsApp instances — one per branch.
-- ─────────────────────────────────────────────────────────────────────────────

-- Drop the UNIQUE constraint on partner_id (was 1:1 org ↔ instance)
ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_partner_id_key;

-- Add index for partner_id lookups (non-unique now to allow multiple per org)
CREATE INDEX IF NOT EXISTS idx_tenants_partner_id ON tenants(partner_id);

-- instance_name remains UNIQUE — each evo instance name must be globally unique
-- (enforced by the existing UNIQUE constraint on instance_name)
