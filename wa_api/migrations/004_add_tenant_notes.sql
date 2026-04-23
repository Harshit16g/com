-- Migration 004: Add tenant notes and status for cleanup auditing
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS cleanup_notes TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

COMMENT ON COLUMN tenants.cleanup_notes IS 'Notes describing why a tenant instance was cleaned up or marked inactive';
