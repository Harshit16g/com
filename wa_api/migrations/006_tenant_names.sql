-- Migration 006: Add business and partner name fields to tenants
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS business_name TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS partner_name TEXT;

COMMENT ON COLUMN tenants.business_name IS 'Public business name used in Lora greetings (e.g. Acme Spa)';
COMMENT ON COLUMN tenants.partner_name IS 'Individual partner/owner name used in Lora redirects (e.g. John Doe)';
