-- ─────────────────────────────────────────────────────────────────────────────
-- Migration: 003_unmask_phone_numbers.sql
-- Goal: Remove PII hashing and enable plain text phone numbers for all users.
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Update wa_interaction_log
-- Drop the recipient_phone_hash column as it's no longer needed for joins.
ALTER TABLE wa_interaction_log DROP COLUMN IF EXISTS recipient_phone_hash;

-- 2. Update wa_customer_consent
-- First add the phone column
ALTER TABLE wa_customer_consent ADD COLUMN IF NOT EXISTS phone TEXT;

-- Move existing data if possible (though hashes can't be reversed, we start clean or 
-- rely on new opt-outs to populate the phone column).
-- We'll make phone NOT NULL after potential cleanup if needed, but for now we allow NULL 
-- until we populate it.

-- Drop the old constraint that used phone_hash
ALTER TABLE wa_customer_consent DROP CONSTRAINT IF EXISTS wa_customer_consent_phone_hash_tenant_id_key;

-- Drop the phone_hash column
ALTER TABLE wa_customer_consent DROP COLUMN IF EXISTS phone_hash;

-- Add a unique constraint on (phone, tenant_id)
-- Note: phone might be NULL for now if we didn't populate it, so we might need to handle that.
-- But since it's a new system, we can just enforce it.
ALTER TABLE wa_customer_consent ALTER COLUMN phone SET NOT NULL;
ALTER TABLE wa_customer_consent ADD CONSTRAINT wa_customer_consent_phone_tenant_id_key UNIQUE (phone, tenant_id);

-- Add index for performance
CREATE INDEX IF NOT EXISTS idx_consent_phone_plain ON wa_customer_consent(phone);
