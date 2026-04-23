-- Migration 005: Enable fuzzy search for contacts
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_wa_contacts_name_trgm ON wa_contacts USING gin (name gin_trgm_ops);
