-- 005_hmac_keys.sql adds the authoritative HMAC key registry required by the v4.4 gateway signed-request authentication model.
-- Scope: HMAC key source-of-truth rows plus indexes that support tenant-, status-, and validity-window-oriented lookups.

BEGIN; -- Apply the HMAC key-registry schema additions atomically so the gateway never sees a partially created HMAC authority table.
CREATE TABLE IF NOT EXISTS hmac_keys (key_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, display_name TEXT NOT NULL, secret_ref TEXT NOT NULL, status TEXT NOT NULL, not_before TIMESTAMPTZ NULL, not_after TIMESTAMPTZ NULL, created_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL, revoked_at TIMESTAMPTZ NULL, metadata BYTEA NOT NULL); -- Store the authoritative HMAC keys used by gateway signed-request authentication together with lifecycle metadata and validity windows.
CREATE INDEX IF NOT EXISTS idx_hmac_keys_tenant_status_created_at ON hmac_keys (tenant_id, status, created_at DESC); -- Accelerate tenant-scoped HMAC key inventory and lifecycle review queries ordered by newest first.
CREATE INDEX IF NOT EXISTS idx_hmac_keys_tenant_validity ON hmac_keys (tenant_id, not_before, not_after); -- Accelerate tenant-scoped validity-window scans used by operations and key-rotation review tooling.
COMMIT; -- Persist the HMAC key-registry schema only after the table and every supporting index have been created successfully.
