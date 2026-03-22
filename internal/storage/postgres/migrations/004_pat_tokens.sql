-- 004_pat_tokens.sql adds the authoritative Personal Access Token table required by the v4.4 gateway PAT authentication model.
-- Scope: PAT token source-of-truth rows plus indexes that support tenant-, subject-, and rotation-oriented lookups.

BEGIN; -- Apply the PAT schema additions atomically so the gateway never sees a partially created PAT authority table.
CREATE TABLE IF NOT EXISTS pat_tokens (token_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, display_name TEXT NOT NULL, secret_hash TEXT NOT NULL, status TEXT NOT NULL, scopes_json BYTEA NOT NULL, expires_at TIMESTAMPTZ NULL, created_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL, revoked_at TIMESTAMPTZ NULL, revoked_by TEXT NULL, rotated_from TEXT NULL REFERENCES pat_tokens(token_id) ON DELETE SET NULL, last_used_at TIMESTAMPTZ NULL, last_used_ip TEXT NULL, metadata BYTEA NOT NULL); -- Store the authoritative PAT rows used by gateway bearer-token authentication together with lifecycle and audit-friendly metadata.
CREATE INDEX IF NOT EXISTS idx_pat_tokens_tenant_status_created_at ON pat_tokens (tenant_id, status, created_at DESC); -- Accelerate tenant-scoped PAT inventory and lifecycle review queries ordered by newest first.
CREATE INDEX IF NOT EXISTS idx_pat_tokens_subject_status_created_at ON pat_tokens (subject_id, status, created_at DESC); -- Accelerate subject-scoped PAT inventory and revocation review queries ordered by newest first.
CREATE INDEX IF NOT EXISTS idx_pat_tokens_rotated_from ON pat_tokens (rotated_from) WHERE rotated_from IS NOT NULL; -- Accelerate PAT rotation-chain inspection without bloating rows that are not part of a rotation lineage.
COMMIT; -- Persist the PAT schema only after the table and every supporting index have been created successfully.
