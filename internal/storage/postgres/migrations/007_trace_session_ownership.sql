-- 007_trace_session_ownership.sql extends sessions and traces with persisted tenant and subject ownership metadata.
-- Scope: add tenant_id and subject_id columns plus supporting indexes so authenticated resource access can validate ownership before exposing session or trace data.

BEGIN; -- Apply the session and trace ownership extension atomically so callers never observe partially upgraded resource-authorization metadata.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tenant_id TEXT NULL; -- Persist the authenticated tenant identifier that created the session so later session-scoped reads can validate tenant ownership.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS subject_id TEXT NULL; -- Persist the authenticated subject identifier that created the session so later session-scoped actions can validate exact ownership.
ALTER TABLE traces ADD COLUMN IF NOT EXISTS tenant_id TEXT NULL; -- Persist the inherited tenant identifier for each trace so trace-scoped reads and WebSocket subscriptions can validate tenant ownership.
ALTER TABLE traces ADD COLUMN IF NOT EXISTS subject_id TEXT NULL; -- Persist the inherited subject identifier for each trace so trace-scoped reads and WebSocket subscriptions can validate exact ownership.
CREATE INDEX IF NOT EXISTS idx_sessions_tenant_subject_created_at ON sessions (tenant_id, subject_id, created_at DESC); -- Accelerate future tenant-scoped and subject-scoped session lookups used by authorization and audit flows.
CREATE INDEX IF NOT EXISTS idx_traces_tenant_subject_created_at ON traces (tenant_id, subject_id, created_at DESC); -- Accelerate future tenant-scoped and subject-scoped trace lookups used by authorization and audit flows.
COMMIT; -- Persist the ownership columns and indexes only after every schema change succeeds together.
