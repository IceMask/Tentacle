-- 002_audit_logs.sql adds the remaining audit-log table promised by the design document without rewriting the existing bootstrap migration.
-- Scope: append-only audit history plus indexes that support project-scoped and resource-scoped inspection.

BEGIN; -- Apply the audit-log schema additions atomically so operators never see a half-created audit table or index set.
CREATE TABLE IF NOT EXISTS audit_logs (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, session_id TEXT NULL REFERENCES sessions(id) ON DELETE SET NULL, trace_id TEXT NULL REFERENCES traces(id) ON DELETE SET NULL, actor TEXT NOT NULL, action TEXT NOT NULL, resource_type TEXT NOT NULL, resource_id TEXT NOT NULL, payload BYTEA NOT NULL, created_at TIMESTAMPTZ NOT NULL); -- Store append-only audit records for lifecycle, artifact, and authentication actions without coupling them to websocket delivery.
CREATE INDEX IF NOT EXISTS idx_audit_logs_project_created_at ON audit_logs (project_id, created_at DESC); -- Accelerate project-scoped audit review and export queries ordered by newest first.
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource_created_at ON audit_logs (resource_type, resource_id, created_at DESC); -- Accelerate lookups for all audit entries tied to a specific session, trace, artifact, or auth subject.
CREATE INDEX IF NOT EXISTS idx_audit_logs_trace_created_at ON audit_logs (trace_id, created_at DESC) WHERE trace_id IS NOT NULL; -- Accelerate trace-scoped audit investigation without bloating rows that do not reference a trace.
COMMIT; -- Persist the audit schema only after the table and every supporting index are in place successfully.
