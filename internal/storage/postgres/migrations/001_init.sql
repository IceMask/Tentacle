-- 001_init.sql creates the PostgreSQL schema required by the current DAO implementation.
-- Scope: sessions, traces, plan_events, and artifacts tables plus the supporting foreign keys and indexes used by repository queries.

BEGIN; -- Apply the DAO-backed schema as one atomic unit so startup never sees a partially created database layout.
CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, status TEXT NOT NULL, capabilities BYTEA NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ NULL); -- Store orchestrator session records together with the serialized capability payload returned at session creation time.
CREATE INDEX IF NOT EXISTS idx_sessions_project_created_at ON sessions (project_id, created_at DESC); -- Accelerate future project-scoped session listing and operational inspection queries.
CREATE INDEX IF NOT EXISTS idx_sessions_status_updated_at ON sessions (status, updated_at DESC); -- Accelerate health, cleanup, and state-oriented session lookups by lifecycle status.
CREATE TABLE IF NOT EXISTS traces (id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE, project_id TEXT NOT NULL, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL); -- Store execution traces that belong to a logical session and carry lifecycle status across plan execution.
CREATE INDEX IF NOT EXISTS idx_traces_session_created_at ON traces (session_id, created_at DESC); -- Accelerate trace retrieval paths that start from a known session identifier.
CREATE INDEX IF NOT EXISTS idx_traces_project_status_updated_at ON traces (project_id, status, updated_at DESC); -- Accelerate project-scoped monitoring and trace-state dashboards that filter by status.
CREATE TABLE IF NOT EXISTS plan_events (trace_id TEXT NOT NULL REFERENCES traces(id) ON DELETE CASCADE, seq BIGINT NOT NULL, step_index INTEGER NOT NULL, status TEXT NOT NULL, payload BYTEA NOT NULL, created_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (trace_id, seq)); -- Store ordered per-trace execution events exactly as emitted by the orchestrator publisher.
CREATE INDEX IF NOT EXISTS idx_plan_events_trace_created_at ON plan_events (trace_id, created_at ASC); -- Accelerate trace event replay ordered by creation time for long-running execution histories.
CREATE TABLE IF NOT EXISTS artifacts (id TEXT PRIMARY KEY, trace_id TEXT NOT NULL REFERENCES traces(id) ON DELETE CASCADE, key TEXT NOT NULL, type TEXT NOT NULL, size BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, metadata BYTEA NOT NULL); -- Store artifact metadata for screenshots and other binary objects whose bytes live in S3-compatible object storage.
CREATE INDEX IF NOT EXISTS idx_artifacts_trace_created_at ON artifacts (trace_id, created_at ASC); -- Accelerate artifact listing for a trace in the same order currently used by the DAO.
CREATE UNIQUE INDEX IF NOT EXISTS idx_artifacts_key_unique ON artifacts (key); -- Prevent duplicate artifact metadata rows from pointing at the same object storage key.
COMMIT; -- Persist the full schema only after every required table and index has been created successfully.
