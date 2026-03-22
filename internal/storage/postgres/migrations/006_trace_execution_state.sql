-- 006_trace_execution_state.sql extends traces with persisted distributed-attempt and terminal-reason state.
-- Scope: add current_attempt and terminal_reason columns plus the supporting index needed by distributed ownership and stale-result protection.

BEGIN; -- Apply the trace execution-state extension atomically so distributed ownership metadata never lands half-created.
ALTER TABLE traces ADD COLUMN IF NOT EXISTS current_attempt INTEGER NOT NULL DEFAULT 0; -- Persist the current distributed execution attempt so late worker callbacks can be rejected deterministically.
ALTER TABLE traces ADD COLUMN IF NOT EXISTS terminal_reason TEXT NULL; -- Persist the terminal reason so operators and clients can distinguish timeout, orphaned, cancelled, and normal completion outcomes.
CREATE INDEX IF NOT EXISTS idx_traces_project_status_attempt_updated_at ON traces (project_id, status, current_attempt, updated_at DESC); -- Accelerate project-scoped trace monitoring that filters by lifecycle state and current attempt.
COMMIT; -- Persist the trace execution-state extension only after both columns and the supporting index succeed together.
