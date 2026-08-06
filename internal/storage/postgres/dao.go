package postgres

import (
	"context"
	stdErrors "errors"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DAO struct {
	pool *pgxpool.Pool
}

// NewDAO executes this operation.
func NewDAO(ctx context.Context, cfg config.PostgresConfig) (*DAO, error) {
	if cfg.DSN == "" {
		return nil, errors.New(errors.CodeConfigMissing, "postgres DSN is empty")
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, errors.Wrap(errors.CodeConfigInvalid, "invalid postgres DSN", err)
	}

	poolConfig.MaxConns = int32(cfg.MaxOpenConns)
	poolConfig.MinConns = int32(cfg.MaxIdleConns)
	poolConfig.MaxConnLifetime = cfg.ConnMaxLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.Wrap(errors.CodeStoreConn, "failed to connect to postgres", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, errors.Wrap(errors.CodeStoreConn, "failed to ping postgres", err)
	}

	return &DAO{pool: pool}, nil
}

// Close executes this operation.
func (d *DAO) Close() {
	d.pool.Close()
}

// Ping executes this operation.
func (d *DAO) Ping(ctx context.Context) error {
	return d.pool.Ping(ctx)
}

// --- Sessions ---

// CreateSession executes this operation.
func (d *DAO) CreateSession(ctx context.Context, s *Session) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO sessions (id, project_id, tenant_id, subject_id, status, capabilities, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, s.ID, s.ProjectID, s.TenantID, s.SubjectID, s.Status, s.Capabilities, s.CreatedAt, s.UpdatedAt) // Persist the optional tenant and subject ownership metadata together with the session so later resource access can validate ownership.
	return err
}

// GetSession executes this operation.
func (d *DAO) GetSession(ctx context.Context, id string) (*Session, error) {
	s := &Session{}
	err := d.pool.QueryRow(ctx, `
		SELECT id, project_id, tenant_id, subject_id, status, capabilities, created_at, updated_at, ended_at
		FROM sessions WHERE id = $1
	`, id).Scan(&s.ID, &s.ProjectID, &s.TenantID, &s.SubjectID, &s.Status, &s.Capabilities, &s.CreatedAt, &s.UpdatedAt, &s.EndedAt) // Load the persisted ownership metadata together with the session lifecycle fields so callers can authorize access.
	if stdErrors.Is(err, pgx.ErrNoRows) { // Map only an authoritative empty result to the public session-not-found contract.
		return nil, errors.Wrap(errors.CodeSessionNotFound, "session not found", err) // Preserve the no-row cause for server logs under the stable lookup code.
	}
	if err != nil { // Keep connection, decoding, and PostgreSQL execution failures distinct from absence.
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to read session", err) // Surface infrastructure failures through the retryable storage-read contract.
	}
	return s, nil
}

// EndSession executes this operation.
func (d *DAO) EndSession(ctx context.Context, id string, endedAt time.Time) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE sessions
		SET status = 'ended', ended_at = $2, updated_at = $2
		WHERE id = $1
	`, id, endedAt)
	return err
}

// --- Traces ---

// CreateTrace executes this operation.
func (d *DAO) CreateTrace(ctx context.Context, t *Trace) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO traces (id, session_id, project_id, tenant_id, subject_id, status, current_attempt, terminal_reason, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, t.ID, t.SessionID, t.ProjectID, t.TenantID, t.SubjectID, t.Status, t.CurrentAttempt, t.TerminalReason, t.CreatedAt, t.UpdatedAt) // Persist the trace together with inherited ownership metadata plus its distributed-attempt and terminal-reason state so callback guards and resource access checks share one source of truth.
	return err // Return the raw insert result so higher layers preserve the existing repository error contract.
}

// CreateOrRearmQueuedTrace inserts a new pending trace or atomically rearms the same owned trace after a confirmed queue-enqueue failure.
func (d *DAO) CreateOrRearmQueuedTrace(ctx context.Context, t *Trace) error {
	var preparedTraceID string // Hold the row returned only when PostgreSQL inserted or safely rearmed the requested trace.
	err := d.pool.QueryRow(ctx, `
		WITH prepared AS (
			INSERT INTO traces (id, session_id, project_id, tenant_id, subject_id, status, current_attempt, terminal_reason, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, 'pending', 0, NULL, $6, $7)
			ON CONFLICT (id) DO UPDATE
			SET status = 'pending', current_attempt = 0, terminal_reason = NULL, updated_at = EXCLUDED.updated_at
			WHERE traces.status = 'failed'
			  AND traces.terminal_reason = 'queue_enqueue_failed'
			  AND traces.session_id = EXCLUDED.session_id
			  AND traces.project_id = EXCLUDED.project_id
			  AND traces.tenant_id IS NOT DISTINCT FROM EXCLUDED.tenant_id
			  AND traces.subject_id IS NOT DISTINCT FROM EXCLUDED.subject_id
			RETURNING id
		), cleared AS (
			DELETE FROM plan_events WHERE trace_id IN (SELECT id FROM prepared)
			RETURNING trace_id
		)
		SELECT id FROM prepared
	`, t.ID, t.SessionID, t.ProjectID, t.TenantID, t.SubjectID, t.CreatedAt, t.UpdatedAt).Scan(&preparedTraceID) // Insert or rearm and clear the prior enqueue-failure marker inside one PostgreSQL statement.
	if stdErrors.Is(err, pgx.ErrNoRows) { // Treat a conflicting active, terminal, differently owned, or differently scoped trace as an idempotency conflict.
		return errors.New(errors.CodeStateConflict, "trace id is already in use") // Reject unsafe trace reuse without exposing the existing row's ownership or lifecycle.
	}
	if err != nil { // Wrap genuine query and write failures separately from the expected conflict path.
		return errors.Wrap(errors.CodeStoreWrite, "failed to create or rearm queued trace", err) // Preserve the PostgreSQL cause for server logs under a stable storage code.
	}
	return nil // Confirm that the trace is pending with no stale enqueue-failure event before Redis enqueue begins.
}

// GetTrace executes this operation.
func (d *DAO) GetTrace(ctx context.Context, id string) (*Trace, error) {
	t := &Trace{} // Allocate the destination trace model before scanning the persisted lifecycle and attempt state.
	err := d.pool.QueryRow(ctx, `
		SELECT id, session_id, project_id, tenant_id, subject_id, status, current_attempt, terminal_reason, created_at, updated_at
		FROM traces WHERE id = $1
	`, id).Scan(&t.ID, &t.SessionID, &t.ProjectID, &t.TenantID, &t.SubjectID, &t.Status, &t.CurrentAttempt, &t.TerminalReason, &t.CreatedAt, &t.UpdatedAt) // Load the persisted trace state, ownership metadata, current attempt, and terminal reason in one query for callers.
	if stdErrors.Is(err, pgx.ErrNoRows) { // Map only an authoritative empty result to the public trace-not-found contract.
		return nil, errors.Wrap(errors.CodeTraceNotFound, "trace not found", err) // Preserve the no-row cause for server logs under the stable lookup code.
	}
	if err != nil { // Keep connection, decoding, and PostgreSQL execution failures distinct from absence.
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to read trace", err) // Surface infrastructure failures through the retryable storage-read contract.
	}
	return t, nil // Return the fully populated trace model once the database scan succeeds.
}

// UpdateTraceStatus executes this operation.
func (d *DAO) UpdateTraceStatus(ctx context.Context, id string, status string) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE traces
		SET status = $2, updated_at = NOW()
		WHERE id = $1
	`, id, status)
	return err
}

// --- Events ---

// InsertPlanEvent executes this operation.
func (d *DAO) InsertPlanEvent(ctx context.Context, e *PlanEvent) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO plan_events (trace_id, seq, step_index, status, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, e.TraceID, e.Seq, e.StepIndex, e.Status, e.Payload, e.CreatedAt)
	return err
}

// ListEvents executes this operation.
func (d *DAO) ListEvents(ctx context.Context, traceID string, sinceSeq int64, limit int) ([]*PlanEvent, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT trace_id, seq, step_index, status, payload, created_at
		FROM plan_events
		WHERE trace_id = $1 AND seq > $2
		ORDER BY seq ASC
		LIMIT $3
	`, traceID, sinceSeq, limit)
	if err != nil {
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to list plan events", err) // Classify query startup failures as storage reads rather than leaking raw driver errors.
	}
	defer rows.Close()

	var events []*PlanEvent
	for rows.Next() {
		e := &PlanEvent{}
		if err := rows.Scan(&e.TraceID, &e.Seq, &e.StepIndex, &e.Status, &e.Payload, &e.CreatedAt); err != nil {
			return nil, errors.Wrap(errors.CodeStoreRead, "failed to scan plan event", err) // Preserve row decoding failures through the stable storage-read contract.
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil { // Detect connection and protocol failures that occur after iteration begins.
		return nil, errors.Wrap(errors.CodeStoreRead, "failed while iterating plan events", err) // Refuse to return a silently truncated event history.
	}
	return events, nil
}

// --- Artifacts ---

// InsertArtifact executes this operation.
func (d *DAO) InsertArtifact(ctx context.Context, a *Artifact) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO artifacts (id, trace_id, key, type, size, created_at, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, a.ID, a.TraceID, a.Key, a.Type, a.Size, a.CreatedAt, a.Metadata)
	if err != nil {
		// Check for duplicate key violation if needed
		return err
	}
	return nil
}

// ListArtifacts executes this operation.
func (d *DAO) ListArtifacts(ctx context.Context, traceID string) ([]*Artifact, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT id, trace_id, key, type, size, created_at, metadata
		FROM artifacts
		WHERE trace_id = $1
		ORDER BY created_at ASC
	`, traceID)
	if err != nil {
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to list artifacts", err) // Classify query startup failures as storage reads rather than leaking raw driver errors.
	}
	defer rows.Close()

	var artifacts []*Artifact
	for rows.Next() {
		a := &Artifact{}
		if err := rows.Scan(&a.ID, &a.TraceID, &a.Key, &a.Type, &a.Size, &a.CreatedAt, &a.Metadata); err != nil {
			return nil, errors.Wrap(errors.CodeStoreRead, "failed to scan artifact", err) // Preserve row decoding failures through the stable storage-read contract.
		}
		artifacts = append(artifacts, a)
	}
	if err := rows.Err(); err != nil { // Detect connection and protocol failures that occur after iteration begins.
		return nil, errors.Wrap(errors.CodeStoreRead, "failed while iterating artifacts", err) // Refuse to return a silently truncated artifact list.
	}
	return artifacts, nil
}
