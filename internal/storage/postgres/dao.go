package postgres

import (
	"context"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"

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
		INSERT INTO sessions (id, project_id, status, capabilities, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, s.ID, s.ProjectID, s.Status, s.Capabilities, s.CreatedAt, s.UpdatedAt)
	return err
}

// GetSession executes this operation.
func (d *DAO) GetSession(ctx context.Context, id string) (*Session, error) {
	s := &Session{}
	err := d.pool.QueryRow(ctx, `
		SELECT id, project_id, status, capabilities, created_at, updated_at, ended_at
		FROM sessions WHERE id = $1
	`, id).Scan(&s.ID, &s.ProjectID, &s.Status, &s.Capabilities, &s.CreatedAt, &s.UpdatedAt, &s.EndedAt)
	if err != nil {
		return nil, errors.Wrap(errors.CodeSessionNotFound, "session not found", err)
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
		INSERT INTO traces (id, session_id, project_id, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, t.ID, t.SessionID, t.ProjectID, t.Status, t.CreatedAt, t.UpdatedAt)
	return err
}

// GetTrace executes this operation.
func (d *DAO) GetTrace(ctx context.Context, id string) (*Trace, error) {
	t := &Trace{}
	err := d.pool.QueryRow(ctx, `
		SELECT id, session_id, project_id, status, created_at, updated_at
		FROM traces WHERE id = $1
	`, id).Scan(&t.ID, &t.SessionID, &t.ProjectID, &t.Status, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, errors.Wrap(errors.CodeTraceNotFound, "trace not found", err)
	}
	return t, nil
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
		return nil, err
	}
	defer rows.Close()

	var events []*PlanEvent
	for rows.Next() {
		e := &PlanEvent{}
		if err := rows.Scan(&e.TraceID, &e.Seq, &e.StepIndex, &e.Status, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
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
		return nil, err
	}
	defer rows.Close()

	var artifacts []*Artifact
	for rows.Next() {
		a := &Artifact{}
		if err := rows.Scan(&a.ID, &a.TraceID, &a.Key, &a.Type, &a.Size, &a.CreatedAt, &a.Metadata); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, a)
	}
	return artifacts, nil
}
