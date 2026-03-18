// coordination.go provides transactional, locking, optimistic state-transition, and audit-log helpers for the PostgreSQL persistence layer.
package postgres

import (
	"context"
	"time"

	"mcp_for_appium/internal/errors"

	"github.com/jackc/pgx/v5"
)

// TxDAO exposes the subset of DAO operations that must run inside a PostgreSQL transaction.
type TxDAO struct {
	tx pgx.Tx
}

// AuditLog stores an append-only audit record for a session, trace, artifact, or authentication event.
type AuditLog struct {
	ID           string
	ProjectID    string
	SessionID    string
	TraceID      string
	Actor        string
	Action       string
	ResourceType string
	ResourceID   string
	Payload      []byte
	CreatedAt    time.Time
}

// WithTx executes fn inside a PostgreSQL transaction and rolls back automatically when fn or commit fails.
func (d *DAO) WithTx(ctx context.Context, fn func(*TxDAO) error) error {
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{}) // Start a transaction so callers can coordinate multiple row reads and writes atomically.
	if err != nil {                                 // Stop immediately when PostgreSQL cannot create the transaction context.
		return errors.Wrap(errors.CodeStoreConn, "failed to begin postgres transaction", err) // Surface transaction bootstrap failures through the storage-layer error contract.
	}

	committed := false // Track commit completion so the deferred rollback only runs for unfinished transactions.
	defer func() {
		if !committed { // Roll back every unfinished transaction to release locks and discard partial writes.
			_ = tx.Rollback(ctx) // Ignore rollback errors because the original failure path already determines the caller-visible result.
		}
	}()

	if err := fn(&TxDAO{tx: tx}); err != nil { // Execute the caller-supplied unit of work inside the open transaction.
		return err // Propagate the callback failure so the deferred rollback keeps the database unchanged.
	}

	if err := tx.Commit(ctx); err != nil { // Commit only after the callback has finished all transactional writes successfully.
		return errors.Wrap(errors.CodeStoreWrite, "failed to commit postgres transaction", err) // Report commit failures as write errors because the transaction body already completed.
	}

	committed = true // Mark the transaction as durable so the deferred rollback does not execute.
	return nil       // Return success after PostgreSQL has durably committed the transaction.
}

// GetSessionForUpdate loads a session row and acquires a row-level lock that remains held until the surrounding transaction completes.
func (d *TxDAO) GetSessionForUpdate(ctx context.Context, id string) (*Session, error) {
	session := &Session{} // Allocate the destination struct before scanning the locked row.
	err := d.tx.QueryRow(ctx, `
		SELECT id, project_id, status, capabilities, created_at, updated_at, ended_at
		FROM sessions
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&session.ID, &session.ProjectID, &session.Status, &session.Capabilities, &session.CreatedAt, &session.UpdatedAt, &session.EndedAt) // Lock the session row so concurrent end-session calls serialize on the same record.
	if err != nil { // Convert missing-row and query failures into the repository's session-not-found contract.
		return nil, errors.Wrap(errors.CodeSessionNotFound, "session not found", err) // Preserve the wrapped database error while keeping the existing caller-facing code path stable.
	}

	return session, nil // Return the locked session snapshot to the transactional caller.
}

// EndSessionIfActive marks a locked session as ended and reports whether this call changed the row.
func (d *TxDAO) EndSessionIfActive(ctx context.Context, id string, endedAt time.Time) (bool, error) {
	tag, err := d.tx.Exec(ctx, `
		UPDATE sessions
		SET status = 'ended', ended_at = $2, updated_at = $2
		WHERE id = $1 AND status <> 'ended'
	`, id, endedAt) // Update only active sessions so repeated end calls stay idempotent without overwriting an existing terminal timestamp.
	if err != nil { // Surface write failures while keeping the idempotent rows-affected contract for callers.
		return false, errors.Wrap(errors.CodeStoreWrite, "failed to update session status", err) // Wrap the database error with a stable storage write code.
	}

	return tag.RowsAffected() == 1, nil // Report whether this transaction performed the state transition.
}

// InsertPlanEvent stores a plan event inside the caller's open transaction.
func (d *TxDAO) InsertPlanEvent(ctx context.Context, e *PlanEvent) error {
	_, err := d.tx.Exec(ctx, `
		INSERT INTO plan_events (trace_id, seq, step_index, status, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, e.TraceID, e.Seq, e.StepIndex, e.Status, e.Payload, e.CreatedAt) // Persist the event as part of the surrounding transactional batch.
	if err != nil { // Convert raw PostgreSQL failures into the repository's write-error contract.
		return errors.Wrap(errors.CodeStoreWrite, "failed to insert plan event", err) // Preserve the underlying insert failure for debugging while returning a stable code.
	}

	return nil // Return success once the event row has been queued inside the current transaction.
}

// CompareAndSwapTraceStatus performs the same optimistic trace transition as DAO.CompareAndSwapTraceStatus but keeps the write inside the caller's open transaction.
func (d *TxDAO) CompareAndSwapTraceStatus(ctx context.Context, id string, currentStatuses []string, nextStatus string) (bool, error) {
	if len(currentStatuses) == 0 { // Reject invalid callers because an empty allow-list would always fail silently inside the transaction too.
		return false, errors.New(errors.CodePlanInvalid, "current trace statuses are required") // Return the same stable validation error used by the non-transactional helper.
	}

	tag, err := d.tx.Exec(ctx, `
		UPDATE traces
		SET status = $2, updated_at = NOW()
		WHERE id = $1 AND status = ANY($3)
	`, id, nextStatus, currentStatuses) // Perform the compare-and-swap while the surrounding transaction still controls any related event inserts.
	if err != nil { // Convert database failures into the repository's storage write contract.
		return false, errors.Wrap(errors.CodeStoreWrite, "failed to transition trace status", err) // Preserve the raw PostgreSQL failure while returning a stable application error code.
	}

	return tag.RowsAffected() == 1, nil // Report whether the optimistic state transition actually won the race inside the current transaction.
}

// NextPlanEventSeq returns the next monotonically increasing event sequence number for one trace inside the caller's open transaction.
func (d *TxDAO) NextPlanEventSeq(ctx context.Context, traceID string) (int64, error) {
	var nextSeq int64                                                                                                          // Hold the computed next sequence number returned by PostgreSQL for this trace.
	err := d.tx.QueryRow(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM plan_events WHERE trace_id = $1`, traceID).Scan(&nextSeq) // Compute the next event slot while the surrounding transaction still protects the paired status change.
	if err != nil {                                                                                                            // Convert aggregate-query failures into the repository's storage read contract.
		return 0, errors.Wrap(errors.CodeStoreRead, "failed to compute next plan event sequence", err) // Preserve the database failure so callers can diagnose sequence allocation problems.
	}

	return nextSeq, nil // Return the next append position for the trace's ordered event stream.
}

// CompareAndSwapTraceStatus performs an optimistic state transition by updating the trace only when its current status is one of the allowed states.
func (d *DAO) CompareAndSwapTraceStatus(ctx context.Context, id string, currentStatuses []string, nextStatus string) (bool, error) {
	if len(currentStatuses) == 0 { // Reject invalid callers because an empty allow-list would always fail silently.
		return false, errors.New(errors.CodePlanInvalid, "current trace statuses are required") // Return a stable validation error for misuse of the optimistic transition API.
	}

	tag, err := d.pool.Exec(ctx, `
		UPDATE traces
		SET status = $2, updated_at = NOW()
		WHERE id = $1 AND status = ANY($3)
	`, id, nextStatus, currentStatuses) // Perform the compare-and-swap in one statement so concurrent transitions cannot interleave between read and write.
	if err != nil { // Convert database failures into the repository's storage write contract.
		return false, errors.Wrap(errors.CodeStoreWrite, "failed to transition trace status", err) // Preserve the raw PostgreSQL failure while returning a stable application error code.
	}

	return tag.RowsAffected() == 1, nil // Report whether the optimistic state transition actually won the race.
}

// TransitionTraceStatusWithEvent atomically moves a trace into a new lifecycle state and appends one ordered terminal event when the status transition wins.
func (d *DAO) TransitionTraceStatusWithEvent(ctx context.Context, id string, currentStatuses []string, nextStatus string, event *PlanEvent) (bool, error) {
	if len(currentStatuses) == 0 { // Reject invalid callers because an empty allow-list would always fail silently before any event could be appended.
		return false, errors.New(errors.CodePlanInvalid, "current trace statuses are required") // Return a stable validation error for misuse of the atomic transition helper.
	}
	if event == nil { // Reject nil event payloads because callers expect this helper to persist one matching terminal event with the status change.
		return false, errors.New(errors.CodePlanInvalid, "plan event is required") // Surface the missing event explicitly so callers cannot commit a terminal state without its matching event row.
	}

	changed := false                             // Track whether this call won the optimistic status transition so callers can suppress duplicate terminal handling.
	err := d.WithTx(ctx, func(tx *TxDAO) error { // Run the transition and event insert inside one transaction so the trace state and final event never diverge.
		updated, err := tx.CompareAndSwapTraceStatus(ctx, id, currentStatuses, nextStatus) // Attempt the guarded trace state transition before reserving an event sequence slot.
		if err != nil {                                                                    // Stop immediately when the transactional compare-and-swap fails.
			return err // Preserve the DAO-produced storage error for the caller.
		}
		if !updated { // Treat lost races as a successful no-op so terminal transitions remain idempotent under concurrent callers.
			return nil // Skip event insertion because another caller already moved the trace out of the allowed source states.
		}

		nextSeq, err := tx.NextPlanEventSeq(ctx, id) // Allocate the next ordered event sequence inside the same transaction as the status change.
		if err != nil {                              // Stop immediately when the next event slot cannot be computed.
			return err // Preserve the wrapped storage read error for the caller.
		}

		event.TraceID = id            // Force the terminal event to target the same trace row that just changed state.
		event.Seq = nextSeq           // Stamp the event with the next monotonic sequence number reserved inside the current transaction.
		if event.CreatedAt.IsZero() { // Fill in the event timestamp only when the caller did not already provide one.
			event.CreatedAt = time.Now().UTC() // Use the current UTC time so the committed terminal event remains chronologically comparable with other persisted rows.
		}
		if err := tx.InsertPlanEvent(ctx, event); err != nil { // Append the terminal event before commit so the status change and final event succeed or fail together.
			return err // Preserve the wrapped storage write error for the caller.
		}

		changed = true // Record that this transaction won the race and staged the matching terminal event.
		return nil     // Allow DAO.WithTx to commit the paired status change and event append atomically.
	})
	if err != nil { // Return any transactional compare-and-swap, sequence, or insert failure unchanged to the caller.
		return false, err // Preserve the original repository error chain so higher layers keep stable error-code handling.
	}

	return changed, nil // Report whether this call committed the terminal state transition and matching event.
}

// InsertAuditLog appends a new audit log row outside of an explicit transaction.
func (d *DAO) InsertAuditLog(ctx context.Context, log *AuditLog) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO audit_logs (id, project_id, session_id, trace_id, actor, action, resource_type, resource_id, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, log.ID, log.ProjectID, log.SessionID, log.TraceID, log.Actor, log.Action, log.ResourceType, log.ResourceID, log.Payload, log.CreatedAt) // Persist the append-only audit record in the shared audit table.
	if err != nil { // Return a wrapped storage error when the audit insert fails.
		return errors.Wrap(errors.CodeStoreWrite, "failed to insert audit log", err) // Preserve the underlying database failure for diagnostics.
	}

	return nil // Return success after the audit record has been written durably.
}

// InsertAuditLog appends a new audit log row inside the caller's open transaction.
func (d *TxDAO) InsertAuditLog(ctx context.Context, log *AuditLog) error {
	_, err := d.tx.Exec(ctx, `
		INSERT INTO audit_logs (id, project_id, session_id, trace_id, actor, action, resource_type, resource_id, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, log.ID, log.ProjectID, log.SessionID, log.TraceID, log.Actor, log.Action, log.ResourceType, log.ResourceID, log.Payload, log.CreatedAt) // Persist the audit record atomically with the caller's surrounding transaction.
	if err != nil { // Convert raw PostgreSQL failures into the repository's write-error contract.
		return errors.Wrap(errors.CodeStoreWrite, "failed to insert audit log", err) // Preserve the database failure while returning the standard storage write code.
	}

	return nil // Return success after the audit insert has been staged inside the current transaction.
}
