// coordination.go provides transactional, locking, optimistic state-transition, and audit-log helpers for the PostgreSQL persistence layer.
package postgres

import (
	"context"
	stdErrors "errors"
	"strings"
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
		SELECT id, project_id, tenant_id, subject_id, status, capabilities, created_at, updated_at, ended_at
		FROM sessions
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&session.ID, &session.ProjectID, &session.TenantID, &session.SubjectID, &session.Status, &session.Capabilities, &session.CreatedAt, &session.UpdatedAt, &session.EndedAt) // Lock the session row together with its ownership metadata so authorization and lifecycle mutations serialize on the same record.
	if stdErrors.Is(err, pgx.ErrNoRows) { // Map only an authoritative empty locked query to session-not-found.
		return nil, errors.Wrap(errors.CodeSessionNotFound, "session not found", err) // Preserve the no-row cause under the stable lookup code.
	}
	if err != nil { // Keep lock acquisition, connection, and scan failures distinct from absence.
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to lock session", err) // Surface infrastructure failures through the storage-read contract.
	}

	return session, nil // Return the locked session snapshot to the transactional caller.
}

// GetTraceForUpdate loads one trace row and acquires a row-level lock that remains held until the surrounding transaction completes.
func (d *TxDAO) GetTraceForUpdate(ctx context.Context, id string) (*Trace, error) {
	trace := &Trace{} // Allocate the destination trace model before scanning the locked row returned by PostgreSQL.
	err := d.tx.QueryRow(ctx, `
		SELECT id, session_id, project_id, tenant_id, subject_id, status, current_attempt, terminal_reason, created_at, updated_at
		FROM traces
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&trace.ID, &trace.SessionID, &trace.ProjectID, &trace.TenantID, &trace.SubjectID, &trace.Status, &trace.CurrentAttempt, &trace.TerminalReason, &trace.CreatedAt, &trace.UpdatedAt) // Lock the trace row together with its ownership metadata so authorization and attempt checks observe one serialized view.
	if stdErrors.Is(err, pgx.ErrNoRows) { // Map only an authoritative empty locked query to trace-not-found.
		return nil, errors.Wrap(errors.CodeTraceNotFound, "trace not found", err) // Preserve the no-row cause under the stable lookup code.
	}
	if err != nil { // Keep lock acquisition, connection, and scan failures distinct from absence.
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to lock trace", err) // Surface infrastructure failures through the storage-read contract.
	}

	return trace, nil // Return the locked trace snapshot to the transactional caller.
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

// MarkSessionEnding moves one locked non-terminal session into the cleanup-in-progress state and reports whether this call changed the row.
func (d *TxDAO) MarkSessionEnding(ctx context.Context, id string, updatedAt time.Time) (bool, error) {
	tag, err := d.tx.Exec(ctx, `
		UPDATE sessions
		SET status = 'ending', updated_at = $2
		WHERE id = $1 AND status NOT IN ('ending', 'ended')
	`, id, updatedAt) // Block new session operations before external Appium and Device Farm cleanup begins.
	if err != nil { // Surface storage failures before callers attempt any non-transactional resource cleanup.
		return false, errors.Wrap(errors.CodeStoreWrite, "failed to mark session ending", err) // Preserve the PostgreSQL cause under the stable write-error contract.
	}
	return tag.RowsAffected() == 1, nil // Report whether this transaction performed the transition into cleanup-in-progress.
}

// EndSessionIfEnding marks one locked cleanup-in-progress session as ended only after external resource cleanup has succeeded.
func (d *TxDAO) EndSessionIfEnding(ctx context.Context, id string, endedAt time.Time) (bool, error) {
	tag, err := d.tx.Exec(ctx, `
		UPDATE sessions
		SET status = 'ended', ended_at = $2, updated_at = $2
		WHERE id = $1 AND status = 'ending'
	`, id, endedAt) // Commit the terminal lifecycle state only for a session whose cleanup phase was prepared explicitly.
	if err != nil { // Surface storage failures so the caller can retry finalization without repeating successful cleanup unsafely.
		return false, errors.Wrap(errors.CodeStoreWrite, "failed to complete session end", err) // Preserve the PostgreSQL cause under the stable write-error contract.
	}
	return tag.RowsAffected() == 1, nil // Report whether this transaction committed the ending-to-ended transition.
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

// UpdateTraceStatusAndReason updates one already-locked trace row to the supplied lifecycle status and terminal reason.
func (d *TxDAO) UpdateTraceStatusAndReason(ctx context.Context, id string, nextStatus string, terminalReason *string) error {
	_, err := d.tx.Exec(ctx, `
		UPDATE traces
		SET status = $2, terminal_reason = $3, updated_at = NOW()
		WHERE id = $1
	`, id, nextStatus, terminalReason) // Persist the locked trace lifecycle update together with the matching terminal reason inside the caller's open transaction.
	if err != nil { // Convert raw PostgreSQL failures into the repository's storage write contract.
		return errors.Wrap(errors.CodeStoreWrite, "failed to update trace status and terminal reason", err) // Preserve the database failure while returning a stable write-error code.
	}

	return nil // Return success once the locked trace update has been staged inside the current transaction.
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

// ReserveTraceAttempt increments one distributed trace attempt counter without moving the trace out of its current lifecycle state.
func (d *DAO) ReserveTraceAttempt(ctx context.Context, id string, currentStatuses []string) (int64, bool, error) {
	if len(currentStatuses) == 0 { // Reject invalid callers because an empty allow-list would silently prevent every trace promotion.
		return 0, false, errors.New(errors.CodePlanInvalid, "current trace statuses are required") // Return a stable validation error for misuse of the distributed-attempt helper.
	}

	var nextAttempt int64 // Hold the incremented attempt number returned by PostgreSQL when this caller wins the reservation race.
	err := d.pool.QueryRow(ctx, `
		UPDATE traces
		SET current_attempt = current_attempt + 1, terminal_reason = NULL, updated_at = NOW()
		WHERE id = $1 AND status = ANY($2)
		RETURNING current_attempt
	`, id, currentStatuses).Scan(&nextAttempt) // Reserve the next distributed attempt in one statement so the worker can receive a stable ownership token before acceptance.
	if err != nil { // Distinguish a lost race from a real PostgreSQL failure before returning to the dispatcher.
		if err == pgx.ErrNoRows { // Treat no returned row as a benign lost race because another lifecycle transition already moved the trace elsewhere.
			return 0, false, nil // Report that this caller did not win the promotion so higher layers can avoid persisting a lease for a stale acceptance.
		}
		return 0, false, errors.Wrap(errors.CodeStoreWrite, "failed to reserve distributed trace attempt", err) // Surface PostgreSQL failures through the repository's write-error contract.
	}

	return nextAttempt, true, nil // Return the reserved current attempt once PostgreSQL has advanced the attempt counter successfully.
}

// MarkTraceAttemptRunning moves one reserved distributed attempt into the running state when the supplied attempt still matches the trace row.
func (d *DAO) MarkTraceAttemptRunning(ctx context.Context, id string, attempt int64, currentStatuses []string) (bool, error) {
	if len(currentStatuses) == 0 { // Reject invalid callers because an empty allow-list would silently prevent every trace promotion.
		return false, errors.New(errors.CodePlanInvalid, "current trace statuses are required") // Return a stable validation error for misuse of the distributed-running helper.
	}

	tag, err := d.pool.Exec(ctx, `
		UPDATE traces
		SET status = 'running', updated_at = NOW()
		WHERE id = $1 AND current_attempt = $2 AND status = ANY($3)
	`, id, attempt, currentStatuses) // Promote the trace to running only when the reserved attempt still matches the current trace row.
	if err != nil { // Convert database failures into the repository's storage write contract.
		return false, errors.Wrap(errors.CodeStoreWrite, "failed to mark distributed trace running", err) // Preserve the PostgreSQL failure while returning the standard write-error code.
	}

	return tag.RowsAffected() == 1, nil // Report whether the reserved attempt still owned the trace and therefore entered the running state successfully.
}

// AppendPlanEventForAttempt appends one ordered step event when the supplied attempt owns a reserved pending or running trace.
func (d *DAO) AppendPlanEventForAttempt(ctx context.Context, id string, expectedAttempt int64, event *PlanEvent) (bool, error) {
	if event == nil { // Reject nil event payloads because distributed callbacks must persist concrete step metadata.
		return false, errors.New(errors.CodePlanInvalid, "plan event is required") // Surface the missing event explicitly so callback handlers do not succeed silently.
	}

	appended := false                            // Track whether this callback still owned the trace attempt and therefore appended an event row.
	err := d.WithTx(ctx, func(tx *TxDAO) error { // Run the attempt check, sequence allocation, and insert inside one transaction so races with terminalization stay deterministic.
		trace, err := tx.GetTraceForUpdate(ctx, id) // Lock the trace row so attempt ownership and lifecycle checks observe one serialized view of the trace state.
		if err != nil {                             // Stop immediately when the trace row cannot be loaded or locked.
			return err // Preserve the trace lookup failure for the caller.
		}
		if (trace.Status != "pending" && trace.Status != "running") || trace.CurrentAttempt != expectedAttempt { // Accept immediate worker callbacks across the pending-to-running acknowledgement boundary while rejecting every stale or terminal attempt.
			return nil // Treat stale callbacks as successful no-ops so higher layers can respond with a clean stale acknowledgement.
		}

		nextSeq, err := tx.NextPlanEventSeq(ctx, id) // Allocate the next replay sequence number while the trace row is still locked for this attempt.
		if err != nil {                              // Stop immediately when sequence allocation fails because the callback cannot be appended safely.
			return err // Preserve the wrapped storage error for the caller.
		}

		event.TraceID = id            // Force the event to target the locked trace row rather than trusting external callback input blindly.
		event.Seq = nextSeq           // Stamp the callback event with the next ordered sequence reserved inside the current transaction.
		if event.CreatedAt.IsZero() { // Fill in the event timestamp only when the caller did not already provide one explicitly.
			event.CreatedAt = time.Now().UTC() // Use the current UTC time so callback events remain chronologically comparable across workers and services.
		}
		if err := tx.InsertPlanEvent(ctx, event); err != nil { // Append the callback event inside the still-open transaction so the attempt check and insert stay atomic.
			return err // Preserve the wrapped storage write error for the caller.
		}

		appended = true // Record that this callback still owned the current running trace attempt and appended its event successfully.
		return nil      // Allow DAO.WithTx to commit the locked event insert.
	})
	if err != nil { // Return any trace-lock, sequence-allocation, or insert failure unchanged to the caller.
		return false, err // Preserve the original repository error chain for higher-layer transport mapping.
	}

	return appended, nil // Report whether the callback event was appended or ignored as stale.
}

// TransitionTraceAttemptWithEvent atomically finalizes one running trace attempt and appends the matching terminal event when the supplied attempt still owns the trace.
func (d *DAO) TransitionTraceAttemptWithEvent(ctx context.Context, id string, expectedAttempt int64, currentStatuses []string, nextStatus string, terminalReason *string, event *PlanEvent) (bool, error) {
	if len(currentStatuses) == 0 { // Reject invalid callers because an empty allow-list would silently ignore every finalize attempt.
		return false, errors.New(errors.CodePlanInvalid, "current trace statuses are required") // Return a stable validation error for misuse of the distributed-finalization helper.
	}
	if event == nil { // Reject nil event payloads because finalization must append one replayable terminal event alongside the status change.
		return false, errors.New(errors.CodePlanInvalid, "plan event is required") // Surface the missing terminal event explicitly so finalizers cannot partially succeed.
	}

	transitioned := false                        // Track whether this caller still owned the trace attempt and therefore committed the terminal state.
	err := d.WithTx(ctx, func(tx *TxDAO) error { // Run the attempt guard, terminal trace update, sequence allocation, and event insert in one transaction.
		trace, err := tx.GetTraceForUpdate(ctx, id) // Lock the trace row so attempt ownership and lifecycle checks observe one serialized view of the trace state.
		if err != nil {                             // Stop immediately when the trace row cannot be loaded or locked.
			return err // Preserve the trace lookup failure for the caller.
		}
		if trace.CurrentAttempt != expectedAttempt || !traceStatusAllowed(trace.Status, currentStatuses) { // Ignore late results once another attempt or terminal state has already won the trace.
			return nil // Treat stale terminal callbacks as successful no-ops so higher layers can acknowledge them cleanly.
		}

		if err := tx.UpdateTraceStatusAndReason(ctx, id, nextStatus, terminalReason); err != nil { // Persist the terminal lifecycle state while the trace row is still locked for this attempt.
			return err // Preserve the wrapped storage write error for the caller.
		}

		nextSeq, err := tx.NextPlanEventSeq(ctx, id) // Allocate the next replay sequence number while the terminal state update is still inside the same transaction.
		if err != nil {                              // Stop immediately when sequence allocation fails because the final event must commit atomically with the status change.
			return err // Preserve the wrapped storage error for the caller.
		}

		event.TraceID = id            // Force the terminal event to target the same locked trace row that just changed state.
		event.Seq = nextSeq           // Stamp the terminal event with the next ordered sequence reserved inside the current transaction.
		if event.CreatedAt.IsZero() { // Fill in the terminal event timestamp only when the caller did not already provide one explicitly.
			event.CreatedAt = time.Now().UTC() // Use the current UTC time so terminal events remain chronologically comparable across callbacks and local execution.
		}
		if err := tx.InsertPlanEvent(ctx, event); err != nil { // Append the terminal event before commit so the status change and final replay marker succeed or fail together.
			return err // Preserve the wrapped storage write error for the caller.
		}

		transitioned = true // Record that this caller still owned the attempt and committed the terminal transition successfully.
		return nil          // Allow DAO.WithTx to commit the paired terminal state change and event append atomically.
	})
	if err != nil { // Return any lock, update, sequence, or insert failure unchanged to the caller.
		return false, err // Preserve the original repository error chain for higher-layer transport mapping.
	}

	return transitioned, nil // Report whether the supplied attempt still owned the trace and therefore committed the terminal outcome.
}

// traceStatusAllowed returns true when one trace status appears inside the caller-supplied allow-list.
func traceStatusAllowed(status string, allowed []string) bool {
	for _, candidate := range allowed { // Scan the supplied allow-list because the trace-finalization helper accepts only a small fixed number of statuses at each call site.
		if candidate == status { // Match the current trace status against the active allow-list entry.
			return true // Return immediately once the current trace status is confirmed as allowed.
		}
	}

	return false // Report that the current trace status is outside the caller's allow-list.
}

// TransitionTraceStatusWithEvent atomically moves a trace into a new lifecycle state and appends one ordered terminal event when the status transition wins.
func (d *DAO) TransitionTraceStatusWithEvent(ctx context.Context, id string, currentStatuses []string, nextStatus string, event *PlanEvent) (bool, error) {
	return d.TransitionTraceStatusWithEventAndReason(ctx, id, currentStatuses, nextStatus, nil, event) // Delegate to the terminal-reason aware helper while preserving the legacy no-reason behavior.
}

// TransitionTraceStatusWithEventAndReason atomically moves a trace into a new lifecycle state, stores the terminal reason, and appends one ordered terminal event when the status transition wins.
func (d *DAO) TransitionTraceStatusWithEventAndReason(ctx context.Context, id string, currentStatuses []string, nextStatus string, terminalReason *string, event *PlanEvent) (bool, error) {
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
		if err := tx.UpdateTraceStatusAndReason(ctx, id, nextStatus, terminalReason); err != nil { // Persist the optional terminal reason while the trace is already locked inside the same transaction.
			return err // Preserve the wrapped storage write error for the caller.
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

	return changed, nil // Report whether this call committed the terminal state transition, optional reason, and matching event.
}

// InsertAuditLog appends a new audit log row outside of an explicit transaction.
func (d *DAO) InsertAuditLog(ctx context.Context, log *AuditLog) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO audit_logs (id, project_id, session_id, trace_id, actor, action, resource_type, resource_id, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, log.ID, log.ProjectID, nullableAuditReference(log.SessionID), nullableAuditReference(log.TraceID), log.Actor, log.Action, log.ResourceType, log.ResourceID, log.Payload, log.CreatedAt) // Persist optional foreign keys as SQL NULL instead of invalid empty-string references.
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
	`, log.ID, log.ProjectID, nullableAuditReference(log.SessionID), nullableAuditReference(log.TraceID), log.Actor, log.Action, log.ResourceType, log.ResourceID, log.Payload, log.CreatedAt) // Persist optional foreign keys as SQL NULL instead of invalid empty-string references.
	if err != nil { // Convert raw PostgreSQL failures into the repository's write-error contract.
		return errors.Wrap(errors.CodeStoreWrite, "failed to insert audit log", err) // Preserve the database failure while returning the standard storage write code.
	}

	return nil // Return success after the audit insert has been staged inside the current transaction.
}

// nullableAuditReference converts an optional audit foreign-key identifier into nil for SQL NULL or a normalized non-empty string.
func nullableAuditReference(value string) interface{} {
	normalizedValue := strings.TrimSpace(value) // Normalize optional identifiers before deciding whether a real foreign key exists.
	if normalizedValue == "" {                  // Store absent session and trace references as SQL NULL.
		return nil // Avoid foreign-key failures against the invalid empty-string identifier.
	}
	return normalizedValue // Preserve the concrete normalized foreign-key identifier when supplied.
}
