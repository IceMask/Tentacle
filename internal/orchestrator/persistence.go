// persistence.go centralizes orchestrator persistence flows that need DAO locking or optimistic status transitions.
package orchestrator

import (
	"context"
	"encoding/json"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/postgres"
)

// markSessionEnded serializes concurrent end-session requests with a row lock and reports whether this call changed the stored session state.
func (s *Service) markSessionEnded(ctx context.Context, sessionID string, endedAt time.Time) (bool, error) {
	changed := false                                          // Track whether this call performed the state transition so callers can keep end-session idempotent.
	err := s.dao.WithTx(ctx, func(tx *postgres.TxDAO) error { // Run the lock-and-update sequence in one transaction so the row lock stays valid for the write.
		session, err := tx.GetSessionForUpdate(ctx, sessionID) // Lock the session row before inspecting its current lifecycle state.
		if err != nil {                                        // Stop immediately when the target session does not exist or cannot be locked.
			return err // Propagate the repository error unchanged so callers preserve the existing not-found contract.
		}

		if session.Status == "ended" { // Treat repeated end calls as a no-op once the session is already terminal.
			return nil // Return success so higher layers can keep the API idempotent.
		}

		updated, err := tx.EndSessionIfActive(ctx, sessionID, endedAt) // Persist the terminal session state while the row lock is still held.
		if err != nil {                                                // Surface storage write failures from the transactional update helper.
			return err // Preserve the wrapped write error so callers can map it consistently.
		}

		changed = updated // Remember whether this transaction performed the transition.
		return nil        // Finish the callback so DAO.WithTx can commit the locked write.
	})
	if err != nil { // Return any lookup, locking, or write failure to the caller.
		return false, err // Keep the original repository error chain intact.
	}

	return changed, nil // Tell the caller whether this request actually ended the session row.
}

// transitionTraceStatus performs an optimistic compare-and-swap on the trace lifecycle state.
func (s *Service) transitionTraceStatus(ctx context.Context, traceID string, currentStatuses []string, nextStatus string) (bool, error) {
	changed, err := s.dao.CompareAndSwapTraceStatus(ctx, traceID, currentStatuses, nextStatus) // Attempt the status transition in one SQL statement so concurrent writers race safely.
	if err != nil {                                                                            // Return storage-layer compare-and-swap failures unchanged so callers keep the original error code and message chain.
		return false, err // Preserve the DAO-produced error instead of wrapping it a second time.
	}

	return changed, nil // Report whether this caller won the optimistic state transition.
}

// finalizeTrace atomically applies one terminal trace transition and appends the matching final event so clients always have a durable terminal marker to replay.
func (s *Service) finalizeTrace(ctx context.Context, traceID string, currentStatuses []string, nextStatus string, message string, cause error, retryCount int64) (bool, error) {
	if message == "" { // Fill in a deterministic fallback message so every terminal event remains readable even when the caller omits custom text.
		message = "trace " + nextStatus // Use the terminal lifecycle state itself as the concise default event message.
	}

	payloadMap := map[string]interface{}{ // Build the terminal event payload in one map so the final event records the same outcome metadata regardless of the caller path.
		"message":        message,    // Preserve the human-readable terminal summary for polling and websocket consumers.
		"phase":          "trace",    // Mark the event as a trace-level terminal event rather than a step-level execution update.
		"terminalStatus": nextStatus, // Duplicate the terminal lifecycle state inside the payload for clients that rely on payload inspection alone.
	}
	if cause != nil { // Attach the underlying error details only when the terminal transition was triggered by a failure path.
		payloadMap["error"] = cause.Error()       // Preserve the original error string so operators can diagnose terminal failures from event replay alone.
		if code, ok := errors.CodeOf(cause); ok { // Attach the structured error code when the failure path used the repository error type.
			payloadMap["errorCode"] = string(code) // Preserve the stable machine-readable error code for downstream diagnostics and metrics correlation.
		}
	}
	if retryCount > 0 { // Record retry exhaustion counts only for terminal dispatch failures that actually consumed retries.
		payloadMap["retryCount"] = retryCount // Preserve the retry count so operators can distinguish immediate failures from exhausted dispatch retries.
	}

	payload, err := json.Marshal(payloadMap) // Serialize the terminal payload before entering the DAO helper so any JSON failure aborts cleanly before the transaction begins.
	if err != nil {                          // Stop immediately when the terminal payload cannot be encoded safely.
		return false, errors.Wrap(errors.CodeInternal, "failed to marshal terminal trace event", err) // Surface payload serialization failures as internal orchestrator errors.
	}

	changed, err := s.dao.TransitionTraceStatusWithEvent(ctx, traceID, currentStatuses, nextStatus, &postgres.PlanEvent{ // Commit the terminal status change and its matching final event in one transaction.
		TraceID:   traceID,    // Target the same trace row that is transitioning into its terminal state.
		StepIndex: -1,         // Use a sentinel step index so clients can distinguish trace-level terminal events from step-level execution events.
		Status:    nextStatus, // Mirror the terminal trace state in the event status for simple client-side filtering.
		Payload:   payload,    // Persist the normalized terminal metadata payload prepared above.
		CreatedAt: time.Now(), // Stamp the terminal event with the current wall-clock time before the transaction commits.
	})
	if err != nil { // Return transactional transition or event-insert failures unchanged so callers can decide whether to retry or surface the error.
		return false, err // Preserve the DAO-produced error chain for consistent higher-layer handling.
	}

	return changed, nil // Report whether this caller actually won the terminal transition and appended the final event.
}

// FinalizeQueuedTrace records a dispatcher-owned terminal outcome for a queued trace without overwriting a terminal state that has already been committed elsewhere.
func (s *Service) FinalizeQueuedTrace(ctx context.Context, traceID string, nextStatus string, message string, cause error, retryCount int64) error {
	_, err := s.finalizeTrace(ctx, traceID, []string{"pending", "running"}, nextStatus, message, cause, retryCount) // Allow dispatcher-originated terminalization to win only while the trace is still active.
	if err != nil {                                                                                                 // Return transactional failures to the dispatcher so it can avoid ACKing prematurely.
		return err // Preserve the original repository or serialization error for the caller.
	}

	return nil // Report success once the dispatcher-owned terminal outcome has been durably recorded or found to be already terminal.
}

// MarkDispatchedTraceRunning records that a distributed trace has left the queue and is now executing on a worker without overwriting any concurrent terminal state.
func (s *Service) MarkDispatchedTraceRunning(ctx context.Context, traceID string) error {
	_, err := s.transitionTraceStatus(ctx, traceID, []string{"pending"}, "running") // Promote the queued trace to running only when no concurrent cancel or terminal transition has already won.
	if err != nil {                                                                 // Return compare-and-swap failures so the dispatcher can log bookkeeping problems after worker acceptance.
		return err // Preserve the DAO-produced error chain for consistent higher-layer logging and diagnosis.
	}

	return nil // Treat a lost compare-and-swap race as a no-op because another lifecycle transition has already moved the trace out of pending.
}
