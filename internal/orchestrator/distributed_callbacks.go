// distributed_callbacks.go implements worker-to-orchestrator distributed callback handling for step events, lease renewals, and terminal results.
package orchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/storage/postgres"
)

// RenewDistributedExecutionLease refreshes one distributed execution lease when the supplied worker attempt still owns the trace.
func (s *Service) RenewDistributedExecutionLease(ctx context.Context, traceID string, workerID string, attempt int64) (bool, error) {
	if strings.TrimSpace(traceID) == "" { // Reject empty trace identifiers because lease state cannot be resolved without a concrete trace id.
		return false, errors.New(errors.CodePlanInvalid, "trace_id is required") // Surface invalid renew requests as plan-validation errors for stable transport mapping.
	}
	if strings.TrimSpace(workerID) == "" { // Reject empty worker identifiers because distributed lease ownership is keyed by worker id and attempt together.
		return false, errors.New(errors.CodePlanInvalid, "worker_id is required") // Surface invalid renew requests as plan-validation errors for stable transport mapping.
	}
	if attempt <= 0 { // Reject non-positive attempts because distributed ownership starts at attempt 1 after worker acceptance.
		return false, errors.New(errors.CodePlanInvalid, "attempt must be > 0") // Surface invalid renew requests before any Redis lease lookup occurs.
	}

	renewed, err := s.dispatcher.RenewExecutionLease(ctx, traceID, workerID, attempt) // Refresh the Redis-backed ownership lease when the worker attempt still owns this distributed trace.
	if err != nil {                                                                   // Surface Redis lease failures because the worker cannot safely continue execution without a valid lease renewal result.
		return false, err // Preserve the wrapped storage error for higher-layer transport mapping and worker logging.
	}

	return renewed, nil // Report whether the distributed lease still belongs to the supplied worker attempt.
}

// ReportDistributedPlanEvent appends one worker-emitted step event when the supplied worker attempt still owns the running trace.
func (s *Service) ReportDistributedPlanEvent(ctx context.Context, req *rpc.ReportPlanEventRequest) (bool, error) {
	if req == nil { // Reject nil callback payloads because no trace, worker, or event data can be validated safely from them.
		return false, errors.New(errors.CodePlanInvalid, "report plan event request is required") // Surface invalid callback payloads before any lease or DAO work begins.
	}

	renewed, err := s.RenewDistributedExecutionLease(ctx, req.TraceID, req.WorkerID, req.Attempt) // Validate current ownership and refresh the lease because active step events imply forward execution progress.
	if err != nil {                                                                               // Surface lease-validation failures because the callback cannot be processed safely without current ownership.
		return false, err // Preserve the wrapped validation or storage error for higher-layer transport mapping and worker logging.
	}
	if !renewed { // Treat stale or cleaned-up leases as a benign no-op so workers can stop emitting events for lost ownership without causing transport failures.
		s.logger.InfoContext(ctx, "ignored distributed step event for stale lease", "trace_id", req.TraceID, "worker_id", req.WorkerID, "attempt", req.Attempt, "step_index", req.StepIndex) // Log stale event drops so operators can correlate late callbacks with ownership changes.
		return false, nil                                                                                                                                                                    // Report the stale callback without surfacing it as an infrastructure error.
	}

	payload, err := json.Marshal(map[string]interface{}{ // Normalize the worker event into the same payload shape used by the monolith executor path.
		"message":      req.Message,      // Preserve the step-level status message exactly as emitted by the worker-side executor.
		"metrics":      req.Metrics,      // Preserve the worker-reported attempt and webdriver metrics for replay and diagnostics.
		"artifactRefs": req.ArtifactRefs, // Preserve any artifact references emitted by the worker-side executor for replay consumers.
		"phase":        req.Phase,        // Preserve the optional worker-reported phase field so replay clients can distinguish step-level sub-phases.
	}) // Serialize the normalized payload before acquiring any DAO transaction so JSON encoding failures abort cleanly.
	if err != nil { // Stop immediately when the distributed step event payload cannot be encoded safely.
		return false, errors.Wrap(errors.CodeInternal, "failed to marshal distributed plan event payload", err) // Surface payload serialization failures as internal orchestrator errors.
	}

	appended, err := s.dao.AppendPlanEventForAttempt(ctx, req.TraceID, req.Attempt, &postgres.PlanEvent{ // Append the worker-emitted step event only when the supplied attempt still owns the running trace.
		TraceID:   req.TraceID,   // Target the same trace that the worker callback claims to be updating.
		StepIndex: req.StepIndex, // Preserve the worker-reported step index so replay order remains understandable to clients.
		Status:    req.Status,    // Preserve the worker-reported step lifecycle state for replay filtering and diagnostics.
		Payload:   payload,       // Persist the normalized worker event payload prepared above.
		CreatedAt: time.Now(),    // Stamp the callback with the current wall-clock time before the DAO transaction allocates its ordered sequence number.
	}) // Persist the step event only when this worker attempt still owns the running trace.
	if err != nil { // Surface DAO attempt-guard or insert failures because the callback could not be stored safely.
		return false, err // Preserve the wrapped DAO error for higher-layer transport mapping and worker logging.
	}
	if !appended { // Treat stale or already-terminal traces as a benign no-op once the DAO confirms this callback no longer owns the trace.
		s.logger.InfoContext(ctx, "ignored distributed step event after DAO ownership check", "trace_id", req.TraceID, "worker_id", req.WorkerID, "attempt", req.Attempt, "step_index", req.StepIndex) // Log the stale drop so operators can correlate late callbacks with terminalization races.
		return false, nil                                                                                                                                                                              // Report the stale callback without surfacing it as an infrastructure failure.
	}

	return true, nil // Report that the worker-emitted step event was appended successfully for the current distributed attempt.
}

// CompleteDistributedPlan finalizes one distributed trace attempt when the supplied worker attempt still owns the trace.
func (s *Service) CompleteDistributedPlan(ctx context.Context, req *rpc.CompletePlanRequest) (bool, error) {
	if req == nil { // Reject nil callback payloads because no trace, worker, or terminal outcome can be validated safely from them.
		return false, errors.New(errors.CodePlanInvalid, "complete plan request is required") // Surface invalid callback payloads before any lease or DAO work begins.
	}

	renewed, err := s.RenewDistributedExecutionLease(ctx, req.TraceID, req.WorkerID, req.Attempt) // Validate current ownership before accepting a distributed terminal result from the worker.
	if err != nil {                                                                               // Surface lease-validation failures because the terminal callback cannot be processed safely without current ownership.
		return false, err // Preserve the wrapped validation or storage error for higher-layer transport mapping and worker logging.
	}
	if !renewed { // Treat stale or cleaned-up leases as a benign no-op so late worker completions cannot overwrite a newer owner or terminal state.
		s.logger.InfoContext(ctx, "ignored distributed completion for stale lease", "trace_id", req.TraceID, "worker_id", req.WorkerID, "attempt", req.Attempt, "final_status", req.FinalStatus) // Log stale completion drops so operators can correlate late results with ownership changes.
		return false, nil                                                                                                                                                                        // Report the stale callback without surfacing it as an infrastructure failure.
	}

	nextStatus, terminalReason, completionErr, err := normalizeDistributedCompletion(req) // Normalize the worker-reported terminal result into one orchestrator lifecycle outcome.
	if err != nil {                                                                       // Surface invalid terminal-result shapes before any DAO finalization work begins.
		return false, err // Preserve the validation failure for higher-layer transport mapping and worker logging.
	}

	finalized, err := s.finalizeDistributedAttempt(ctx, req.TraceID, req.Attempt, nextStatus, terminalReason, req.Message, completionErr) // Atomically finalize the owned trace attempt and append its terminal replay event.
	if err != nil {                                                                                                                       // Surface DAO finalization failures because the terminal callback could not be stored safely.
		return false, err // Preserve the wrapped DAO or serialization failure for higher-layer transport mapping and worker logging.
	}

	released, releaseErr := s.dispatcher.ReleaseExecutionLease(ctx, req.TraceID, req.WorkerID, req.Attempt) // Remove the Redis-backed ownership lease once this worker attempt has either finalized or been confirmed stale.
	if releaseErr != nil {                                                                                  // Log lease-release failures without discarding the already-persisted terminal outcome.
		s.logger.WarnContext(ctx, "failed to release distributed execution lease", "trace_id", req.TraceID, "worker_id", req.WorkerID, "attempt", req.Attempt, "error", releaseErr) // Surface the Redis cleanup failure for operator visibility.
	}
	if !released { // Log lease-release misses so operators can see when another path already cleaned up the ownership record first.
		s.logger.InfoContext(ctx, "distributed execution lease already released", "trace_id", req.TraceID, "worker_id", req.WorkerID, "attempt", req.Attempt) // Record the benign double-cleanup so races stay observable.
	}
	if !finalized { // Treat late results rejected by the DAO attempt guard as a benign stale callback after lease validation already succeeded.
		s.logger.InfoContext(ctx, "ignored distributed completion after DAO ownership check", "trace_id", req.TraceID, "worker_id", req.WorkerID, "attempt", req.Attempt, "final_status", req.FinalStatus) // Log the stale drop so operators can correlate it with terminalization races.
		return false, nil                                                                                                                                                                                  // Report the stale callback without surfacing it as an infrastructure failure.
	}

	return true, nil // Report that the worker terminal callback finalized the current distributed attempt successfully.
}

// finalizeDistributedAttempt atomically applies one distributed terminal trace transition for the supplied current attempt and appends the matching final event.
func (s *Service) finalizeDistributedAttempt(ctx context.Context, traceID string, attempt int64, nextStatus string, terminalReason string, message string, cause error) (bool, error) {
	if message == "" { // Fill in a deterministic fallback message so every terminal event remains readable even when the worker omits custom text.
		message = "trace " + nextStatus // Use the terminal lifecycle state itself as the concise default event message.
	}

	payloadMap := map[string]interface{}{ // Build the terminal event payload in one map so the final event records the same outcome metadata regardless of worker path.
		"message":        message,        // Preserve the human-readable terminal summary for polling and websocket consumers.
		"phase":          "trace",        // Mark the event as a trace-level terminal event rather than a step-level execution update.
		"terminalStatus": nextStatus,     // Duplicate the terminal lifecycle state inside the payload for clients that rely on payload inspection alone.
		"terminalReason": terminalReason, // Persist the structured terminal reason so replay consumers can distinguish timeout, orphaned, cancelled, and normal completion outcomes.
	}
	if cause != nil { // Attach the underlying error details only when the terminal transition was triggered by a failure path.
		payloadMap["error"] = cause.Error()       // Preserve the original error string so operators can diagnose distributed terminal failures from event replay alone.
		if code, ok := errors.CodeOf(cause); ok { // Attach the structured error code when the failure path used the repository-standard error type.
			payloadMap["errorCode"] = string(code) // Preserve the stable machine-readable error code for downstream diagnostics and metrics correlation.
		}
	}

	payload, err := json.Marshal(payloadMap) // Serialize the terminal payload before entering the DAO helper so any JSON failure aborts cleanly before the transaction begins.
	if err != nil {                          // Stop immediately when the terminal payload cannot be encoded safely.
		return false, errors.Wrap(errors.CodeInternal, "failed to marshal distributed terminal trace event", err) // Surface payload serialization failures as internal orchestrator errors.
	}

	terminalReasonCopy := terminalReason                                                                                                                      // Copy the terminal reason into one addressable local variable for PostgreSQL parameter binding.
	finalized, err := s.dao.TransitionTraceAttemptWithEvent(ctx, traceID, attempt, []string{"running"}, nextStatus, &terminalReasonCopy, &postgres.PlanEvent{ // Commit the terminal status change, reason, and final event only when this attempt still owns the running trace.
		TraceID:   traceID,    // Target the same trace row that is transitioning into its terminal state.
		StepIndex: -1,         // Use a sentinel step index so clients can distinguish trace-level terminal events from step-level execution events.
		Status:    nextStatus, // Mirror the terminal trace state in the event status for simple client-side filtering.
		Payload:   payload,    // Persist the normalized terminal metadata payload prepared above.
		CreatedAt: time.Now(), // Stamp the terminal event with the current wall-clock time before the transaction commits.
	}) // Persist the distributed terminal outcome only when the supplied attempt still owns the trace.
	if err != nil { // Return transactional transition or event-insert failures unchanged so callers can decide whether to retry or surface the error.
		return false, err // Preserve the DAO-produced error chain for consistent higher-layer handling.
	}

	return finalized, nil // Report whether this worker attempt actually won the terminal transition and appended the final event.
}

// normalizeDistributedCompletion converts one worker-reported distributed terminal callback into the orchestrator's lifecycle state, terminal reason, and optional structured error.
func normalizeDistributedCompletion(req *rpc.CompletePlanRequest) (string, string, error, error) {
	if strings.TrimSpace(req.TraceID) == "" { // Reject empty trace identifiers because terminal callbacks must target one concrete persisted trace.
		return "", "", nil, errors.New(errors.CodePlanInvalid, "trace_id is required") // Surface invalid terminal callbacks before any lease or DAO work begins.
	}
	if strings.TrimSpace(req.WorkerID) == "" { // Reject empty worker identifiers because distributed terminal ownership is keyed by worker id and attempt together.
		return "", "", nil, errors.New(errors.CodePlanInvalid, "worker_id is required") // Surface invalid terminal callbacks before any lease or DAO work begins.
	}
	if req.Attempt <= 0 { // Reject non-positive attempts because distributed ownership starts at attempt 1 after worker acceptance.
		return "", "", nil, errors.New(errors.CodePlanInvalid, "attempt must be > 0") // Surface invalid terminal callbacks before any lease or DAO work begins.
	}

	switch strings.ToLower(strings.TrimSpace(req.FinalStatus)) { // Normalize the worker-reported terminal status into the orchestrator lifecycle model.
	case "completed":
		return "completed", "completed", nil, nil // Treat successful worker completion as the canonical completed terminal outcome.
	case "cancelled":
		if strings.TrimSpace(req.ErrorMessage) == "" { // Keep cancellation noise low when the worker did not report an extra error string.
			return "cancelled", "cancelled", nil, nil // Treat clean worker cancellation as the canonical cancelled terminal outcome.
		}
		return "cancelled", "cancelled", errors.New(errors.CodeStateConflict, strings.TrimSpace(req.ErrorMessage)), nil // Preserve the worker cancellation reason in the final payload when one was supplied.
	case "failed":
		terminalReason := strings.TrimSpace(req.TerminalReason) // Reuse any caller-supplied structured terminal reason before falling back to the default distributed failure reason.
		if terminalReason == "" {                               // Default every unspecified distributed worker failure to the generic execution_failed reason.
			terminalReason = "execution_failed" // Keep the stored terminal reason deterministic for distributed worker-side failures.
		}
		if strings.TrimSpace(req.ErrorCode) != "" { // Preserve the worker-supplied structured error code whenever one is available.
			return "failed", terminalReason, errors.New(errors.ErrorCode(strings.TrimSpace(req.ErrorCode)), strings.TrimSpace(req.ErrorMessage)), nil // Rebuild the repository-standard coded error so payloads and transports keep the stable code.
		}
		if strings.TrimSpace(req.ErrorMessage) != "" { // Preserve the worker-supplied failure message even when no structured error code accompanied it.
			return "failed", terminalReason, errors.New(errors.CodeInternal, strings.TrimSpace(req.ErrorMessage)), nil // Wrap uncoded worker failures as internal errors for payload consistency.
		}
		return "failed", terminalReason, nil, nil // Treat message-less worker failures as generic terminal failures without an extra error payload field.
	default:
		return "", "", nil, errors.New(errors.CodePlanInvalid, "final_status must be one of: completed, failed, cancelled") // Reject unsupported distributed terminal states before any DAO finalization begins.
	}
}
