package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/telemetry"
	"mcp_for_appium/internal/worker/appium"
)

// GRPCServer implements rpc.WorkerServiceServer.
// Each incoming ExecutePlan call spawns a goroutine that creates its own
// Appium client and Executor so that multiple plans can run concurrently.
type GRPCServer struct {
	appiumURL   string
	stepTimeout time.Duration
	autoWaitMax time.Duration
	workerID    string
	reporter    distributedPlanReporter
	renewEvery  time.Duration
	newClient   func(string) sessionAwareAppiumClient
	ensureReady func(context.Context) error

	planMu     sync.Mutex
	planCancel map[string]context.CancelFunc

	logger *slog.Logger
}

// distributedPlanReporter exposes the worker-to-orchestrator callback RPCs needed by distributed execution.
type distributedPlanReporter interface {
	ReportPlanEvent(ctx context.Context, req *rpc.ReportPlanEventRequest) (*rpc.ReportPlanEventResponse, error)
	RenewLease(ctx context.Context, req *rpc.RenewLeaseRequest) (*rpc.RenewLeaseResponse, error)
	CompletePlan(ctx context.Context, req *rpc.CompletePlanRequest) (*rpc.CompletePlanResponse, error)
}

// sessionAwareAppiumClient extends the executor AppiumClient contract with the ability to attach to an existing Appium session id.
type sessionAwareAppiumClient interface {
	AppiumClient
	AttachSession(sessionID string)
}

// NewGRPCServer creates a new worker gRPC service handler.
func NewGRPCServer(appiumURL string, stepTimeout, autoWaitMax time.Duration, workerID string, reporter distributedPlanReporter, renewEvery time.Duration) *GRPCServer {
	if renewEvery <= 0 { // Fall back to one conservative default renewal cadence when the caller omits an explicit interval.
		renewEvery = 5 * time.Second // Keep lease renewals frequent enough that the orchestrator can detect lost ownership promptly.
	}
	return &GRPCServer{
		appiumURL:   appiumURL,
		stepTimeout: stepTimeout,
		autoWaitMax: autoWaitMax,
		workerID:    workerID,
		reporter:    reporter,
		renewEvery:  renewEvery,
		newClient: func(url string) sessionAwareAppiumClient { // Build the default Appium client factory once so tests can swap it with a stub easily.
			return appium.NewClient(url) // Return the production Appium HTTP client used by distributed worker execution.
		},
		ensureReady: nil, // Leave Appium readiness unmanaged by default so callers can opt into local auto-start explicitly from process wiring.
		planCancel:  make(map[string]context.CancelFunc),
		logger:      telemetry.Logger(),
	}
}

// SetAppiumReadyFunc installs one optional readiness function that is invoked before each distributed plan execution starts.
func (s *GRPCServer) SetAppiumReadyFunc(ensure func(context.Context) error) {
	s.ensureReady = ensure // Replace the per-plan Appium readiness hook so the caller can inject local auto-start or dependency-probe behavior.
}

// ActiveLoad returns the number of plans currently executing on this worker.
func (s *GRPCServer) ActiveLoad() int {
	s.planMu.Lock()
	defer s.planMu.Unlock()
	return len(s.planCancel)
}

// ExecutePlan implements rpc.WorkerServiceServer.
// The plan is executed asynchronously; this method returns "accepted" immediately.
func (s *GRPCServer) ExecutePlan(_ context.Context, req *rpc.ExecutePlanRequest) (*rpc.ExecutePlanResponse, error) {
	s.logger.Info("received ExecutePlan", "trace_id", req.TraceID, "session_id", req.SessionID, "attempt", req.Attempt) // Log the distributed execute request together with the reserved attempt now owning the trace.
	go s.runPlan(req.TraceID, req.SessionID, req.Attempt, req.Plan)                                                     // Execute the plan asynchronously so the RPC can acknowledge acceptance immediately.
	return &rpc.ExecutePlanResponse{Status: "accepted"}, nil
}

// CancelPlan implements rpc.WorkerServiceServer.
func (s *GRPCServer) CancelPlan(_ context.Context, req *rpc.CancelPlanRequest) (*rpc.CancelPlanResponse, error) {
	s.planMu.Lock()
	cancel, ok := s.planCancel[req.TraceID]
	s.planMu.Unlock()

	if !ok {
		return &rpc.CancelPlanResponse{Status: "not_found"}, nil
	}
	cancel()
	s.logger.Info("plan cancelled", "trace_id", req.TraceID)
	return &rpc.CancelPlanResponse{Status: "cancelled"}, nil
}

// runPlan executes this operation.
func (s *GRPCServer) runPlan(traceID string, sessionID string, attempt int64, planRaw json.RawMessage) {
	ctx, cancel := context.WithCancel(context.Background()) // Create the per-plan root context so worker-side cancel RPCs and lost-lease cancellation can stop execution cleanly.
	s.planMu.Lock()
	s.planCancel[traceID] = cancel // Register the per-trace cancel function so later worker cancel RPCs can stop the active run cleanly.
	s.planMu.Unlock()
	defer func() {
		cancel() // Release the execution context so any renewal loop or nested timeouts stop promptly on every exit path.
		s.planMu.Lock()
		delete(s.planCancel, traceID) // Remove the trace from the in-flight plan map so ActiveLoad drops immediately after execution stops.
		s.planMu.Unlock()
	}()

	if s.reporter != nil { // Start the lease-renewal loop only when the worker has a wired orchestrator callback client.
		go s.renewLeaseLoop(ctx, cancel, traceID, attempt) // Refresh the distributed ownership lease in the background until this run exits or loses ownership.
	}
	if s.ensureReady != nil { // Run the optional Appium readiness hook before parsing or executing the plan so local Appium auto-start can happen on demand per plan.
		if err := s.ensureReady(ctx); err != nil { // Stop immediately when the configured Appium dependency is still unavailable after the readiness hook runs.
			s.logger.Error("failed to ensure worker appium service", "trace_id", traceID, "attempt", attempt, "error", err)             // Log the worker-side Appium bootstrap failure so operators can diagnose local automation dependency issues quickly.
			s.reportCompletion(traceID, attempt, "failed", "appium_unavailable", "trace failed before worker appium became ready", err) // Report one failed terminal result so the orchestrator closes the distributed trace instead of leaving it running forever.
			return                                                                                                                      // Stop execution because no distributed plan can run without one reachable Appium dependency.
		}
	}

	steps, err := ParsePlan(planRaw)
	if err != nil {
		s.logger.Error("failed to parse plan", "trace_id", traceID, "attempt", attempt, "error", err)               // Log plan-parse failures together with the distributed attempt for operator diagnosis.
		s.reportCompletion(traceID, attempt, "failed", "parse_error", "trace failed before execution started", err) // Report the parse failure back to the orchestrator so the distributed trace reaches a terminal state.
		return                                                                                                      // Stop execution because the worker cannot run an invalid distributed plan.
	}

	client := s.newClient(s.appiumURL) // Construct or inject one Appium client bound to the configured worker-side Appium endpoint.
	if sessionID != "" {               // Attach the worker-side Appium client to the orchestrator-created session whenever one has been supplied.
		client.AttachSession(sessionID) // Reuse the distributed session instead of creating a new Appium session inside the worker.
	}
	exec := NewExecutor(client, s.stepTimeout, s.autoWaitMax, func(ev StepEvent) {
		s.logger.Info("step event",
			"trace_id", traceID,
			"attempt", attempt,
			"step_index", ev.StepIndex,
			"status", ev.Status,
			"message", ev.Message)
		s.reportStepEvent(traceID, attempt, ev) // Forward each worker-emitted step event to the orchestrator so distributed traces accumulate replayable events too.
	})

	if err := exec.Execute(ctx, steps); err != nil {
		s.logger.Error("plan execution failed", "trace_id", traceID, "attempt", attempt, "error", err) // Log worker-side execution failures together with the distributed attempt for operator diagnosis.
		if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {                    // Treat worker-side context cancellation as the canonical distributed cancellation outcome.
			s.reportCompletion(traceID, attempt, "cancelled", "cancelled", "trace cancelled during distributed execution", err) // Report the cancelled terminal outcome so the orchestrator can close the trace cleanly.
			return                                                                                                              // Stop after reporting the distributed cancellation outcome.
		}
		s.reportCompletion(traceID, attempt, "failed", distributedFailureReason(err), "trace failed during distributed execution", err) // Report the failed terminal outcome so the orchestrator can close the trace cleanly.
		return                                                                                                                          // Stop after reporting the distributed failure outcome.
	}
	s.logger.Info("plan execution completed", "trace_id", traceID, "attempt", attempt)     // Log worker-side completion together with the distributed attempt for operator visibility.
	s.reportCompletion(traceID, attempt, "completed", "completed", "trace completed", nil) // Report the completed terminal outcome so the orchestrator can close the distributed trace cleanly.
}

// renewLeaseLoop keeps the distributed execution lease alive while the current worker attempt still owns the trace.
func (s *GRPCServer) renewLeaseLoop(ctx context.Context, cancel context.CancelFunc, traceID string, attempt int64) {
	ticker := time.NewTicker(s.renewEvery) // Create one renewal ticker so lease refresh RPCs happen at a steady cadence while the run remains active.
	defer ticker.Stop()                    // Release the renewal ticker resources when the current run exits or loses ownership.

	for { // Keep renewing the lease until the run exits or the orchestrator reports that ownership has gone stale.
		select {
		case <-ctx.Done():
			return // Stop renewing once the run has exited because ownership no longer needs to be refreshed.
		case <-ticker.C:
			if s.reporter == nil { // Guard against a nil reporter even though the caller only starts this loop when a reporter is configured.
				return // Stop renewing because there is no orchestrator callback client available anymore.
			}
			renewCtx, renewCancel := context.WithTimeout(context.Background(), 5*time.Second)                                              // Bound each lease-renewal RPC so a stalled orchestrator connection cannot hang the renewal loop forever.
			resp, err := s.reporter.RenewLease(renewCtx, &rpc.RenewLeaseRequest{TraceID: traceID, WorkerID: s.workerID, Attempt: attempt}) // Ask the orchestrator to refresh the current distributed ownership lease for this worker attempt.
			renewCancel()                                                                                                                  // Release the per-renewal timeout resources immediately after the RPC returns.
			if err != nil {                                                                                                                // Log transient renewal RPC failures while allowing the next tick to retry before the orchestrator lease expires.
				s.logger.Warn("failed to renew distributed execution lease", "trace_id", traceID, "attempt", attempt, "error", err) // Surface renewal transport failures so operators can correlate future orphan closures with worker-side renewal issues.
				continue                                                                                                            // Keep retrying on the next renewal tick because the lease may still be salvageable before expiry.
			}
			if resp != nil && resp.Stale { // Cancel the local run immediately once the orchestrator reports that this worker attempt no longer owns the trace.
				s.logger.Info("distributed execution lease became stale", "trace_id", traceID, "attempt", attempt) // Record the ownership loss so worker-side cancellation has a clear reason in logs.
				cancel()                                                                                           // Stop the local run because it no longer owns the distributed trace.
				return                                                                                             // Exit the renewal loop after ownership has been lost.
			}
		}
	}
}

// reportStepEvent forwards one worker-emitted step event to the orchestrator when the distributed callback client is configured.
func (s *GRPCServer) reportStepEvent(traceID string, attempt int64, ev StepEvent) {
	if s.reporter == nil { // Skip distributed callbacks entirely when the worker has no orchestrator callback client wired in.
		return // Preserve the local-only behavior for tests or legacy call sites that omit the reporter.
	}

	reportCtx, reportCancel := context.WithTimeout(context.Background(), 5*time.Second) // Bound each step-event callback so a stalled orchestrator connection cannot block worker execution forever.
	defer reportCancel()                                                                // Release the per-callback timeout resources immediately after the RPC returns.
	resp, err := s.reporter.ReportPlanEvent(reportCtx, &rpc.ReportPlanEventRequest{     // Forward the worker-emitted step event using the distributed callback RPC contract.
		TraceID:      traceID,                                                                                                         // Target the same trace currently executing on this worker.
		WorkerID:     s.workerID,                                                                                                      // Identify the worker instance that emitted the current distributed step event.
		Attempt:      attempt,                                                                                                         // Attach the reserved distributed attempt so the orchestrator can reject stale callbacks safely.
		StepIndex:    ev.StepIndex,                                                                                                    // Preserve the worker-reported step index for replay ordering and diagnostics.
		Status:       ev.Status,                                                                                                       // Preserve the worker-reported step status for replay filtering and diagnostics.
		Message:      ev.Message,                                                                                                      // Preserve the human-readable step message emitted by the executor.
		Phase:        ev.Phase,                                                                                                        // Preserve the optional worker-reported phase field for richer replay consumers.
		ArtifactRefs: ev.ArtifactRefs,                                                                                                 // Preserve any artifact references emitted by the worker-side executor.
		Metrics:      rpc.PlanEventMetrics{Attempt: ev.Metrics.Attempt, WDCalls: ev.Metrics.WDCalls, ElapsedMs: ev.Metrics.ElapsedMs}, // Preserve the worker executor metrics in the distributed callback payload.
	})
	if err != nil { // Log step-event callback failures without aborting the active worker run because later terminal callbacks can still close the trace.
		s.logger.Warn("failed to report distributed step event", "trace_id", traceID, "attempt", attempt, "step_index", ev.StepIndex, "error", err) // Surface callback failures so operators can correlate missing replay events with worker-side transport issues.
		return                                                                                                                                      // Keep the worker run alive because step-event loss alone should not abort execution.
	}
	if resp != nil && resp.Stale { // Log stale callback rejections so late worker events remain visible without aborting the current run immediately.
		s.logger.Info("distributed step event rejected as stale", "trace_id", traceID, "attempt", attempt, "step_index", ev.StepIndex) // Record the stale event rejection for operator diagnosis.
	}
}

// reportCompletion forwards one worker terminal result to the orchestrator when the distributed callback client is configured.
func (s *GRPCServer) reportCompletion(traceID string, attempt int64, finalStatus string, terminalReason string, message string, runErr error) {
	if s.reporter == nil { // Skip distributed terminal callbacks entirely when the worker has no orchestrator callback client wired in.
		return // Preserve the local-only behavior for tests or legacy call sites that omit the reporter.
	}

	completionRequest := &rpc.CompletePlanRequest{ // Build the distributed terminal callback request once so optional error metadata can be attached consistently.
		TraceID:        traceID,        // Target the same trace currently executing on this worker.
		WorkerID:       s.workerID,     // Identify the worker instance reporting the current terminal distributed outcome.
		Attempt:        attempt,        // Attach the reserved distributed attempt so the orchestrator can reject stale terminal callbacks safely.
		FinalStatus:    finalStatus,    // Preserve the worker-reported terminal lifecycle state for orchestrator finalization.
		TerminalReason: terminalReason, // Preserve the structured terminal reason so the orchestrator can store it on the trace row and final payload.
		Message:        message,        // Preserve the human-readable terminal message emitted by the worker-side execution path.
	}
	if runErr != nil { // Attach optional structured error metadata only when the worker terminal path ended with an error.
		completionRequest.ErrorMessage = runErr.Error() // Preserve the worker-side terminal error string for final trace payloads and operator diagnosis.
		if code, ok := errors.CodeOf(runErr); ok {      // Attach the stable repository error code whenever the worker failure used the standardized error wrapper.
			completionRequest.ErrorCode = string(code) // Preserve the machine-readable error code for final payloads and higher-layer diagnostics.
		}
	}

	reportCtx, reportCancel := context.WithTimeout(context.Background(), 10*time.Second) // Bound the terminal callback so a stalled orchestrator connection cannot hang worker shutdown forever.
	defer reportCancel()                                                                 // Release the per-callback timeout resources immediately after the RPC returns.
	resp, err := s.reporter.CompletePlan(reportCtx, completionRequest)                   // Forward the worker terminal result to the orchestrator so the distributed trace can be finalized durably.
	if err != nil {                                                                      // Log terminal-callback failures because the watchdog will have to orphan or time out the trace later.
		s.logger.Warn("failed to report distributed completion", "trace_id", traceID, "attempt", attempt, "final_status", finalStatus, "error", err) // Surface terminal callback transport failures so operators can correlate later orphan closures with worker-side issues.
		return                                                                                                                                       // Stop after logging because the watchdog remains the fallback closer for this trace.
	}
	if resp != nil && resp.Stale { // Log stale terminal callback rejections so late worker completions remain visible without overriding newer state.
		s.logger.Info("distributed completion rejected as stale", "trace_id", traceID, "attempt", attempt, "final_status", finalStatus) // Record the stale terminal rejection for operator diagnosis.
	}
}

// distributedFailureReason maps one worker-side execution error to the structured terminal reason stored on the distributed trace.
func distributedFailureReason(err error) string {
	if errors.IsCode(err, errors.CodeSessionDead) || errors.IsCode(err, errors.CodeSessionBroken) { // Classify broken or dead Appium sessions as one session-level distributed failure reason.
		return "session_broken" // Store the session-broken terminal reason so operators can distinguish it from generic step failures.
	}
	if errors.IsCode(err, errors.CodePlanInvalid) { // Preserve invalid distributed plans under the same parse-error reason used by pre-execution failures.
		return "parse_error" // Store the parse-error terminal reason so replay and diagnostics stay consistent with worker plan validation failures.
	}

	return "execution_failed" // Fall back to the generic distributed execution-failed reason for all other worker-side terminal errors.
}
