// grpc_server_test.go verifies that the distributed worker gRPC server attaches existing sessions and reports callbacks back to the orchestrator client.
package worker

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	apperrors "mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/rpc"
)

// fakeSessionAwareAppiumClient records AttachSession calls while satisfying the executor Appium client contract used by the worker gRPC server.
type fakeSessionAwareAppiumClient struct {
	attachedSession string
}

// AttachSession records the attached Appium session id so the test can assert that distributed worker execution reuses the orchestrator-created session.
func (f *fakeSessionAwareAppiumClient) AttachSession(sessionID string) {
	f.attachedSession = sessionID // Record the attached Appium session id exactly as the worker gRPC server passed it.
}

// FindElement returns one deterministic element id because the wait-only test plan never needs real Appium lookups.
func (f *fakeSessionAwareAppiumClient) FindElement(ctx context.Context, strategy, selector string) (string, error) {
	return "element-1", nil // Return one deterministic fake element id so any unexpected lookup still succeeds during the test.
}

// Click returns success immediately because the wait-only test plan never needs real click behavior.
func (f *fakeSessionAwareAppiumClient) Click(ctx context.Context, elementID string) error {
	return nil // Return success so any unexpected click still keeps the worker test focused on callback wiring.
}

// SendKeys returns success immediately because the wait-only test plan never needs real text-entry behavior.
func (f *fakeSessionAwareAppiumClient) SendKeys(ctx context.Context, elementID, text string) error {
	return nil // Return success so any unexpected sendKeys still keeps the worker test focused on callback wiring.
}

// Clear returns success immediately because the wait-only test plan never needs real clear-element behavior.
func (f *fakeSessionAwareAppiumClient) Clear(ctx context.Context, elementID string) error {
	return nil // Return success so any unexpected clear still keeps the worker test focused on callback wiring.
}

// Screenshot returns one deterministic fake image payload because the wait-only test plan never needs real screenshot behavior.
func (f *fakeSessionAwareAppiumClient) Screenshot(ctx context.Context) ([]byte, error) {
	return []byte("fake-screenshot"), nil // Return one fake screenshot payload so any unexpected failure-path capture still succeeds during the test.
}

// PageSource returns one deterministic fake page source because the wait-only test plan never needs real page-source behavior.
func (f *fakeSessionAwareAppiumClient) PageSource(ctx context.Context) (string, error) {
	return "<hierarchy/>", nil // Return one fake page source payload so any unexpected snapshot read still succeeds during the test.
}

// Tap returns success immediately because the wait-only test plan never needs real tap behavior.
func (f *fakeSessionAwareAppiumClient) Tap(ctx context.Context, x int, y int) error {
	return nil // Return success so any unexpected tap still keeps the worker test focused on callback wiring.
}

// Swipe returns success immediately because the wait-only test plan never needs real swipe behavior.
func (f *fakeSessionAwareAppiumClient) Swipe(ctx context.Context, x1 int, y1 int, x2 int, y2 int, durationMs int) error {
	return nil // Return success so any unexpected swipe still keeps the worker test focused on callback wiring.
}

// LongPress returns success immediately because the wait-only test plan never needs real long-press behavior.
func (f *fakeSessionAwareAppiumClient) LongPress(ctx context.Context, elementID string, durationMs int) error {
	return nil // Return success so any unexpected long press still keeps the worker test focused on callback wiring.
}

// Back returns success immediately because the wait-only test plan never needs real back-button behavior.
func (f *fakeSessionAwareAppiumClient) Back(ctx context.Context) error {
	return nil // Return success so any unexpected back navigation still keeps the worker test focused on callback wiring.
}

// HideKeyboard returns success immediately because the wait-only test plan never needs real keyboard-dismiss behavior.
func (f *fakeSessionAwareAppiumClient) HideKeyboard(ctx context.Context) error {
	return nil // Return success so any unexpected hide-keyboard action still keeps the worker test focused on callback wiring.
}

// fakeDistributedReporter records distributed worker callbacks emitted by the worker gRPC server under test.
type fakeDistributedReporter struct {
	mu             sync.Mutex
	reportedEvents []*rpc.ReportPlanEventRequest
	renewRequests  []*rpc.RenewLeaseRequest
	completions    []*rpc.CompletePlanRequest
}

// ReportPlanEvent records the distributed step-event callback so the test can assert the worker emitted replayable events.
func (f *fakeDistributedReporter) ReportPlanEvent(ctx context.Context, req *rpc.ReportPlanEventRequest) (*rpc.ReportPlanEventResponse, error) {
	f.mu.Lock()                                              // Protect the recorded callback slices because worker callbacks may be emitted from multiple goroutines.
	defer f.mu.Unlock()                                      // Release the fake reporter lock immediately after the callback request has been recorded.
	f.reportedEvents = append(f.reportedEvents, req)         // Record the step-event callback exactly as the worker gRPC server emitted it.
	return &rpc.ReportPlanEventResponse{Accepted: true}, nil // Acknowledge the callback so the worker continues execution normally.
}

// RenewLease records the distributed lease-renewal callback so the test can assert that the worker attempts to keep ownership alive.
func (f *fakeDistributedReporter) RenewLease(ctx context.Context, req *rpc.RenewLeaseRequest) (*rpc.RenewLeaseResponse, error) {
	f.mu.Lock()                                         // Protect the recorded callback slices because worker callbacks may be emitted from multiple goroutines.
	defer f.mu.Unlock()                                 // Release the fake reporter lock immediately after the callback request has been recorded.
	f.renewRequests = append(f.renewRequests, req)      // Record the lease-renewal callback exactly as the worker gRPC server emitted it.
	return &rpc.RenewLeaseResponse{Accepted: true}, nil // Acknowledge the lease renewal so the worker keeps executing normally.
}

// CompletePlan records the distributed terminal callback so the test can assert the worker emitted a final result.
func (f *fakeDistributedReporter) CompletePlan(ctx context.Context, req *rpc.CompletePlanRequest) (*rpc.CompletePlanResponse, error) {
	f.mu.Lock()                                           // Protect the recorded callback slices because worker callbacks may be emitted from multiple goroutines.
	defer f.mu.Unlock()                                   // Release the fake reporter lock immediately after the callback request has been recorded.
	f.completions = append(f.completions, req)            // Record the terminal callback exactly as the worker gRPC server emitted it.
	return &rpc.CompletePlanResponse{Accepted: true}, nil // Acknowledge the terminal callback so the worker can exit normally.
}

// TestRunPlanAttachesSessionAndReportsCallbacks verifies that distributed worker execution reuses the orchestrator-created Appium session and emits event plus completion callbacks.
func TestRunPlanAttachesSessionAndReportsCallbacks(t *testing.T) {
	reporter := &fakeDistributedReporter{}                                                                        // Construct one fake distributed reporter so the worker callback wiring can be asserted deterministically.
	appiumClient := &fakeSessionAwareAppiumClient{}                                                               // Construct one fake Appium client so the test can assert session attachment without real HTTP traffic.
	server := NewGRPCServer("http://unused-appium", time.Second, time.Second, "worker-test", reporter, time.Hour) // Construct the worker gRPC server with a long renewal interval so the short test run does not need to observe renewals to pass.
	server.newClient = func(url string) sessionAwareAppiumClient {                                                // Replace the production Appium client factory with the fake session-aware client for this unit test.
		return appiumClient // Return the deterministic fake Appium client so the worker run stays fully in-process.
	}

	response, err := server.ExecutePlan(context.Background(), &rpc.ExecutePlanRequest{TraceID: "trace-1", SessionID: "appium-session-1", Attempt: 7, Plan: json.RawMessage(`[{"type":"wait","params":{"ms":1}}]`)}) // Admit one tiny plan through the production synchronous worker admission boundary.
	if err != nil {                                                                                                                                                                                                 // Fail when the in-process worker RPC unexpectedly returns a transport-level error.
		t.Fatalf("expected worker plan admission to succeed, got error: %v", err) // Surface admission failures because callback assertions require an accepted run.
	}
	if response.Status != "accepted" { // Fail when the worker rejects a valid plan despite available capacity.
		t.Fatalf("expected worker plan status accepted, got %q", response.Status) // Surface the unexpected admission outcome for diagnosis.
	}
	waitForWorkerCompletions(t, reporter, 1) // Wait until asynchronous execution emits its terminal callback and releases worker capacity.

	if appiumClient.attachedSession != "appium-session-1" { // Fail the test when the worker did not attach to the orchestrator-created Appium session before execution.
		t.Fatalf("expected attached session appium-session-1, got %q", appiumClient.attachedSession) // Surface the unexpected attached session because distributed execution must reuse the existing session.
	}

	reporter.mu.Lock()         // Snapshot the recorded callback state under the fake reporter lock so concurrent callback writes cannot race with assertions.
	defer reporter.mu.Unlock() // Release the fake reporter lock after the callback assertions complete.

	if len(reporter.reportedEvents) != 2 { // Fail the test when the worker did not emit both the running and passed step callbacks for the wait step.
		t.Fatalf("expected 2 distributed step events, got %d", len(reporter.reportedEvents)) // Surface the unexpected event count because the worker must emit replayable step callbacks.
	}
	if reporter.reportedEvents[0].Attempt != 7 || reporter.reportedEvents[1].Attempt != 7 { // Fail the test when the worker does not preserve the distributed attempt across step callbacks.
		t.Fatalf("expected distributed step events to preserve attempt 7, got %#v and %#v", reporter.reportedEvents[0].Attempt, reporter.reportedEvents[1].Attempt) // Surface the unexpected attempt propagation because stale-result protection depends on it.
	}
	if len(reporter.completions) != 1 { // Fail the test when the worker did not emit exactly one terminal callback after execution completed.
		t.Fatalf("expected 1 distributed completion callback, got %d", len(reporter.completions)) // Surface the unexpected completion count because distributed traces must end with one terminal callback.
	}
	if reporter.completions[0].FinalStatus != "completed" { // Fail the test when the worker did not report the completed terminal outcome for the successful wait-only plan.
		t.Fatalf("expected distributed completion status completed, got %q", reporter.completions[0].FinalStatus) // Surface the unexpected terminal status because distributed completion must mirror execution success.
	}
	if reporter.completions[0].Attempt != 7 { // Fail the test when the worker terminal callback does not preserve the reserved attempt number.
		t.Fatalf("expected distributed completion attempt 7, got %d", reporter.completions[0].Attempt) // Surface the unexpected attempt because stale-result protection depends on it.
	}
}

// TestRunPlanFailsWhenAppiumReadyHookFails verifies that distributed worker execution reports one failed terminal result when the injected Appium readiness hook cannot make Appium available.
func TestRunPlanFailsWhenAppiumReadyHookFails(t *testing.T) {
	reporter := &fakeDistributedReporter{}                                                                        // Construct one fake distributed reporter so the worker terminal callback can be asserted deterministically.
	server := NewGRPCServer("http://unused-appium", time.Second, time.Second, "worker-test", reporter, time.Hour) // Construct the worker gRPC server with one long renewal interval so the short failure path does not depend on lease-renewal timing.
	ensureReadyCalls := 0                                                                                         // Track how many times the worker invoked the injected Appium readiness hook before attempting plan execution.
	server.SetAppiumReadyFunc(func(ctx context.Context) error {                                                   // Inject one deterministic Appium readiness failure so the worker must stop before plan parsing or execution begins.
		ensureReadyCalls++                                                   // Count the readiness invocation so the test can prove the worker actually ran the injected hook.
		return apperrors.New(apperrors.CodeHealthDown, "appium unavailable") // Return one stable dependency-health failure so the worker must terminalize the trace immediately.
	}) // Close the injected readiness hook installation.
	server.newClient = func(url string) sessionAwareAppiumClient { // Replace the production Appium client factory with one panic stub because the readiness failure path must stop before any client is created.
		t.Fatal("expected appium client factory not to be called when readiness hook fails") // Fail immediately when execution reaches Appium client creation despite the injected readiness failure.
		return nil                                                                           // Return nil only to satisfy the compiler because the fatal assertion above should abort the test first.
	} // Close the panic client-factory override.

	response, err := server.ExecutePlan(context.Background(), &rpc.ExecutePlanRequest{TraceID: "trace-2", SessionID: "appium-session-2", Attempt: 8, Plan: json.RawMessage(`[{"type":"wait","params":{"ms":1}}]`)}) // Admit one tiny plan through the production worker boundary so readiness failure runs asynchronously.
	if err != nil {                                                                                                                                                                                                 // Fail when the in-process worker RPC unexpectedly returns a transport-level error.
		t.Fatalf("expected worker plan admission to succeed, got error: %v", err) // Surface admission failures because readiness assertions require the run to start.
	}
	if response.Status != "accepted" { // Fail when the worker rejects a valid plan before invoking the readiness hook.
		t.Fatalf("expected worker plan status accepted, got %q", response.Status) // Surface the unexpected admission outcome for diagnosis.
	}
	waitForWorkerCompletions(t, reporter, 1) // Wait until the asynchronous readiness failure emits its terminal callback.

	if ensureReadyCalls != 1 { // Fail the test when the worker did not invoke the readiness hook exactly once before deciding the trace outcome.
		t.Fatalf("expected readiness hook to be called once, got %d", ensureReadyCalls) // Surface the unexpected readiness-hook call count because local Appium auto-start depends on this hook firing reliably.
	}

	reporter.mu.Lock()         // Snapshot the recorded callback state under the fake reporter lock so concurrent callback writes cannot race with assertions.
	defer reporter.mu.Unlock() // Release the fake reporter lock after the callback assertions complete.

	if len(reporter.reportedEvents) != 0 { // Fail the test when the worker emitted step events even though Appium readiness failed before execution began.
		t.Fatalf("expected 0 distributed step events after readiness failure, got %d", len(reporter.reportedEvents)) // Surface the unexpected event count because no plan execution should begin after readiness failure.
	}
	if len(reporter.completions) != 1 { // Fail the test when the worker did not emit exactly one terminal callback for the readiness failure path.
		t.Fatalf("expected 1 distributed completion callback after readiness failure, got %d", len(reporter.completions)) // Surface the unexpected completion count because the trace must reach one terminal state.
	}
	if reporter.completions[0].FinalStatus != "failed" { // Fail the test when the worker did not report the failed terminal outcome for the readiness failure path.
		t.Fatalf("expected distributed completion status failed, got %q", reporter.completions[0].FinalStatus) // Surface the unexpected terminal status because readiness failure must close the trace as failed.
	}
	if reporter.completions[0].TerminalReason != "appium_unavailable" { // Fail the test when the worker did not preserve the dedicated terminal reason for Appium readiness failure.
		t.Fatalf("expected distributed terminal reason appium_unavailable, got %q", reporter.completions[0].TerminalReason) // Surface the unexpected terminal reason because orchestrator diagnosis depends on it.
	}
	if reporter.completions[0].Attempt != 8 { // Fail the test when the worker terminal callback does not preserve the reserved attempt number on readiness failure.
		t.Fatalf("expected distributed completion attempt 8, got %d", reporter.completions[0].Attempt) // Surface the unexpected attempt because stale-result protection still applies on failure paths.
	}
}

// TestExecutePlanEnforcesCapacityAndDeduplicatesAttempts verifies that admission rejects excess work and never starts an exact active or completed attempt twice.
func TestExecutePlanEnforcesCapacityAndDeduplicatesAttempts(t *testing.T) {
	reporter := &fakeDistributedReporter{}                                                                               // Record callbacks so the test can prove only one physical execution completed.
	server := NewGRPCServer("http://unused-appium", time.Second, time.Second, "worker-capacity", reporter, time.Hour, 1) // Construct one worker with exactly one authoritative execution slot.
	server.newClient = func(url string) sessionAwareAppiumClient {                                                       // Replace production Appium traffic with one deterministic in-process stub.
		return &fakeSessionAwareAppiumClient{} // Return an isolated fake client for the single admitted wait plan.
	}
	request := &rpc.ExecutePlanRequest{TraceID: "trace-capacity-1", SessionID: "session-capacity-1", Attempt: 3, Plan: json.RawMessage(`[{"type":"wait","params":{"ms":100}}]`)} // Build one slow-enough valid plan so admission state remains active during duplicate and capacity checks.
	firstResponse, err := server.ExecutePlan(context.Background(), request)                                                                                                      // Admit the first trace into the worker's only slot.
	if err != nil {                                                                                                                                                              // Fail when in-process admission unexpectedly returns a transport error.
		t.Fatalf("expected first worker admission to succeed, got error: %v", err) // Surface the admission failure because the rest of the capacity scenario depends on an active run.
	}
	if firstResponse.Status != "accepted" { // Fail when the available worker slot does not admit the first plan.
		t.Fatalf("expected first admission status accepted, got %q", firstResponse.Status) // Surface the unexpected admission result.
	}

	duplicateResponse, err := server.ExecutePlan(context.Background(), request) // Replay the exact active trace attempt while its first goroutine still owns capacity.
	if err != nil {                                                             // Fail when duplicate admission unexpectedly returns a transport error.
		t.Fatalf("expected duplicate active admission to return cleanly, got error: %v", err) // Surface the duplicate handling failure.
	}
	if duplicateResponse.Status != "accepted" { // Require idempotent acknowledgement for exact response-loss replays.
		t.Fatalf("expected duplicate active admission status accepted, got %q", duplicateResponse.Status) // Surface any rejection that could make the orchestrator retry dangerously.
	}

	busyResponse, err := server.ExecutePlan(context.Background(), &rpc.ExecutePlanRequest{TraceID: "trace-capacity-2", SessionID: "session-capacity-2", Attempt: 1, Plan: json.RawMessage(`[{"type":"wait","params":{"ms":1}}]`)}) // Attempt a different trace while the only slot remains occupied.
	if err != nil {                                                                                                                                                                                                                // Fail when capacity rejection unexpectedly uses a transport error.
		t.Fatalf("expected busy admission to return cleanly, got error: %v", err) // Surface the capacity handling failure.
	}
	if busyResponse.Status != "busy" { // Require the stable transient status understood by dispatcher retry logic.
		t.Fatalf("expected second trace admission status busy, got %q", busyResponse.Status) // Surface oversubscription or incorrect terminal rejection.
	}
	if activeLoad := server.ActiveLoad(); activeLoad != 1 { // Prove duplicate and busy calls did not create extra active goroutines.
		t.Fatalf("expected authoritative active load 1, got %d", activeLoad) // Surface worker-side load-accounting drift.
	}

	waitForWorkerCompletions(t, reporter, 1)                                  // Wait for the single physically admitted plan to finish and create its deduplication tombstone.
	completedReplay, err := server.ExecutePlan(context.Background(), request) // Replay the exact attempt after completion while its bounded history remains present.
	if err != nil {                                                           // Fail when completed-attempt deduplication unexpectedly returns a transport error.
		t.Fatalf("expected completed replay to return cleanly, got error: %v", err) // Surface the completed replay handling failure.
	}
	if completedReplay.Status != "accepted" { // Acknowledge the retained completed attempt without executing it again.
		t.Fatalf("expected completed replay status accepted, got %q", completedReplay.Status) // Surface a response that could provoke unsafe retries.
	}
	time.Sleep(20 * time.Millisecond)            // Allow enough time for any accidental duplicate goroutine to emit an observable second completion.
	reporter.mu.Lock()                           // Serialize the final completion-count assertion with callback writes.
	completionCount := len(reporter.completions) // Snapshot how many physical terminal callbacks were emitted.
	reporter.mu.Unlock()                         // Release the reporter lock before evaluating the assertion.
	if completionCount != 1 {                    // Reject any duplicate physical execution across active and completed replays.
		t.Fatalf("expected exactly one worker completion, got %d", completionCount) // Surface at-most-once admission regression explicitly.
	}
	if activeLoad := server.ActiveLoad(); activeLoad != 0 { // Confirm the completed tombstone does not consume execution capacity.
		t.Fatalf("expected authoritative active load 0 after completion, got %d", activeLoad) // Surface a leaked worker slot.
	}
}

// waitForWorkerCompletions waits for the fake reporter to observe the requested terminal callback count without introducing an unbounded test hang.
func waitForWorkerCompletions(t *testing.T, reporter *fakeDistributedReporter, expected int) {
	t.Helper()                                 // Attribute timeout failures to the scenario that requested completion.
	deadline := time.NewTimer(2 * time.Second) // Bound asynchronous worker execution to one short deterministic test window.
	defer deadline.Stop()                      // Release the timeout timer when the expected callback arrives early.
	ticker := time.NewTicker(time.Millisecond) // Poll the mutex-protected fake reporter at a low-cost cadence.
	defer ticker.Stop()                        // Release the polling ticker on success or timeout.
	for {                                      // Continue until the expected terminal callback count appears or the deadline expires.
		reporter.mu.Lock()                           // Serialize the completion count read with callback append operations.
		completionCount := len(reporter.completions) // Snapshot the current terminal callback count.
		reporter.mu.Unlock()                         // Release the reporter lock before blocking on timers.
		if completionCount >= expected {             // Return once asynchronous execution has produced every expected terminal callback.
			return // Allow the caller to assert stable callback and client state.
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d worker completions; got %d", expected, completionCount) // Surface the missing callback count for diagnosis.
		case <-ticker.C:
			// Retry after one millisecond so short wait-only plans finish quickly without a busy loop.
		}
	}
}
