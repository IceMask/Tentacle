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

	server.runPlan("trace-1", "appium-session-1", 7, json.RawMessage(`[{"type":"wait","params":{"ms":1}}]`)) // Execute one tiny distributed plan synchronously so the test can assert its callback side effects immediately.

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

	server.runPlan("trace-2", "appium-session-2", 8, json.RawMessage(`[{"type":"wait","params":{"ms":1}}]`)) // Execute one tiny plan synchronously so the worker failure path can be asserted immediately.

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
