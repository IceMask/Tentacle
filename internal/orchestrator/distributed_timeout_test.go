// distributed_timeout_test.go verifies that accepted distributed traces are promoted to running and eventually finalized when no worker result ever comes back.
package orchestrator

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"mcp_for_appium/internal/rpc"

	"google.golang.org/grpc"
)

// acceptedWorkerService acknowledges every distributed ExecutePlan request immediately and then provides no later completion callback.
type acceptedWorkerService struct{}

// ExecutePlan returns an accepted response immediately so the orchestrator must rely on its in-flight watchdog to close the trace later.
func (acceptedWorkerService) ExecutePlan(ctx context.Context, req *rpc.ExecutePlanRequest) (*rpc.ExecutePlanResponse, error) {
	return &rpc.ExecutePlanResponse{Status: "accepted"}, nil // Return the same accepted contract as the current distributed worker path without any later completion reporting.
}

// CancelPlan returns not_found because this timeout-focused fake worker never tracks in-flight plans beyond the immediate acceptance response.
func (acceptedWorkerService) CancelPlan(ctx context.Context, req *rpc.CancelPlanRequest) (*rpc.CancelPlanResponse, error) {
	return &rpc.CancelPlanResponse{Status: "not_found"}, nil // Return the minimal valid cancel response because this helper exists only for timeout-path testing.
}

// startAcceptedWorkerServer starts one IPv4 loopback worker gRPC server that accepts plans immediately and returns its address plus a shutdown function.
func startAcceptedWorkerServer(t *testing.T) (string, func()) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind one explicit IPv4 loopback listener so the sandbox does not force the test onto blocked IPv6 localhost behavior.
	if err != nil {                                    // Skip the test when even explicit loopback listeners are unavailable in the current environment.
		t.Skipf("skipping distributed timeout test because worker listener setup failed: %v", err) // Surface the environment limitation without misclassifying the production logic as passing.
		return "", func() {}                                                                       // Return zero values after Skipf so the compiler sees a complete return path.
	}

	server := grpc.NewServer()                                       // Create one minimal gRPC server that exposes the fake worker service used by this test.
	rpc.RegisterWorkerServiceServer(server, acceptedWorkerService{}) // Register the fake worker service so dispatcher ExecutePlan RPCs receive an immediate accepted response.
	go func() {
		_ = server.Serve(listener) // Serve the fake worker RPCs in the background so the dispatcher can dial the loopback endpoint during the test.
	}()

	return listener.Addr().String(), func() { // Return the bound worker address together with a cleanup closure for the caller.
		server.GracefulStop() // Stop the fake worker gRPC server once the distributed timeout test completes.
		_ = listener.Close()  // Close the listener explicitly so the loopback port is released immediately after the test.
	}
}

// TestDispatchToWorkerFinalizesAcceptedTraceAfterPlanTimeout verifies that one accepted distributed trace is marked running and then closed with E.TIMEOUT.PLAN when no worker result ever arrives.
func TestDispatchToWorkerFinalizesAcceptedTraceAfterPlanTimeout(t *testing.T) {
	harness := newTraceTerminalHarness(t) // Start one isolated orchestrator harness so this test can exercise real DAO, Redis, and dispatcher timeout behavior.
	defer harness.service.Stop()          // Stop any background dispatcher resources the harness service may have started or allocated once the test finishes.

	_, traceID := harness.seedPendingTrace(t)                 // Seed one pending trace because distributed dispatch should start from the queued lifecycle state.
	workerAddress, stopWorker := startAcceptedWorkerServer(t) // Start one fake worker that immediately accepts the plan but never reports any later completion result.
	defer stopWorker()                                        // Ensure the loopback fake worker is stopped after the test completes.

	if err := harness.service.registry.Register(context.Background(), &WorkerNode{ID: "worker-timeout", Address: workerAddress, Capacity: 1, Tags: map[string]string{}}); err != nil { // Register one healthy worker so dispatcher assignment can choose it deterministically.
		t.Fatalf("failed to register fake worker: %v", err) // Surface worker-registration failures because the distributed dispatch path cannot run without an assignable worker.
	}

	harness.service.dispatcher.executor = nil                      // Force the dispatcher onto the distributed RPC path even though the shared harness defaults to monolith execution mode.
	harness.service.dispatcher.planTimeout = 50 * time.Millisecond // Use a short plan timeout so the watchdog path can be exercised quickly inside this unit test.

	if err := harness.service.dispatcher.dispatchToWorker(context.Background(), traceID, "test-project", "unused-session", string(json.RawMessage(`[{"type":"wait","params":{"ms":1}}]`))); err != nil { // Dispatch one valid-looking plan payload through the real worker-assignment and ExecutePlan RPC path.
		t.Fatalf("expected distributed dispatch to succeed, got error: %v", err) // Surface unexpected dispatch failures because the timeout path requires an accepted in-flight trace.
	}

	runningTrace, runningEvents := harness.loadTraceAndEvents(t, traceID) // Read back the trace immediately after acceptance so the test can verify the running-state promotion happened.
	if runningTrace.Status != "running" {                                 // Fail the test when accepted distributed dispatch does not move the trace out of pending.
		t.Fatalf("expected trace status running after worker acceptance, got %s", runningTrace.Status) // Surface the stale queued state because accepted distributed work should appear active to polling clients.
	}
	if len(runningEvents) != 0 { // Fail the test when simple acceptance appends terminal replay events before any timeout has occurred.
		t.Fatalf("expected no terminal events immediately after dispatch acceptance, got %d", len(runningEvents)) // Surface premature terminalization because timeout closure should happen only after the watchdog fires.
	}

	time.Sleep(80 * time.Millisecond)                                      // Wait past the shortened plan timeout so the in-flight assignment is definitely overdue before the watchdog sweep runs.
	harness.service.dispatcher.checkInflightTimeouts(context.Background()) // Run one watchdog sweep explicitly so the test can deterministically assert the overdue distributed trace is finalized now.

	trace, events := harness.loadTraceAndEvents(t, traceID) // Read back the persisted trace row and replayable events after the watchdog timeout sweep.
	if trace.Status != "failed" {                           // Fail the test when the overdue distributed trace did not persist the failed terminal state.
		t.Fatalf("expected trace status failed after distributed timeout, got %s", trace.Status) // Surface the unexpected trace state because accepted traces must not remain running forever without closure.
	}
	if len(events) != 1 { // Fail the test when timeout closure did not append exactly one final replayable terminal event.
		t.Fatalf("expected 1 terminal event after distributed timeout, got %d", len(events)) // Surface the unexpected event count because replay consumers depend on one durable terminal marker.
	}
	if events[0].Status != "failed" { // Fail the test when the terminal event did not mirror the failed timeout state.
		t.Fatalf("expected terminal event status failed after distributed timeout, got %s", events[0].Status) // Surface the unexpected event status because clients filter by event status during replay.
	}

	payload := decodeEventPayload(t, events[0].Payload) // Decode the terminal payload so the test can assert the timeout-specific metadata fields.
	if payload["errorCode"] != "E.TIMEOUT.PLAN" {       // Fail the test when the watchdog timeout does not preserve the structured plan-timeout error code.
		t.Fatalf("expected terminal payload error code E.TIMEOUT.PLAN, got %#v", payload["errorCode"]) // Surface the unexpected code because callers and dashboards rely on the standardized timeout classification.
	}
	if payload["terminalStatus"] != "failed" { // Fail the test when the timeout event payload does not record the final failed state.
		t.Fatalf("expected terminal payload status failed, got %#v", payload["terminalStatus"]) // Surface the unexpected payload field because replay consumers may inspect payload status directly.
	}

	if _, err := harness.cache.Get(context.Background(), inflightTracePrefix+traceID); err == nil { // Verify that timeout closure removed the accepted worker-assignment key from Redis.
		t.Fatalf("expected inflight trace assignment to be removed after timeout finalization") // Surface leaked watchdog state because stale assignments break later cancel and retry semantics.
	}
}
