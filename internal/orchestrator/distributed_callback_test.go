// distributed_callback_test.go verifies that distributed worker callbacks append events, finalize traces, and reject stale attempts safely.
package orchestrator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"mcp_for_appium/internal/rpc"
)

// TestDistributedCallbacksFinalizeAcceptedTrace verifies that one accepted distributed worker attempt can append step events and finalize the trace successfully.
func TestDistributedCallbacksFinalizeAcceptedTrace(t *testing.T) {
	harness := newTraceTerminalHarness(t) // Start one isolated orchestrator harness so this test can exercise real DAO, Redis, and distributed callback handling.
	defer harness.service.Stop()          // Stop any background dispatcher resources the harness service may have started or allocated once the test finishes.

	_, traceID := harness.seedPendingTrace(t)                 // Seed one pending trace because distributed dispatch should start from the queued lifecycle state.
	workerAddress, stopWorker := startAcceptedWorkerServer(t) // Start one fake worker that immediately accepts the distributed ExecutePlan RPC.
	defer stopWorker()                                        // Ensure the loopback fake worker is stopped after the test completes.

	const workerID = "worker-callback"                                                                                                                                         // Use one stable fake worker id so the distributed lease and callback ownership checks remain deterministic.
	if err := harness.service.registry.Register(context.Background(), &WorkerNode{ID: workerID, Address: workerAddress, Capacity: 1, Tags: map[string]string{}}); err != nil { // Register one healthy worker so dispatcher assignment can choose it deterministically.
		t.Fatalf("failed to register fake worker: %v", err) // Surface worker-registration failures because the distributed dispatch path cannot run without an assignable worker.
	}

	harness.service.dispatcher.executor = nil                                                                                                                                                            // Force the dispatcher onto the distributed RPC path even though the shared harness defaults to monolith execution mode.
	harness.service.dispatcher.planTimeout = time.Minute                                                                                                                                                 // Keep the plan timeout comfortably above the callback test duration so no watchdog timeout interferes with this success path.
	if err := harness.service.dispatcher.dispatchToWorker(context.Background(), traceID, "test-project", "unused-session", string(json.RawMessage(`[{"type":"wait","params":{"ms":1}}]`))); err != nil { // Dispatch one valid-looking distributed plan payload through the real worker-assignment and ExecutePlan RPC path.
		t.Fatalf("expected distributed dispatch to succeed, got error: %v", err) // Surface unexpected dispatch failures because callback finalization requires an accepted in-flight trace first.
	}

	runningTrace, runningEvents := harness.loadTraceAndEvents(t, traceID) // Read back the trace immediately after worker acceptance so the test can verify the running-state promotion happened.
	if runningTrace.Status != "running" {                                 // Fail the test when accepted distributed dispatch does not move the trace out of pending.
		t.Fatalf("expected trace status running after worker acceptance, got %s", runningTrace.Status) // Surface the stale queued state because accepted distributed work should appear active to polling clients.
	}
	if runningTrace.CurrentAttempt != 1 { // Fail the test when the first accepted distributed dispatch does not reserve attempt 1 on the trace row.
		t.Fatalf("expected current attempt 1 after first distributed acceptance, got %d", runningTrace.CurrentAttempt) // Surface the unexpected attempt because stale-result protection depends on persisted attempt tracking.
	}
	if len(runningEvents) != 0 { // Fail the test when acceptance appends terminal events before any worker callback has been processed.
		t.Fatalf("expected no events immediately after dispatch acceptance, got %d", len(runningEvents)) // Surface premature event persistence because callbacks should drive replayable events here.
	}

	acceptedEvent, err := harness.service.ReportDistributedPlanEvent(context.Background(), &rpc.ReportPlanEventRequest{ // Append one worker-emitted step event through the production distributed callback path.
		TraceID:   traceID,                                                    // Target the trace accepted by the fake worker above.
		WorkerID:  workerID,                                                   // Match the registered fake worker so the Redis lease validation succeeds.
		Attempt:   runningTrace.CurrentAttempt,                                // Match the persisted current attempt so the callback still owns the running trace.
		StepIndex: 0,                                                          // Report the first step index for the synthetic wait step callback.
		Status:    "passed",                                                   // Report the synthetic step as passed so replay consumers can observe a normal step event.
		Message:   "wait completed",                                           // Provide one deterministic step message so payload assertions remain stable.
		Phase:     "step",                                                     // Provide one deterministic phase value so payload assertions remain stable.
		Metrics:   rpc.PlanEventMetrics{Attempt: 1, WDCalls: 0, ElapsedMs: 1}, // Provide one deterministic worker metric payload so replay assertions stay stable.
	})
	if err != nil { // Fail the test when the production distributed step-event callback path returns an unexpected error.
		t.Fatalf("expected distributed step event callback to succeed, got error: %v", err) // Surface the callback failure because the trace should still be owned by the fake worker attempt.
	}
	if !acceptedEvent { // Fail the test when the production distributed step-event callback path rejects the current worker attempt as stale.
		t.Fatal("expected distributed step event callback to be accepted") // Surface the stale rejection because the current worker attempt still owns the trace.
	}

	acceptedCompletion, err := harness.service.CompleteDistributedPlan(context.Background(), &rpc.CompletePlanRequest{ // Finalize the same distributed attempt through the production terminal callback path.
		TraceID:        traceID,                     // Target the same trace accepted by the fake worker above.
		WorkerID:       workerID,                    // Match the registered fake worker so the Redis lease validation succeeds.
		Attempt:        runningTrace.CurrentAttempt, // Match the persisted current attempt so the callback still owns the running trace.
		FinalStatus:    "completed",                 // Report the successful distributed terminal status.
		TerminalReason: "completed",                 // Report the successful structured terminal reason so the trace row stores it explicitly.
		Message:        "trace completed",           // Provide one deterministic terminal message so payload assertions remain stable.
	})
	if err != nil { // Fail the test when the production distributed completion callback path returns an unexpected error.
		t.Fatalf("expected distributed completion callback to succeed, got error: %v", err) // Surface the callback failure because the trace should still be owned by the fake worker attempt.
	}
	if !acceptedCompletion { // Fail the test when the production distributed completion callback path rejects the current worker attempt as stale.
		t.Fatal("expected distributed completion callback to be accepted") // Surface the stale rejection because the current worker attempt still owns the trace.
	}

	trace, events := harness.loadTraceAndEvents(t, traceID) // Read back the persisted trace row and replayable events after the distributed terminal callback.
	if trace.Status != "completed" {                        // Fail the test when the distributed terminal callback did not persist the completed trace state.
		t.Fatalf("expected trace status completed after distributed completion callback, got %s", trace.Status) // Surface the unexpected trace state because callback finalization must close the trace durably.
	}
	if trace.TerminalReason == nil || *trace.TerminalReason != "completed" { // Fail the test when the trace row did not persist the structured terminal reason.
		t.Fatalf("expected terminal reason completed, got %#v", trace.TerminalReason) // Surface the unexpected terminal reason because v4.4 requires persisted terminal-reason state.
	}
	if len(events) != 2 { // Fail the test when the accepted step event plus the final terminal event were not both persisted.
		t.Fatalf("expected 2 replayable events after distributed callbacks, got %d", len(events)) // Surface the unexpected event count because the distributed callback path should persist one step event plus one terminal event here.
	}
	if events[0].Status != "passed" || events[1].Status != "completed" { // Fail the test when the persisted event statuses do not mirror the callback inputs.
		t.Fatalf("expected event statuses passed then completed, got %s then %s", events[0].Status, events[1].Status) // Surface the unexpected replay sequence because clients rely on it for progress and terminal state.
	}

	finalPayload := decodeEventPayload(t, events[1].Payload) // Decode the persisted terminal payload so the test can inspect its structured terminal metadata.
	if finalPayload["terminalReason"] != "completed" {       // Fail the test when the terminal event payload does not mirror the stored terminal reason.
		t.Fatalf("expected terminal payload reason completed, got %#v", finalPayload["terminalReason"]) // Surface the unexpected payload field because replay consumers inspect terminal metadata here.
	}

	if _, err := harness.cache.Get(context.Background(), inflightTracePrefix+traceID); err == nil { // Verify that successful completion removed the distributed execution lease from Redis.
		t.Fatalf("expected inflight trace assignment to be removed after distributed completion") // Surface leaked lease state because later callbacks and cancels would see stale ownership metadata.
	}
}

// TestDistributedCallbacksRejectStaleAttempt verifies that late callbacks from an older attempt cannot append events or overwrite the current trace state.
func TestDistributedCallbacksRejectStaleAttempt(t *testing.T) {
	harness := newTraceTerminalHarness(t) // Start one isolated orchestrator harness so this test can exercise real DAO, Redis, and distributed callback handling.
	defer harness.service.Stop()          // Stop any background dispatcher resources the harness service may have started or allocated once the test finishes.

	_, traceID := harness.seedPendingTrace(t)                 // Seed one pending trace because distributed dispatch should start from the queued lifecycle state.
	workerAddress, stopWorker := startAcceptedWorkerServer(t) // Start one fake worker that immediately accepts the distributed ExecutePlan RPC.
	defer stopWorker()                                        // Ensure the loopback fake worker is stopped after the test completes.

	const workerID = "worker-stale"                                                                                                                                            // Use one stable fake worker id so the distributed lease and callback ownership checks remain deterministic.
	if err := harness.service.registry.Register(context.Background(), &WorkerNode{ID: workerID, Address: workerAddress, Capacity: 1, Tags: map[string]string{}}); err != nil { // Register one healthy worker so dispatcher assignment can choose it deterministically.
		t.Fatalf("failed to register fake worker: %v", err) // Surface worker-registration failures because the distributed dispatch path cannot run without an assignable worker.
	}

	harness.service.dispatcher.executor = nil                                                                                                                                                            // Force the dispatcher onto the distributed RPC path even though the shared harness defaults to monolith execution mode.
	harness.service.dispatcher.planTimeout = time.Minute                                                                                                                                                 // Keep the plan timeout comfortably above the callback test duration so no watchdog timeout interferes with this stale-callback path.
	if err := harness.service.dispatcher.dispatchToWorker(context.Background(), traceID, "test-project", "unused-session", string(json.RawMessage(`[{"type":"wait","params":{"ms":1}}]`))); err != nil { // Dispatch one valid-looking distributed plan payload through the real worker-assignment and ExecutePlan RPC path.
		t.Fatalf("expected distributed dispatch to succeed, got error: %v", err) // Surface unexpected dispatch failures because stale-callback rejection requires an accepted in-flight trace first.
	}

	runningTrace, _ := harness.loadTraceAndEvents(t, traceID) // Read back the running trace so the test can confirm the persisted current attempt before sending stale callbacks.
	if runningTrace.CurrentAttempt != 1 {                     // Fail the test when the first accepted distributed dispatch does not reserve attempt 1 on the trace row.
		t.Fatalf("expected current attempt 1 after first distributed acceptance, got %d", runningTrace.CurrentAttempt) // Surface the unexpected attempt because stale callback rejection depends on a known current attempt.
	}

	acceptedEvent, err := harness.service.ReportDistributedPlanEvent(context.Background(), &rpc.ReportPlanEventRequest{ // Send one deliberately non-current step-event callback from attempt 2.
		TraceID:   traceID,                                                    // Target the same trace accepted by the fake worker above.
		WorkerID:  workerID,                                                   // Match the registered fake worker so only the attempt mismatch makes the callback stale.
		Attempt:   2,                                                          // Deliberately report a non-current attempt so the callback is stale immediately.
		StepIndex: 0,                                                          // Report a synthetic step index even though the callback should be rejected as stale.
		Status:    "passed",                                                   // Report a synthetic step status even though the callback should be rejected as stale.
		Message:   "late step",                                                // Provide one deterministic stale message so any unexpected persistence would be obvious.
		Metrics:   rpc.PlanEventMetrics{Attempt: 1, WDCalls: 0, ElapsedMs: 1}, // Provide one deterministic worker metric payload even though the callback should be rejected as stale.
	})
	if err != nil { // Fail the test when the stale distributed step-event callback path returns an infrastructure error instead of a clean stale rejection.
		t.Fatalf("expected stale distributed step event callback to return cleanly, got error: %v", err) // Surface the unexpected infrastructure failure because stale callbacks should be benign.
	}
	if acceptedEvent { // Fail the test when the stale step-event callback is unexpectedly accepted.
		t.Fatal("expected stale distributed step event callback to be rejected") // Surface the unexpected acceptance because late callbacks must not append replay events.
	}

	acceptedCompletion, err := harness.service.CompleteDistributedPlan(context.Background(), &rpc.CompletePlanRequest{ // Send one deliberately non-current terminal callback from attempt 2.
		TraceID:        traceID,           // Target the same trace accepted by the fake worker above.
		WorkerID:       workerID,          // Match the registered fake worker so only the attempt mismatch makes the callback stale.
		Attempt:        2,                 // Deliberately report a non-current attempt so the callback is stale immediately.
		FinalStatus:    "completed",       // Report a synthetic success state even though the callback should be rejected as stale.
		TerminalReason: "completed",       // Report a synthetic terminal reason even though the callback should be rejected as stale.
		Message:        "late completion", // Provide one deterministic stale message so any unexpected persistence would be obvious.
	})
	if err != nil { // Fail the test when the stale distributed completion callback path returns an infrastructure error instead of a clean stale rejection.
		t.Fatalf("expected stale distributed completion callback to return cleanly, got error: %v", err) // Surface the unexpected infrastructure failure because stale callbacks should be benign.
	}
	if acceptedCompletion { // Fail the test when the stale terminal callback is unexpectedly accepted.
		t.Fatal("expected stale distributed completion callback to be rejected") // Surface the unexpected acceptance because late callbacks must not overwrite current trace state.
	}

	trace, events := harness.loadTraceAndEvents(t, traceID) // Read back the persisted trace row and replayable events after the stale callbacks.
	if trace.Status != "running" {                          // Fail the test when stale callbacks unexpectedly move the trace away from its current running state.
		t.Fatalf("expected trace status running after stale callbacks, got %s", trace.Status) // Surface the unexpected trace state because stale callbacks must not overwrite current ownership.
	}
	if len(events) != 0 { // Fail the test when stale callbacks unexpectedly append replayable events.
		t.Fatalf("expected 0 replayable events after stale callbacks, got %d", len(events)) // Surface unexpected event persistence because stale callbacks must be ignored.
	}
	if _, err := harness.cache.Get(context.Background(), inflightTracePrefix+traceID); err != nil { // Verify that stale callbacks do not clear the current execution lease from Redis.
		t.Fatalf("expected inflight trace assignment to remain after stale callbacks, got error: %v", err) // Surface missing lease state because stale callbacks must not release current ownership.
	}
}
