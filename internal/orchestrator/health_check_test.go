// health_check_test.go verifies that orchestrator health reporting covers execution dependencies, worker capacity, and queue state.
package orchestrator

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// TestHealthCheckReportsMonolithAppiumFailure verifies that monolith health turns down when the embedded Appium dependency is unreachable while core storage stays healthy.
func TestHealthCheckReportsMonolithAppiumFailure(t *testing.T) {
	harness := newTraceTerminalHarness(t)                                      // Start one isolated orchestrator harness so the health check exercises real PostgreSQL and Redis dependencies.
	unusedPort := reserveLoopbackPort(t)                                       // Reserve one currently free loopback port so the Appium probe can target an address with no listening server.
	harness.service.appiumURL = "http://127.0.0.1:" + strconv.Itoa(unusedPort) // Point the monolith Appium probe at an intentionally unreachable loopback endpoint.

	payload := harness.service.HealthCheck(context.Background()) // Evaluate the production health snapshot against the intentionally unreachable Appium dependency.
	if payload["status"] != "down" {                             // Fail the test when monolith health does not drop to down after the execution dependency becomes unreachable.
		t.Fatalf("expected monolith health status down, got %#v", payload["status"]) // Surface the unexpected health state because direct execution should be unavailable.
	}

	checks := payload["checks"].(map[string]string) // Read the compact dependency-check map so the test can assert individual subsystem statuses.
	if checks["database"] != "ok" {                 // Fail the test when the real PostgreSQL dependency is not reported healthy in the harness.
		t.Fatalf("expected database health ok, got %#v", checks["database"]) // Surface the unexpected storage status because the harness provisions a healthy database.
	}
	if checks["redis"] != "ok" { // Fail the test when the real Redis dependency is not reported healthy in the harness.
		t.Fatalf("expected redis health ok, got %#v", checks["redis"]) // Surface the unexpected cache status because the harness provisions a healthy Redis instance.
	}
	if checks["appium"] != "error" { // Fail the test when the unreachable Appium dependency is not reported as a failed execution dependency.
		t.Fatalf("expected appium health error, got %#v", checks["appium"]) // Surface the unexpected Appium status because monolith execution depends on it directly.
	}
	if checks["queue"] != "ok" { // Fail the test when queue inspection regresses even though Redis itself is healthy.
		t.Fatalf("expected queue health ok, got %#v", checks["queue"]) // Surface the unexpected queue status because the health endpoint should still inspect Redis streams successfully.
	}
	if checks["s3"] != "skipped" { // Fail the test when the harness without an S3 client does not report the artifact store probe as skipped.
		t.Fatalf("expected s3 health skipped, got %#v", checks["s3"]) // Surface the unexpected S3 status because the harness intentionally omits object storage wiring.
	}
	details := payload["details"].(map[string]interface{})      // Read public dependency details so the test can enforce diagnostic sanitization.
	appiumDetails := details["appium"].(map[string]interface{}) // Read the failed Appium probe's safe public metadata.
	if _, exists := appiumDetails["error"]; exists {            // Reject raw reachability diagnostics in externally visible health responses.
		t.Fatalf("expected appium health details to omit raw error text, got %#v", appiumDetails["error"]) // Surface sensitive diagnostic regression explicitly.
	}
	if _, exists := appiumDetails["url"]; exists { // Reject configured endpoint disclosure because URLs can contain internal hosts or credentials.
		t.Fatalf("expected appium health details to omit configured URL, got %#v", appiumDetails["url"]) // Surface endpoint disclosure regression explicitly.
	}
}

// TestHealthCheckReportsDistributedWorkerAndQueueSummary verifies that distributed health reports worker-capacity degradation and live queue counters.
func TestHealthCheckReportsDistributedWorkerAndQueueSummary(t *testing.T) {
	harness := newTraceTerminalHarness(t)               // Start one isolated orchestrator harness so the health check exercises real PostgreSQL and Redis dependencies.
	harness.service.execMode = ExecutionModeDistributed // Force the health endpoint onto the distributed execution branch so worker capacity and local Appium skipping are exercised.
	harness.service.dispatcher.executor = nil           // Match the distributed dispatcher shape so queue health reflects remote-worker execution mode.

	if err := harness.service.registry.Register(context.Background(), &WorkerNode{ID: "healthy-worker", Address: "127.0.0.1:19090", Capacity: 2, Tags: map[string]string{}}); err != nil { // Register one healthy worker so distributed execution capacity is available.
		t.Fatalf("failed to register healthy worker: %v", err) // Surface worker-registration failures because the distributed health snapshot depends on registry contents.
	}
	if err := harness.service.registry.Register(context.Background(), &WorkerNode{ID: "degraded-worker", Address: "127.0.0.1:19091", Capacity: 1, Tags: map[string]string{}}); err != nil { // Register one second worker that the test will mark degraded to simulate partial fleet impairment.
		t.Fatalf("failed to register degraded worker: %v", err) // Surface worker-registration failures because the degraded-fleet scenario depends on two workers being present.
	}
	updateWorkerForTest(t, harness.service.registry, "degraded-worker", func(worker *WorkerNode) { // Seed partial fleet impairment under the registry's production synchronization boundary.
		worker.Status = "degraded" // Mark the selected worker degraded so distributed health reports reduced fleet quality.
	})

	if _, err := harness.cache.XAdd(context.Background(), harness.service.dispatcher.streamName(0), map[string]interface{}{"trace_id": "queued-trace"}, streamTrimMaxLen); err != nil { // Add one retained queue message so the health endpoint has non-zero stream depth to report.
		t.Fatalf("failed to seed queue stream entry: %v", err) // Surface queue-seeding failures because the queue summary assertion depends on one retained message.
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Reserve one explicit IPv4 loopback address so the in-flight summary can point at a realistic worker endpoint.
	if err != nil {                                    // Skip the test only when even explicit loopback listeners are unavailable in the current environment.
		t.Skipf("skipping distributed health queue summary test because loopback listen failed: %v", err) // Surface the environment limitation without misclassifying the production logic as passing.
	}
	listenerAddress := listener.Addr().String()                                                                                                                                                                                                                                                               // Capture the kernel-assigned loopback address before the temporary listener is closed.
	listener.Close()                                                                                                                                                                                                                                                                                          // Release the temporary loopback listener immediately so the address can be used only as inert metadata in the in-flight assignment record.
	if _, err := harness.service.dispatcher.persistInflightAssignment(context.Background(), "inflight-trace", inflightTraceAssignment{Address: listenerAddress, WorkerID: "healthy-worker", DispatchedAtUnix: time.Now().UnixMilli(), DeadlineAtUnix: time.Now().Add(time.Minute).UnixMilli()}); err != nil { // Seed one active distributed assignment so the health endpoint can report current in-flight trace count.
		t.Fatalf("failed to persist inflight trace assignment: %v", err) // Surface Redis fixture failures because health cannot report an assignment that was never stored.
	}

	payload := harness.service.HealthCheck(context.Background()) // Evaluate the production health snapshot against the seeded distributed-capacity and queue state.
	if payload["status"] != "degraded" {                         // Fail the test when partial worker degradation does not surface as an overall degraded health state.
		t.Fatalf("expected distributed health status degraded, got %#v", payload["status"]) // Surface the unexpected health state because the fleet still has capacity but is partially impaired.
	}

	checks := payload["checks"].(map[string]string) // Read the compact dependency-check map so the test can assert worker, queue, and Appium statuses.
	if checks["workers"] != "degraded" {            // Fail the test when the health endpoint does not surface partial worker degradation explicitly.
		t.Fatalf("expected workers health degraded, got %#v", checks["workers"]) // Surface the unexpected worker status because distributed capacity is intentionally partially impaired.
	}
	if checks["queue"] != "ok" { // Fail the test when queue inspection regresses despite healthy Redis and seeded queue state.
		t.Fatalf("expected queue health ok, got %#v", checks["queue"]) // Surface the unexpected queue status because the health endpoint should still inspect queue state successfully.
	}
	if checks["appium"] != "skipped" { // Fail the test when distributed health does not skip local Appium probing.
		t.Fatalf("expected appium health skipped in distributed mode, got %#v", checks["appium"]) // Surface the unexpected Appium status because local Appium is not a distributed execution dependency.
	}

	details := payload["details"].(map[string]interface{})       // Read the structured details map so the test can assert raw worker and queue counters.
	workerDetails := details["workers"].(map[string]interface{}) // Read the worker detail map returned by the health endpoint.
	if workerDetails["healthy"] != 1 {                           // Fail the test when the health endpoint does not report the single healthy worker correctly.
		t.Fatalf("expected healthy worker count 1, got %#v", workerDetails["healthy"]) // Surface the unexpected healthy count because execution capacity depends on it.
	}
	if workerDetails["degraded"] != 1 { // Fail the test when the health endpoint does not report the single degraded worker correctly.
		t.Fatalf("expected degraded worker count 1, got %#v", workerDetails["degraded"]) // Surface the unexpected degraded count because the test seeded one degraded worker explicitly.
	}
	queueDetails := details["queue"].(map[string]interface{}) // Read the queue detail map returned by the health endpoint.
	if queueDetails["retainedMessages"] != int64(1) {         // Fail the test when the seeded retained queue message is not reflected in the health snapshot.
		t.Fatalf("expected retained queue messages 1, got %#v", queueDetails["retainedMessages"]) // Surface the unexpected retained count because the health endpoint should expose live queue depth.
	}
	if queueDetails["inflightTraces"] != 1 { // Fail the test when the seeded active distributed assignment is not reflected in the health snapshot.
		t.Fatalf("expected inflight traces 1, got %#v", queueDetails["inflightTraces"]) // Surface the unexpected in-flight count because the health endpoint should expose active distributed work.
	}
}
