// worker_registry_recovery_test.go verifies that offline workers re-enter through a degraded recovery window before returning to healthy.
package orchestrator

import (
	"context"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	redisstore "mcp_for_appium/internal/storage/redis"

	"github.com/alicebob/miniredis/v2"
)

// TestWorkerHeartbeatRecoveryWindow verifies that offline workers return through degraded first and only become healthy again after the configured stabilization window.
func TestWorkerHeartbeatRecoveryWindow(t *testing.T) {
	redisServer := miniredis.RunT(t)                                                // Start one in-memory Redis server so the registry can exercise real heartbeat key writes during the recovery-path test.
	cache, err := redisstore.NewCache(config.RedisConfig{Addr: redisServer.Addr()}) // Open the production Redis cache wrapper against the isolated in-memory Redis server.
	if err != nil {                                                                 // Stop immediately when the Redis cache wrapper cannot connect to the isolated in-memory Redis server.
		t.Fatalf("failed to create redis cache: %v", err) // Surface the Redis setup failure because worker-registry recovery depends on cache operations.
	}
	defer func() {
		_ = cache.Close()   // Close the production cache wrapper before the in-memory Redis server is torn down.
		redisServer.Close() // Stop the in-memory Redis server after the cache wrapper has released its connection.
	}()

	registry := NewWorkerRegistry(cache)                                                                            // Construct the production worker registry over the isolated Redis cache so heartbeat persistence uses real commands.
	worker := &WorkerNode{ID: "worker-recovery", Address: "127.0.0.1:9092", Capacity: 1, Tags: map[string]string{}} // Construct one minimal worker node record so the registry can exercise its recovery transitions.
	if err := registry.Register(context.Background(), worker); err != nil {                                         // Register the worker so the registry owns one persisted worker record before recovery transitions are tested.
		t.Fatalf("failed to register worker: %v", err) // Surface registration failures because the recovery-path test cannot proceed without a persisted worker.
	}

	liveWorker := registry.GetWorker(worker.ID) // Load an isolated worker snapshot so the test first verifies successful registration visibility.
	if liveWorker == nil {                      // Fail the test when the worker is not present in the in-memory registry after successful registration.
		t.Fatal("expected registered worker to be present in registry") // Surface the missing registry entry because the recovery-path test cannot proceed without it.
	}

	updateWorkerForTest(t, registry, worker.ID, func(current *WorkerNode) { // Mutate registry-owned lifecycle state under its production mutex to seed the offline recovery scenario safely.
		current.Status = "offline"            // Force the worker offline so the next heartbeat must enter the recovery window first.
		current.RecoveryStarted = time.Time{} // Clear prior recovery state so this test controls the stabilization window.
	})
	if err := registry.Heartbeat(context.Background(), worker.ID, 0); err != nil { // Deliver one fresh heartbeat so the offline worker enters the degraded recovery window.
		t.Fatalf("expected offline worker heartbeat to succeed, got error: %v", err) // Surface heartbeat failures because the recovery transition cannot be asserted without one.
	}
	liveWorker = registry.GetWorker(worker.ID) // Read a fresh isolated snapshot because registry queries no longer expose mutable internal pointers.
	if liveWorker.Status != "degraded" {       // Fail the test when offline workers do not re-enter through degraded first.
		t.Fatalf("expected worker status degraded after first recovery heartbeat, got %s", liveWorker.Status) // Surface the unexpected recovery state because workers must stabilize before becoming healthy again.
	}
	if liveWorker.RecoveryStarted.IsZero() { // Fail the test when the recovery-window timestamp was not recorded during the first recovery heartbeat.
		t.Fatal("expected recovery window to start on first heartbeat after offline") // Surface the missing timestamp because later stabilization checks depend on it.
	}

	updateWorkerForTest(t, registry, worker.ID, func(current *WorkerNode) { // Advance the registry-owned recovery marker under the same mutex used by production heartbeats.
		current.RecoveryStarted = time.Now().Add(-recoveryWindow) // Pretend the worker has remained reachable for the entire stabilization interval.
	})
	if err := registry.Heartbeat(context.Background(), worker.ID, 0); err != nil { // Deliver one more heartbeat so the registry can promote the recovered worker back to healthy.
		t.Fatalf("expected recovering worker heartbeat to succeed, got error: %v", err) // Surface heartbeat failures because the healthy promotion cannot be asserted without one.
	}
	liveWorker = registry.GetWorker(worker.ID) // Read the post-heartbeat snapshot so assertions observe the newly published healthy state.
	if liveWorker.Status != "healthy" {        // Fail the test when the worker does not become healthy again after the stabilization window has elapsed.
		t.Fatalf("expected worker status healthy after recovery window elapsed, got %s", liveWorker.Status) // Surface the unexpected recovery state because healthy scheduling depends on this promotion.
	}
	if !liveWorker.RecoveryStarted.IsZero() { // Fail the test when the recovery-window marker is not cleared after the worker becomes healthy again.
		t.Fatalf("expected recovery timestamp to be cleared after healthy promotion, got %v", liveWorker.RecoveryStarted) // Surface the lingering recovery marker because later recovery cycles must start fresh.
	}
}

// updateWorkerForTest mutates one registry-owned worker under the production mutex so lifecycle tests never depend on escaped internal pointers.
func updateWorkerForTest(t *testing.T, registry *WorkerRegistry, workerID string, update func(*WorkerNode)) {
	t.Helper()                               // Attribute failures to the scenario call site instead of this synchronization helper.
	registry.mu.Lock()                       // Serialize test-only state injection with the same lock used by production registry mutations.
	defer registry.mu.Unlock()               // Always release the registry lock even when a test assertion aborts inside this helper.
	worker, ok := registry.workers[workerID] // Resolve the private registry record while mutation is exclusively locked.
	if !ok {                                 // Fail immediately when the requested worker was not registered successfully.
		t.Fatalf("worker %s not found in registry", workerID) // Surface the missing fixture because the requested state mutation cannot be applied.
	}
	update(worker) // Apply the narrowly scoped test mutation without exposing the private worker pointer outside the lock.
}
