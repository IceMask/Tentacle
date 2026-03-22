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

	liveWorker := registry.GetWorker(worker.ID) // Load the in-memory worker record so the test can manipulate its state directly before heartbeat recovery assertions.
	if liveWorker == nil {                      // Fail the test when the worker is not present in the in-memory registry after successful registration.
		t.Fatal("expected registered worker to be present in registry") // Surface the missing registry entry because the recovery-path test cannot proceed without it.
	}

	liveWorker.Status = "offline"                                                  // Force the worker into the offline state so the next heartbeat must enter the recovery window instead of becoming healthy immediately.
	liveWorker.RecoveryStarted = time.Time{}                                       // Clear any previous recovery timestamp so the test controls the entire recovery-window transition explicitly.
	if err := registry.Heartbeat(context.Background(), worker.ID, 0); err != nil { // Deliver one fresh heartbeat so the offline worker enters the degraded recovery window.
		t.Fatalf("expected offline worker heartbeat to succeed, got error: %v", err) // Surface heartbeat failures because the recovery transition cannot be asserted without one.
	}
	if liveWorker.Status != "degraded" { // Fail the test when offline workers do not re-enter through degraded first.
		t.Fatalf("expected worker status degraded after first recovery heartbeat, got %s", liveWorker.Status) // Surface the unexpected recovery state because workers must stabilize before becoming healthy again.
	}
	if liveWorker.RecoveryStarted.IsZero() { // Fail the test when the recovery-window timestamp was not recorded during the first recovery heartbeat.
		t.Fatal("expected recovery window to start on first heartbeat after offline") // Surface the missing timestamp because later stabilization checks depend on it.
	}

	liveWorker.RecoveryStarted = time.Now().Add(-recoveryWindow)                   // Pretend the worker has already stayed reachable throughout the full stabilization window.
	if err := registry.Heartbeat(context.Background(), worker.ID, 0); err != nil { // Deliver one more heartbeat so the registry can promote the recovered worker back to healthy.
		t.Fatalf("expected recovering worker heartbeat to succeed, got error: %v", err) // Surface heartbeat failures because the healthy promotion cannot be asserted without one.
	}
	if liveWorker.Status != "healthy" { // Fail the test when the worker does not become healthy again after the stabilization window has elapsed.
		t.Fatalf("expected worker status healthy after recovery window elapsed, got %s", liveWorker.Status) // Surface the unexpected recovery state because healthy scheduling depends on this promotion.
	}
	if !liveWorker.RecoveryStarted.IsZero() { // Fail the test when the recovery-window marker is not cleared after the worker becomes healthy again.
		t.Fatalf("expected recovery timestamp to be cleared after healthy promotion, got %v", liveWorker.RecoveryStarted) // Surface the lingering recovery marker because later recovery cycles must start fresh.
	}
}
