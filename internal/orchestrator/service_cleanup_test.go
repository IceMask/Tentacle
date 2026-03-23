// service_cleanup_test.go verifies orchestrator in-memory cache pruning and shutdown cleanup behavior.
package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/worker/appium"
)

// newCleanupTestService constructs one minimal orchestrator service suitable for direct in-memory cleanup tests.
func newCleanupTestService() *Service {
	return &Service{ // Allocate only the fields that the cleanup helpers and Stop path touch directly during these unit tests.
		cfg:            config.OrchestratorConfig{SnapshotTTL: 1500 * time.Millisecond},
		appiumMap:      make(map[string]*appium.Client),
		appiumLastUsed: make(map[string]time.Time),
		planCancel:     make(map[string]context.CancelFunc),
		snapshots:      make(map[string]*snapshotCache),
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestPruneInMemoryStateRemovesIdleAndExpiredEntries verifies that stale cached Appium clients and expired snapshots are evicted while fresh entries remain.
func TestPruneInMemoryStateRemovesIdleAndExpiredEntries(t *testing.T) {
	service := newCleanupTestService() // Construct one minimal service so the pruning helper can run without any external dependencies.
	now := time.Now()                  // Capture one stable wall-clock reference so every eviction comparison inside the test uses the same timestamp.

	service.appiumMap["stale-session"] = appium.NewClient("http://127.0.0.1:4723")                                                        // Seed one cached Appium client entry that should be evicted by the idle-client policy.
	service.appiumLastUsed["stale-session"] = now.Add(-appiumClientIdleTTL - time.Minute)                                                 // Mark the cached Appium client as older than the idle TTL so the janitor should evict it.
	service.appiumMap["fresh-session"] = appium.NewClient("http://127.0.0.1:4723")                                                        // Seed one cached Appium client entry that should survive the pruning pass.
	service.appiumLastUsed["fresh-session"] = now.Add(-time.Minute)                                                                       // Mark the cached Appium client as recently used so the janitor should keep it.
	service.snapshots["stale-session"] = &snapshotCache{rev: "old", ref: "snapshot:old", cachedAt: now.Add(-2 * service.cfg.SnapshotTTL)} // Seed one expired snapshot entry that should be evicted by the snapshot TTL policy.
	service.snapshots["fresh-session"] = &snapshotCache{rev: "new", ref: "snapshot:new", cachedAt: now.Add(-500 * time.Millisecond)}      // Seed one fresh snapshot entry that should survive the pruning pass.

	removedAppiumClients, removedSnapshots := service.pruneInMemoryState(now) // Execute the production pruning helper against the seeded in-memory state.
	if removedAppiumClients != 1 {                                            // Assert the exact number of evicted Appium clients so the idle-client policy remains precise.
		t.Fatalf("expected 1 removed appium client, got %d", removedAppiumClients) // Surface the actual eviction count so pruning regressions are easy to diagnose.
	}
	if removedSnapshots != 1 { // Assert the exact number of evicted snapshots so the snapshot TTL policy remains precise.
		t.Fatalf("expected 1 removed snapshot, got %d", removedSnapshots) // Surface the actual eviction count so pruning regressions are easy to diagnose.
	}
	if service.appiumMap["stale-session"] != nil { // Fail when the stale Appium client entry was not actually removed from the cache map.
		t.Fatal("expected stale appium client to be removed") // Surface the lingering entry because stale-client eviction is the behavior under test.
	}
	if service.appiumMap["fresh-session"] == nil { // Fail when the fresh Appium client entry was incorrectly evicted by the janitor.
		t.Fatal("expected fresh appium client to remain cached") // Surface the missing entry because fresh-client retention is the behavior under test.
	}
	if _, exists := service.snapshots["stale-session"]; exists { // Fail when the expired snapshot entry was not actually removed from the cache map.
		t.Fatal("expected stale snapshot to be removed") // Surface the lingering entry because snapshot eviction is the behavior under test.
	}
	if _, exists := service.snapshots["fresh-session"]; !exists { // Fail when the fresh snapshot entry was incorrectly evicted by the janitor.
		t.Fatal("expected fresh snapshot to remain cached") // Surface the missing entry because fresh-snapshot retention is the behavior under test.
	}
}

// TestStopCancelsTrackedPlansAndClearsCaches verifies that service shutdown cancels tracked plans and clears every in-memory cache map.
func TestStopCancelsTrackedPlansAndClearsCaches(t *testing.T) {
	service := newCleanupTestService() // Construct one minimal service so the Stop path can run without dispatcher or registry dependencies.

	cancelled := make(chan struct{}, 1)                                                                        // Allocate one buffered signal channel so the test can observe shutdown-driven cancel invocation without blocking.
	service.planCancel["trace-1"] = func() { cancelled <- struct{}{} }                                         // Seed one tracked cancel function so Stop can prove it drains the planCancel map.
	service.appiumMap["session-1"] = appium.NewClient("http://127.0.0.1:4723")                                 // Seed one cached Appium client so Stop can prove it drains the cache map.
	service.appiumLastUsed["session-1"] = time.Now()                                                           // Seed the matching last-used timestamp so Stop can prove it drains the tracking map too.
	service.snapshots["session-1"] = &snapshotCache{rev: "rev-1", ref: "snapshot:rev-1", cachedAt: time.Now()} // Seed one cached snapshot so Stop can prove it drains the snapshot map.

	service.Stop() // Execute the production Stop path so shutdown-driven plan cancellation and cache clearing run for real.

	select {
	case <-cancelled: // Accept the observed cancel signal because shutdown should invoke every tracked cancel function exactly once.
	default:
		t.Fatal("expected Stop to invoke tracked plan cancel function") // Surface the missing cancel signal because shutdown cancellation is the behavior under test.
	}
	if len(service.planCancel) != 0 { // Fail when Stop leaves tracked cancel functions behind in memory.
		t.Fatalf("expected empty planCancel map, got %d entries", len(service.planCancel)) // Surface the remaining entry count so shutdown regressions are easy to diagnose.
	}
	if len(service.appiumMap) != 0 || len(service.appiumLastUsed) != 0 { // Fail when Stop leaves cached Appium clients or timestamps behind in memory.
		t.Fatalf("expected empty appium caches, got %d clients and %d timestamps", len(service.appiumMap), len(service.appiumLastUsed)) // Surface the remaining cache sizes so shutdown regressions are easy to diagnose.
	}
	if len(service.snapshots) != 0 { // Fail when Stop leaves cached snapshots behind in memory.
		t.Fatalf("expected empty snapshot cache, got %d entries", len(service.snapshots)) // Surface the remaining cache size so shutdown regressions are easy to diagnose.
	}
}
