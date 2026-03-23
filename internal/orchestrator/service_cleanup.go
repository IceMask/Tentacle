// service_cleanup.go owns in-memory orchestrator cache cleanup and shutdown-time map draining.
package orchestrator

import (
	"context"
	"time"
)

var (
	serviceCleanupInterval = time.Minute      // serviceCleanupInterval controls how often the background janitor prunes stale in-memory orchestrator state.
	appiumClientIdleTTL    = 15 * time.Minute // appiumClientIdleTTL bounds how long one cached Appium client may remain unused before the janitor evicts it from memory.
)

// startCleanupLoop launches the background janitor that prunes stale in-memory state until the service stops.
func (s *Service) startCleanupLoop(parent context.Context) {
	if s == nil { // Guard nil services because no janitor can be started without one service instance.
		return // Stop immediately because there is no state to manage on a nil receiver.
	}
	if s.cleanupCancel != nil { // Avoid launching duplicate janitor goroutines when Start is called more than once on the same service.
		return // Stop immediately because one cleanup loop is already bound to the service lifecycle.
	}

	cleanupCtx, cleanupCancel := context.WithCancel(parent) // Derive one cleanup-specific lifecycle context so Stop can terminate the janitor deterministically.
	s.cleanupCancel = cleanupCancel                         // Store the janitor cancel function so Stop can terminate the background loop promptly.
	s.cleanupWG.Add(1)                                      // Track the janitor goroutine so Stop can wait for a clean shutdown before returning.
	go s.runCleanupLoop(cleanupCtx)                         // Launch the janitor loop in the background so stale appium and snapshot entries are pruned during long-lived service runs.
}

// stopCleanupLoop terminates the background janitor and waits for it to exit before returning.
func (s *Service) stopCleanupLoop() {
	if s == nil || s.cleanupCancel == nil { // Guard nil services and never-started janitors because there is nothing to stop in those cases.
		return // Stop immediately because no cleanup goroutine is currently owned by the service.
	}

	s.cleanupCancel()     // Signal the janitor loop to stop so it exits its ticker wait promptly.
	s.cleanupWG.Wait()    // Wait for the janitor goroutine to finish so Stop returns only after in-memory cleanup activity has ceased.
	s.cleanupCancel = nil // Clear the cancel handle so later Start calls can launch one fresh janitor loop safely.
}

// runCleanupLoop periodically prunes stale in-memory state until the supplied lifecycle context is canceled.
func (s *Service) runCleanupLoop(ctx context.Context) {
	defer s.cleanupWG.Done() // Mark the janitor goroutine as complete when the loop exits so Stop can wait deterministically.

	ticker := time.NewTicker(serviceCleanupInterval) // Drive pruning on one fixed interval so stale state does not grow unbounded during long service runs.
	defer ticker.Stop()                              // Release the ticker resources promptly once the janitor loop exits.

	for { // Continue pruning until service shutdown cancels the janitor lifecycle context.
		select {
		case <-ctx.Done(): // Stop the janitor promptly when the service lifecycle is shutting down.
			return // Exit the janitor loop because no further periodic cleanup should run after shutdown begins.
		case <-ticker.C: // Run one pruning pass on every configured janitor interval.
			s.pruneInMemoryState(time.Now()) // Prune stale appium and snapshot entries using the current wall-clock time as the eviction reference.
		}
	}
}

// pruneInMemoryState removes stale cached Appium clients and expired snapshot entries from in-memory maps.
func (s *Service) pruneInMemoryState(now time.Time) (int, int) {
	removedAppiumClients := 0 // Track how many cached Appium client entries were evicted during this pruning pass.
	removedSnapshots := 0     // Track how many expired snapshot cache entries were evicted during this pruning pass.

	s.appiumMu.Lock()                                     // Serialize access to the cached Appium client maps while this pruning pass examines and deletes stale entries.
	for sessionID, lastUsedAt := range s.appiumLastUsed { // Walk every tracked cached Appium client access timestamp so idle entries can be evicted deterministically.
		if now.Sub(lastUsedAt) <= appiumClientIdleTTL { // Keep recently used cached Appium clients because they still provide useful in-memory reuse.
			continue // Skip eviction for this entry because it has not exceeded the configured idle TTL.
		}
		delete(s.appiumMap, sessionID)      // Evict the stale cached Appium client pointer so long-lived processes do not retain unused client objects indefinitely.
		delete(s.appiumLastUsed, sessionID) // Evict the matching last-used timestamp so the tracking map stays consistent with the client map.
		removedAppiumClients++              // Count the eviction so tests and future diagnostics can observe janitor activity precisely.
	}
	s.appiumMu.Unlock() // Release the Appium cache lock before moving on to snapshot eviction.

	s.snapshotMu.Lock()                         // Serialize access to the snapshot cache map while this pruning pass examines and deletes expired entries.
	for sessionID, entry := range s.snapshots { // Walk every cached snapshot entry so expired items can be removed deterministically.
		if entry == nil { // Remove nil entries immediately because they provide no usable cache value and only consume map space.
			delete(s.snapshots, sessionID) // Evict the nil snapshot entry so the cache map cannot grow with unusable placeholders.
			removedSnapshots++             // Count the eviction so tests and future diagnostics can observe janitor activity precisely.
			continue                       // Continue because there is no timestamp to compare for this nil entry.
		}
		if s.cfg.SnapshotTTL > 0 && now.Sub(entry.cachedAt) <= s.cfg.SnapshotTTL { // Keep snapshots that are still inside the configured freshness window.
			continue // Skip eviction for this entry because it has not exceeded the configured snapshot TTL yet.
		}
		delete(s.snapshots, sessionID) // Evict the expired snapshot entry so stale cached page sources do not accumulate in memory indefinitely.
		removedSnapshots++             // Count the eviction so tests and future diagnostics can observe janitor activity precisely.
	}
	s.snapshotMu.Unlock() // Release the snapshot cache lock after the pruning pass completes.

	return removedAppiumClients, removedSnapshots // Return the eviction counts so tests can assert the exact pruning behavior without relying on logs.
}

// cancelTrackedPlans cancels every in-memory tracked plan execution context and clears the tracking map.
func (s *Service) cancelTrackedPlans() int {
	cancelledPlans := 0 // Track how many in-flight plan cancel functions were invoked during shutdown.

	s.planMu.Lock()                             // Serialize access to the tracked cancel-function map while shutdown drains it.
	for traceID, cancel := range s.planCancel { // Walk every tracked in-flight plan so shutdown can cancel them deterministically.
		if cancel != nil { // Invoke only non-nil cancel functions because nil entries would panic on invocation.
			cancel()         // Signal the in-flight execution context to stop promptly during service shutdown.
			cancelledPlans++ // Count the cancelled plan so tests and future diagnostics can observe shutdown behavior precisely.
		}
		delete(s.planCancel, traceID) // Remove the tracking entry so shutdown leaves the cancel-function map empty.
	}
	s.planMu.Unlock() // Release the plan cancel lock after the shutdown drain completes.

	return cancelledPlans // Return the number of cancelled in-flight plans so tests can assert shutdown behavior directly.
}

// clearInMemoryCaches drops every remaining cached Appium client and snapshot entry from the service maps.
func (s *Service) clearInMemoryCaches() (int, int) {
	removedAppiumClients := 0 // Track how many cached Appium client entries were dropped during shutdown.
	removedSnapshots := 0     // Track how many snapshot cache entries were dropped during shutdown.

	s.appiumMu.Lock()                       // Serialize access to the cached Appium client maps while shutdown drains them.
	removedAppiumClients = len(s.appiumMap) // Record the current number of cached Appium client entries before clearing the map.
	for sessionID := range s.appiumMap {    // Delete every cached Appium client entry so shutdown leaves no in-memory client map residue.
		delete(s.appiumMap, sessionID) // Remove the cached Appium client pointer for the current session id.
	}
	for sessionID := range s.appiumLastUsed { // Delete every last-used timestamp so the tracking map stays consistent with the client cache after shutdown.
		delete(s.appiumLastUsed, sessionID) // Remove the cached last-used timestamp for the current session id.
	}
	s.appiumMu.Unlock() // Release the Appium cache lock after the shutdown drain completes.

	s.snapshotMu.Lock()                  // Serialize access to the snapshot cache while shutdown drains it.
	removedSnapshots = len(s.snapshots)  // Record the current number of cached snapshot entries before clearing the map.
	for sessionID := range s.snapshots { // Delete every snapshot cache entry so shutdown leaves no stale page-source cache residue.
		delete(s.snapshots, sessionID) // Remove the cached snapshot entry for the current session id.
	}
	s.snapshotMu.Unlock() // Release the snapshot cache lock after the shutdown drain completes.

	return removedAppiumClients, removedSnapshots // Return the number of removed cache entries so tests can assert shutdown cleanup behavior directly.
}
