// worker_registry.go maintains race-free worker health, capacity reservations, Redis persistence, and lifecycle auditing.
package orchestrator

import (
	"context"
	"encoding/json"
	stdErrors "errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"mcp_for_appium/internal/audit"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/telemetry"

	goredis "github.com/redis/go-redis/v9"
)

const (
	workerHashKey      = "workers:registry"
	workerHeartbeatKey = "workers:heartbeat:%s"
	heartbeatTTL       = 30 * time.Second
	heartbeatInterval  = 10 * time.Second
	heartbeatMissMax   = 3
	degradeAfterMisses = 2
	removeAfterSeconds = 60
	monitorInterval    = 5 * time.Second
	recoveryWindow     = 60 * time.Second
)

// WorkerRegistry stores authoritative in-process worker snapshots and serializes every mutation that affects scheduling.
type WorkerRegistry struct {
	cache         *redis.Cache
	logger        *slog.Logger
	mu            sync.RWMutex
	workers       map[string]*WorkerNode
	reservations  map[string]string
	stopCh        chan struct{}
	stopOnce      sync.Once
	auditRecorder audit.Recorder
}

// WorkerNode describes one worker's routing metadata, capacity, load, and heartbeat-derived lifecycle state.
type WorkerNode struct {
	ID              string            `json:"id"`
	Address         string            `json:"address"`
	Tags            map[string]string `json:"tags"`
	Capacity        int               `json:"capacity"`
	ActiveLoad      int               `json:"active_load"`
	Status          string            `json:"status"`
	LastSeen        time.Time         `json:"last_seen"`
	MissedBeats     int               `json:"missed_beats"`
	RecoveryStarted time.Time         `json:"recovery_started,omitempty"`
}

// NewWorkerRegistry creates an empty worker registry backed by the supplied Redis cache.
func NewWorkerRegistry(cache *redis.Cache) *WorkerRegistry {
	return &WorkerRegistry{ // Return a fully initialized registry whose map and stop channel are safe for immediate concurrent use.
		cache:        cache,                        // Persist worker metadata and heartbeat liveness through the shared Redis cache.
		logger:       telemetry.Logger(),           // Reuse the process-wide structured logger for lifecycle and failure diagnostics.
		workers:      make(map[string]*WorkerNode), // Keep mutable worker records private behind the registry mutex.
		reservations: make(map[string]string),      // Track exact trace-attempt reservations so cleanup remains idempotent.
		stopCh:       make(chan struct{}),          // Allow the monitor loop to stop independently from its parent context.
	} // Complete registry construction with no externally shared mutable worker pointers.
}

// SetAuditRecorder installs the optional append-only sink used for worker lifecycle audit events.
func (r *WorkerRegistry) SetAuditRecorder(recorder audit.Recorder) {
	r.mu.Lock()                // Serialize recorder replacement with concurrent lifecycle-event snapshots.
	r.auditRecorder = recorder // Store the caller-provided sink without exposing mutable registry state.
	r.mu.Unlock()              // Release the registry lock after the recorder pointer is visible atomically.
}

// Register validates, persists, and publishes one healthy worker snapshot without retaining the caller's mutable pointer.
func (r *WorkerRegistry) Register(ctx context.Context, worker *WorkerNode) error {
	if worker == nil { // Reject nil registration payloads before reading worker metadata.
		return errors.New(errors.CodePlanInvalid, "worker is required") // Return a stable validation error for malformed internal RPC input.
	}
	if strings.TrimSpace(worker.ID) == "" { // Require a stable worker identifier for registry and heartbeat keys.
		return errors.New(errors.CodePlanInvalid, "worker id is required") // Prevent anonymous workers from colliding in Redis.
	}
	if strings.TrimSpace(worker.Address) == "" { // Require a routable address before making the worker schedulable.
		return errors.New(errors.CodePlanInvalid, "worker address is required") // Prevent assignments that cannot produce a worker RPC client.
	}
	if worker.Capacity <= 0 { // Reject workers that can never accept one execution slot.
		return errors.New(errors.CodePlanInvalid, "worker capacity must be greater than zero") // Preserve a clear internal validation contract.
	}

	now := time.Now().UTC()                                    // Capture one registration timestamp for both liveness and persisted metadata.
	normalized := cloneWorkerNode(worker)                      // Deep-copy tags so caller mutations cannot race with registry readers.
	normalized.ID = strings.TrimSpace(normalized.ID)           // Normalize the identifier used in all Redis and in-memory keys.
	normalized.Address = strings.TrimSpace(normalized.Address) // Normalize the routable address before it is handed to the dispatcher.
	normalized.ActiveLoad = 0                                  // Treat registration as a fresh process with no accepted local plans yet.
	normalized.Status = "healthy"                              // Make a successfully persisted worker immediately eligible for scheduling.
	normalized.LastSeen = now                                  // Seed liveness from the successful registration timestamp.
	normalized.MissedBeats = 0                                 // Clear stale heartbeat history supplied by any caller.
	normalized.RecoveryStarted = time.Time{}                   // Clear stale recovery state supplied by any caller.
	data, err := json.Marshal(normalized)                      // Encode the isolated snapshot before acquiring the mutation lock.
	if err != nil {                                            // Stop when worker metadata cannot be serialized safely.
		return errors.Wrap(errors.CodeInternal, "failed to marshal worker", err) // Preserve the serialization cause under the repository error contract.
	}

	r.mu.Lock()                                                                                                   // Serialize Redis registration and in-memory publication with heartbeat, monitor, and re-registration mutations.
	if err := r.cache.HSet(ctx, workerHashKey, map[string]interface{}{normalized.ID: string(data)}); err != nil { // Persist routing metadata before making the worker visible to local scheduling.
		r.mu.Unlock()                                                                                                                         // Release the mutation lock before returning the storage failure.
		r.recordAudit(ctx, "worker.register", normalized.ID, "failure", "store_write", map[string]interface{}{"address": normalized.Address}) // Record the safe failure classification without persisting backend error text.
		return errors.Wrap(errors.CodeStoreWrite, "failed to register worker", err)                                                           // Surface the Redis hash failure as a storage write error.
	}
	hbKey := fmt.Sprintf(workerHeartbeatKey, normalized.ID)            // Build the worker-specific liveness key from the normalized identifier.
	if err := r.cache.Set(ctx, hbKey, "1", heartbeatTTL); err != nil { // Require the heartbeat key before publishing this worker as healthy.
		_ = r.cache.HDel(ctx, workerHashKey, normalized.ID)                                                                                             // Best-effort remove the incomplete hash record so a later sync cannot resurrect it as healthy.
		r.mu.Unlock()                                                                                                                                   // Release the mutation lock before returning the heartbeat persistence failure.
		r.recordAudit(ctx, "worker.register", normalized.ID, "failure", "heartbeat_store_write", map[string]interface{}{"address": normalized.Address}) // Audit the stable failure class without recording Redis details.
		return errors.Wrap(errors.CodeStoreWrite, "failed to set heartbeat", err)                                                                       // Surface the heartbeat write failure through the stable storage contract.
	}
	r.workers[normalized.ID] = normalized                         // Publish only the registry-owned deep copy after both Redis writes succeed.
	for reservationID, reservedWorkerID := range r.reservations { // Remove abandoned reservations when this worker id registers as a fresh process.
		if reservedWorkerID == normalized.ID { // Match only capacity tokens routed to the restarted worker.
			delete(r.reservations, reservationID) // Prevent pre-restart assignments from consuming new-process capacity.
		}
	}
	r.mu.Unlock() // Allow schedulers and heartbeat handlers to observe the complete worker atomically.

	r.logger.InfoContext(ctx, "worker registered", "worker_id", normalized.ID, "address", normalized.Address, "capacity", normalized.Capacity)                  // Log safe routing and capacity metadata for operator visibility.
	r.recordAudit(ctx, "worker.register", normalized.ID, "success", "", map[string]interface{}{"address": normalized.Address, "capacity": normalized.Capacity}) // Append a successful worker-registration audit record.
	return nil                                                                                                                                                  // Confirm that Redis and in-memory registry state now contain the healthy worker.
}

// Heartbeat refreshes worker liveness and atomically reconciles reported active load plus recovery state.
func (r *WorkerRegistry) Heartbeat(ctx context.Context, workerID string, activeLoad int) error {
	workerID = strings.TrimSpace(workerID) // Normalize the identifier before any Redis or in-memory lookup.
	if workerID == "" {                    // Reject heartbeat calls that cannot identify a registry entry.
		return errors.New(errors.CodePlanInvalid, "worker id is required") // Return a stable validation error to the internal RPC boundary.
	}
	if activeLoad < 0 { // Reject impossible negative load reports before they affect scheduling ratios.
		return errors.New(errors.CodePlanInvalid, "worker active load cannot be negative") // Preserve non-negative capacity invariants inside the registry.
	}

	r.mu.Lock()                       // Serialize the full heartbeat reconciliation with scheduler reservations and monitor transitions.
	worker, ok := r.workers[workerID] // Resolve the private mutable record while holding the registry lock.
	if !ok {                          // Reload workers missing from this process before applying the heartbeat.
		r.mu.Unlock()                                                // Avoid recursively acquiring the registry lock during Redis synchronization.
		if err := r.syncWorkerFromRedis(ctx, workerID); err != nil { // Restore the registered worker snapshot from the shared Redis hash.
			return err // Preserve not-found or storage failures from synchronization.
		}
		r.mu.Lock()                  // Reacquire the mutation lock after the worker snapshot has been synchronized.
		worker = r.workers[workerID] // Resolve the synchronized registry-owned record under the lock.
		if worker == nil {           // Reject a worker removed concurrently after Redis synchronization completed.
			r.mu.Unlock()                                                                // Release the mutation lock before returning the lost-registration result.
			return errors.New(errors.CodeWorkerNotFound, "worker not found in registry") // Prevent a nil snapshot from entering heartbeat reconciliation.
		}
	}

	hbKey := fmt.Sprintf(workerHeartbeatKey, workerID)                 // Build the liveness key while the worker mutation remains serialized.
	if err := r.cache.Set(ctx, hbKey, "1", heartbeatTTL); err != nil { // Refresh the heartbeat before changing the worker's schedulable state.
		r.mu.Unlock()                                                                // Release the mutation lock before returning the storage failure.
		return errors.Wrap(errors.CodeStoreWrite, "failed to update heartbeat", err) // Keep liveness failures under the storage write contract.
	}

	now := time.Now().UTC()                        // Capture one timestamp for liveness and recovery calculations.
	next := cloneWorkerNode(worker)                // Mutate a private candidate so failed persistence leaves the prior snapshot intact.
	next.LastSeen = now                            // Record when this successful heartbeat reached the orchestrator.
	reservedLoad := r.reservedLoadLocked(workerID) // Count pre-RPC and accepted trace reservations while the mutation lock is held.
	if activeLoad < reservedLoad {                 // Preserve locally promised capacity that the worker heartbeat cannot observe yet.
		next.ActiveLoad = reservedLoad // Keep all exact reservations represented until their owning cleanup path releases them.
	} else {
		next.ActiveLoad = activeLoad // Reconcile to the authoritative worker count when it covers every known reservation.
	}
	next.MissedBeats = 0          // Clear all missed-heartbeat history after successful liveness persistence.
	action := ""                  // Track an optional lifecycle transition for logs and audit after persistence succeeds.
	if next.Status == "offline" { // Route offline workers through a stabilization period before new assignment.
		next.Status = "degraded"     // Keep the worker outside the healthy scheduling pool during recovery.
		next.RecoveryStarted = now   // Start the recovery window at this first successful heartbeat.
		action = "worker.recovering" // Classify the lifecycle transition for audit and logs.
	} else if next.Status == "degraded" && !next.RecoveryStarted.IsZero() && now.Sub(next.RecoveryStarted) >= recoveryWindow { // Promote only after the full recovery window has elapsed.
		next.Status = "healthy"            // Restore scheduling eligibility after sustained liveness.
		next.RecoveryStarted = time.Time{} // Clear the completed recovery marker for future outages.
		action = "worker.recovered"        // Classify the successful recovery transition.
	} else if next.Status == "degraded" && next.RecoveryStarted.IsZero() { // Preserve the fast recovery path for workers that never reached offline.
		next.Status = "healthy"     // Restore scheduling after a transient degraded heartbeat period.
		action = "worker.recovered" // Classify the transient recovery transition.
	}

	data, err := json.Marshal(next) // Encode the complete candidate before replacing the in-memory snapshot.
	if err != nil {                 // Stop when the worker candidate cannot be serialized.
		r.mu.Unlock()                                                                      // Release the mutation lock before returning the internal serialization failure.
		return errors.Wrap(errors.CodeInternal, "failed to marshal worker heartbeat", err) // Preserve the encoding cause under a stable internal code.
	}
	if err := r.cache.HSet(ctx, workerHashKey, map[string]interface{}{workerID: string(data)}); err != nil { // Persist the reconciled worker snapshot before local publication.
		r.mu.Unlock()                                                                        // Release the mutation lock while preserving the previous local snapshot.
		return errors.Wrap(errors.CodeStoreWrite, "failed to persist worker heartbeat", err) // Surface Redis hash failures consistently.
	}
	r.workers[workerID] = next // Publish the fully persisted heartbeat snapshot atomically to schedulers.
	r.mu.Unlock()              // Release the mutation lock before logging or audit I/O.

	if action != "" { // Emit lifecycle diagnostics only when the heartbeat changed health state.
		r.logger.InfoContext(ctx, "worker heartbeat changed lifecycle state", "worker_id", workerID, "status", next.Status) // Log the resulting state without exposing request credentials or payloads.
		r.recordAudit(ctx, action, workerID, "success", "", map[string]interface{}{"status": next.Status})                  // Append the matching worker recovery audit record.
	}
	return nil // Confirm that both heartbeat liveness and worker metadata were persisted successfully.
}

// AssignWorker selects the least-loaded matching healthy worker and atomically binds one exact reservation id to a local execution slot.
func (r *WorkerRegistry) AssignWorker(ctx context.Context, projectID string, requiredTags map[string]string, reservationID string) *WorkerNode {
	r.mu.Lock()                                            // Serialize candidate comparison and slot reservation so concurrent dispatches cannot oversubscribe local registry capacity.
	if workerID, ok := r.reservations[reservationID]; ok { // Reuse a prior routing decision when the same trace attempt is scheduled more than once.
		worker, exists := r.workers[workerID] // Resolve the worker that owns this exact reservation.
		if exists {                           // Return the prior assignment while its worker remains registered.
			reserved := cloneWorkerNode(worker) // Isolate the existing worker state before releasing the registry lock.
			r.mu.Unlock()                       // Release scheduling serialization without incrementing load a second time.
			return reserved                     // Preserve idempotent assignment for duplicate delivery of one trace attempt.
		}
		delete(r.reservations, reservationID) // Drop a token whose worker disappeared so scheduling can select a live candidate.
	}
	var selected *WorkerNode           // Track the best private worker record found in this locked scheduling snapshot.
	for _, worker := range r.workers { // Inspect every registered worker while no heartbeat or monitor mutation can race.
		if worker.Status != "healthy" || worker.Capacity <= 0 || worker.ActiveLoad >= worker.Capacity { // Skip unhealthy, malformed, or fully reserved workers.
			continue // Continue searching for an assignable candidate.
		}
		if !tagsMatch(worker.Tags, requiredTags) { // Enforce every caller-required scheduling tag.
			continue // Skip workers whose declared tags do not satisfy the request.
		}
		if selected == nil || workerLessLoaded(worker, selected) { // Prefer lower utilization and then stable worker id order.
			selected = worker // Retain the best registry-owned candidate while still under the lock.
		}
	}
	if selected == nil { // Report cleanly when this registry snapshot contains no available capacity.
		r.mu.Unlock()                                                              // Release the scheduling lock before writing logs.
		r.logger.WarnContext(ctx, "no available workers", "project_id", projectID) // Surface capacity exhaustion without treating it as an infrastructure failure.
		return nil                                                                 // Let dispatcher retry the queued message through the transient no-worker path.
	}
	selected.ActiveLoad++                       // Reserve one slot before any competing dispatcher can select the same capacity.
	r.reservations[reservationID] = selected.ID // Bind this trace attempt to the selected worker for idempotent cleanup.
	reserved := cloneWorkerNode(selected)       // Return an isolated snapshot that cannot mutate registry state outside the lock.
	r.mu.Unlock()                               // Release scheduling serialization after the slot reservation is visible.

	r.logger.InfoContext(ctx, "worker assigned", "worker_id", reserved.ID, "project_id", projectID, "load", reserved.ActiveLoad, "capacity", reserved.Capacity) // Log post-reservation utilization for capacity diagnostics.
	return reserved                                                                                                                                             // Return the immutable scheduling snapshot for RPC routing and later rollback by worker id.
}

// ReleaseWorkerReservation releases one exact trace-attempt slot after rejection, failure, completion, cancellation, or timeout.
func (r *WorkerRegistry) ReleaseWorkerReservation(reservationID string) {
	r.mu.Lock()                                         // Serialize reservation rollback with heartbeat reconciliation and new assignments.
	workerID, reserved := r.reservations[reservationID] // Resolve the exact capacity token targeted by this cleanup call.
	if reserved {                                       // Ignore duplicate cleanup after another path already consumed the same token.
		delete(r.reservations, reservationID)                               // Remove ownership before adjusting the aggregate worker load.
		if worker, ok := r.workers[workerID]; ok && worker.ActiveLoad > 0 { // Decrement only an existing positive count so rollback cannot underflow.
			worker.ActiveLoad-- // Return exactly one scheduling slot to the local availability snapshot.
		}
	}
	r.mu.Unlock() // Release the registry lock after the idempotent capacity correction.
}

// ListAvailable returns deep-copy snapshots of healthy workers that currently retain at least one unreserved slot.
func (r *WorkerRegistry) ListAvailable(_ context.Context) ([]*WorkerNode, error) {
	r.mu.RLock()                                      // Freeze the registry snapshot while worker records are copied.
	workers := make([]*WorkerNode, 0, len(r.workers)) // Preallocate enough room for the maximum possible healthy snapshot.
	for _, worker := range r.workers {                // Inspect every private worker record under the read lock.
		if worker.Status == "healthy" && worker.Capacity > worker.ActiveLoad { // Include only workers that can accept another local reservation.
			workers = append(workers, cloneWorkerNode(worker)) // Deep-copy the worker so callers cannot mutate registry state.
		}
	}
	r.mu.RUnlock()                            // Release the read lock after every returned snapshot is isolated.
	sort.Slice(workers, func(i, j int) bool { // Return deterministic order for health output and tests.
		return workers[i].ID < workers[j].ID // Sort snapshots by stable worker identifier.
	})
	return workers, nil // Return the isolated available-worker snapshot without exposing registry pointers.
}

// HealthSummary returns race-free counts for every worker lifecycle state in the current registry snapshot.
func (r *WorkerRegistry) HealthSummary() workerRegistryHealthSummary {
	r.mu.RLock()                             // Freeze worker lifecycle state for the duration of summary aggregation.
	defer r.mu.RUnlock()                     // Release the read lock after every worker has been classified.
	summary := workerRegistryHealthSummary{} // Allocate the mutable aggregate returned to the health endpoint.
	for _, worker := range r.workers {       // Classify every registry-owned worker record under the read lock.
		summary.Total++        // Count every known worker regardless of lifecycle state.
		switch worker.Status { // Bucket the worker by its current heartbeat-derived state.
		case "healthy":
			summary.Healthy++ // Count workers currently eligible for scheduling.
		case "degraded":
			summary.Degraded++ // Count workers temporarily excluded after missed heartbeats or recovery.
		case "offline":
			summary.Offline++ // Count workers known to be unreachable but not yet removed.
		default:
			summary.Unknown++ // Expose malformed lifecycle values rather than silently treating them as healthy.
		}
	}
	return summary // Return the complete race-free fleet snapshot.
}

// StartMonitor starts the heartbeat monitor loop in a background goroutine.
func (r *WorkerRegistry) StartMonitor(ctx context.Context) {
	go r.monitorLoop(ctx)                            // Run periodic liveness checks without blocking orchestrator startup.
	r.logger.Info("worker registry monitor started") // Record that heartbeat monitoring is active.
}

// Stop idempotently signals the heartbeat monitor to exit.
func (r *WorkerRegistry) Stop() {
	r.stopOnce.Do(func() { // Close the stop channel exactly once across repeated service shutdown paths.
		close(r.stopCh) // Wake the monitor select loop and prevent future heartbeat sweeps.
	})
	r.logger.Info("worker registry stopped") // Record the idempotent monitor shutdown request.
}

// monitorLoop periodically checks worker heartbeat keys until the parent context or registry stop signal fires.
func (r *WorkerRegistry) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(monitorInterval) // Create the fixed-cadence heartbeat sweep ticker.
	defer ticker.Stop()                       // Release ticker resources when monitoring exits.
	for {                                     // Continue monitoring until one explicit shutdown signal arrives.
		select {
		case <-ctx.Done():
			return // Stop when the orchestrator parent context is cancelled.
		case <-r.stopCh:
			return // Stop when Service.Stop closes the registry channel.
		case <-ticker.C:
			r.checkHeartbeats(ctx) // Evaluate all known workers against their Redis heartbeat keys.
		}
	}
}

// checkHeartbeats degrades, offlines, or removes workers with expired heartbeat keys without treating Redis outages as fleet failure.
func (r *WorkerRegistry) checkHeartbeats(ctx context.Context) {
	r.mu.RLock()                                   // Freeze map membership while collecting worker identifiers for this sweep.
	workerIDs := make([]string, 0, len(r.workers)) // Preallocate one identifier snapshot for every known worker.
	for workerID := range r.workers {              // Collect stable identifiers without exposing worker pointers.
		workerIDs = append(workerIDs, workerID) // Add this worker to the independent sweep list.
	}
	r.mu.RUnlock()          // Allow heartbeat and registration mutations while Redis keys are queried.
	sort.Strings(workerIDs) // Use deterministic sweep order for reproducible logs and tests.

	now := time.Now().UTC()              // Capture one comparison timestamp for all removal decisions in this sweep.
	for _, workerID := range workerIDs { // Check every worker that existed when the sweep began.
		hbKey := fmt.Sprintf(workerHeartbeatKey, workerID) // Build the worker-specific Redis liveness key.
		if _, err := r.cache.Get(ctx, hbKey); err == nil { // Leave workers unchanged while their heartbeat key remains present.
			continue // Advance to the next worker without incrementing missed-heartbeat state.
		} else if !stdErrors.Is(err, goredis.Nil) { // Treat Redis infrastructure failures as unknown liveness rather than mass worker death.
			r.logger.WarnContext(ctx, "failed to read worker heartbeat", "worker_id", workerID, "error", err) // Surface the cache outage for operators while preserving fleet state.
			continue                                                                                          // Retry on the next sweep instead of degrading a worker without evidence its key expired.
		}

		r.mu.Lock()                       // Serialize this missed-heartbeat transition with concurrent heartbeats and scheduler reservations.
		worker, ok := r.workers[workerID] // Re-resolve the worker because it may have been removed after the identifier snapshot.
		if !ok {                          // Skip identifiers removed concurrently before this state transition.
			r.mu.Unlock() // Release the mutation lock before advancing the sweep.
			continue      // Avoid recreating or auditing a worker that no longer exists.
		}
		next := cloneWorkerNode(worker)                                       // Mutate an isolated candidate so failed persistence does not expose a partial state.
		next.MissedBeats++                                                    // Record this confirmed heartbeat-key expiration.
		action := ""                                                          // Track at most one lifecycle transition generated by this missed beat.
		if next.MissedBeats >= heartbeatMissMax && next.Status != "offline" { // Move any sufficiently stale worker to the offline state.
			next.Status = "offline"            // Exclude the worker from all scheduling immediately.
			next.RecoveryStarted = time.Time{} // Require a new stabilization window after it returns.
			action = "worker.offline"          // Classify the lifecycle transition for logs and audit.
		} else if next.MissedBeats >= degradeAfterMisses && next.Status == "healthy" { // Degrade healthy workers before the full offline threshold.
			next.Status = "degraded"   // Remove the worker from scheduling while retaining it for recovery.
			action = "worker.degraded" // Classify the intermediate liveness transition.
		}
		remove := next.Status == "offline" && now.Sub(next.LastSeen) > removeAfterSeconds*time.Second // Remove workers that remain offline beyond the retention window.
		if remove {                                                                                   // Delete old offline workers from Redis and memory under the same mutation lock.
			if err := r.cache.HDel(ctx, workerHashKey, workerID); err != nil { // Require Redis hash removal before dropping the local record.
				r.mu.Unlock()                                                                                         // Release the mutation lock before logging the failed removal.
				r.logger.ErrorContext(ctx, "failed to remove worker from redis", "worker_id", workerID, "error", err) // Surface the storage failure so the next sweep can retry.
				continue                                                                                              // Keep the local offline record until durable registry removal succeeds.
			}
			delete(r.workers, workerID)                                   // Remove the same worker from local scheduling and health snapshots.
			for reservationID, reservedWorkerID := range r.reservations { // Remove capacity tokens owned by the deleted worker.
				if reservedWorkerID == workerID { // Match only reservations routed to this removed registry record.
					delete(r.reservations, reservationID) // Prevent deleted-worker reservations from leaking indefinitely.
				}
			}
			r.mu.Unlock()                                                                       // Release the mutation lock after durable and local removal agree.
			r.logger.InfoContext(ctx, "worker removed", "worker_id", workerID)                  // Record the completed lifecycle cleanup.
			r.recordAudit(ctx, "worker.removed", workerID, "success", "heartbeat_timeout", nil) // Append the worker removal audit event with a stable reason.
			continue                                                                            // Advance because this worker no longer has metadata to persist.
		}

		data, err := json.Marshal(next) // Encode the full degraded or offline candidate before publication.
		if err != nil {                 // Preserve the prior local snapshot when encoding unexpectedly fails.
			r.mu.Unlock()                                                                                               // Release the mutation lock before logging the internal failure.
			r.logger.ErrorContext(ctx, "failed to marshal worker heartbeat state", "worker_id", workerID, "error", err) // Surface the serialization failure for diagnosis.
			continue                                                                                                    // Retry the missed-heartbeat transition on the next monitor sweep.
		}
		if err := r.cache.HSet(ctx, workerHashKey, map[string]interface{}{workerID: string(data)}); err != nil { // Persist monitor-derived state before local publication.
			r.mu.Unlock()                                                                                               // Release the mutation lock while preserving the prior local snapshot.
			r.logger.ErrorContext(ctx, "failed to persist worker heartbeat state", "worker_id", workerID, "error", err) // Surface the Redis write failure for diagnosis.
			continue                                                                                                    // Retry the transition on the next monitor sweep.
		}
		r.workers[workerID] = next // Publish the persisted liveness state atomically to schedulers and health readers.
		r.mu.Unlock()              // Release the mutation lock before logs and audit writes.
		if action != "" {          // Emit lifecycle records only when this missed beat crossed a state threshold.
			r.logger.WarnContext(ctx, "worker heartbeat changed lifecycle state", "worker_id", workerID, "status", next.Status, "missed_beats", next.MissedBeats) // Log the safe state transition details.
			r.recordAudit(ctx, action, workerID, "success", "heartbeat_timeout", map[string]interface{}{"missedBeats": next.MissedBeats, "status": next.Status})  // Append the matching lifecycle audit event.
		}
	}
}

// syncWorkerFromRedis reloads one worker snapshot into local memory when a heartbeat reaches a fresh orchestrator process.
func (r *WorkerRegistry) syncWorkerFromRedis(ctx context.Context, workerID string) error {
	data, err := r.cache.HGetAll(ctx, workerHashKey) // Load the shared worker registry because the cache wrapper does not expose single-field lookup.
	if err != nil {                                  // Surface Redis read failures instead of treating the worker as absent.
		return errors.Wrap(errors.CodeStoreRead, "failed to load worker registry", err) // Preserve the infrastructure failure under the stable storage read code.
	}
	workerData, ok := data[workerID] // Resolve the requested worker's serialized snapshot from the shared hash.
	if !ok {                         // Reject heartbeats from workers that were never durably registered.
		return errors.New(errors.CodeWorkerNotFound, "worker not found in registry") // Keep the internal RPC response deterministic for unknown workers.
	}
	var worker WorkerNode                                               // Allocate an isolated destination for the persisted worker JSON.
	if err := json.Unmarshal([]byte(workerData), &worker); err != nil { // Decode the persisted routing, capacity, and liveness metadata.
		return errors.Wrap(errors.CodeInternal, "failed to unmarshal worker", err) // Preserve malformed shared state as an internal data error.
	}
	if worker.Capacity <= 0 { // Reject corrupted snapshots that could divide by zero during scheduling.
		return errors.New(errors.CodeInternal, "persisted worker capacity must be greater than zero") // Keep malformed registry data outside the scheduling pool.
	}
	r.mu.Lock()                                    // Serialize publication with registrations, monitor transitions, and scheduler reservations.
	r.workers[workerID] = cloneWorkerNode(&worker) // Store a registry-owned deep copy of the decoded snapshot.
	r.mu.Unlock()                                  // Release the mutation lock after the worker becomes visible atomically.
	return nil                                     // Confirm that the local registry now contains the persisted worker.
}

// GetWorker returns a deep-copy snapshot for one worker id, or nil when no worker is registered.
func (r *WorkerRegistry) GetWorker(workerID string) *WorkerNode {
	r.mu.RLock()                      // Protect map lookup and worker-field copying from concurrent mutations.
	worker, ok := r.workers[workerID] // Resolve the private worker record under the read lock.
	if !ok {                          // Return a clean absence result when no matching worker exists.
		r.mu.RUnlock() // Release the read lock before returning nil.
		return nil     // Avoid exposing a fabricated or partially initialized worker.
	}
	snapshot := cloneWorkerNode(worker) // Deep-copy tags and scalar state before releasing the lock.
	r.mu.RUnlock()                      // Allow registry mutations after the snapshot is fully isolated.
	return snapshot                     // Return a caller-owned worker snapshot that cannot race with registry state.
}

// recordAudit appends one best-effort worker lifecycle event without allowing audit sink failure to change scheduling state.
func (r *WorkerRegistry) recordAudit(ctx context.Context, action string, workerID string, result string, reason string, metadata map[string]interface{}) {
	r.mu.RLock()                // Read the optional recorder pointer consistently with SetAuditRecorder.
	recorder := r.auditRecorder // Copy the interface while holding the read lock so later replacement cannot race.
	r.mu.RUnlock()              // Release the registry lock before invoking external audit storage.
	if recorder == nil {        // Skip audit work when process wiring did not install a sink.
		return // Preserve registry operation behavior in tests and minimal deployments.
	}
	event := audit.Event{Action: action, ResourceType: "worker", ResourceID: workerID, Result: result, Reason: reason, ActorType: "system", Metadata: metadata, CreatedAt: time.Now().UTC()} // Build a sanitized lifecycle record with no worker request payload or credential data.
	if err := recorder.Record(ctx, event); err != nil {                                                                                                                                      // Keep audit persistence best-effort so an unavailable audit table cannot deadlock fleet recovery.
		r.logger.WarnContext(ctx, "failed to record worker audit event", "action", action, "worker_id", workerID, "error", err) // Surface sink failures to operators without logging secrets.
	}
}

// reservedLoadLocked counts exact trace-attempt reservations for one worker while the caller holds the registry mutex.
func (r *WorkerRegistry) reservedLoadLocked(workerID string) int {
	reservedLoad := 0                                 // Start from zero locally promised slots for the requested worker.
	for _, reservedWorkerID := range r.reservations { // Inspect every exact reservation under the caller-held mutation lock.
		if reservedWorkerID == workerID { // Count only reservations routed to the requested worker.
			reservedLoad++ // Preserve one unit of worker capacity for this trace attempt.
		}
	}
	return reservedLoad // Return the number of still-owned exact reservations.
}

// cloneWorkerNode deep-copies one worker record so mutable tag maps never cross registry lock boundaries.
func cloneWorkerNode(worker *WorkerNode) *WorkerNode {
	if worker == nil { // Preserve nil semantics for defensive internal call sites.
		return nil // Avoid dereferencing absent worker records.
	}
	clone := *worker                                       // Copy every scalar and time field into caller-owned storage.
	clone.Tags = make(map[string]string, len(worker.Tags)) // Allocate an independent tag map for race-free use.
	for key, value := range worker.Tags {                  // Copy each immutable scheduling tag pair explicitly.
		clone.Tags[key] = value // Preserve the exact tag key and value without sharing map storage.
	}
	return &clone // Return the fully isolated worker snapshot.
}

// tagsMatch reports whether one worker tag set contains every required key and exact value.
func tagsMatch(workerTags map[string]string, requiredTags map[string]string) bool {
	for key, value := range requiredTags { // Check each caller-required tag independently.
		if workerTags[key] != value { // Reject missing keys and mismatched values through the same map lookup.
			return false // Stop at the first unsatisfied scheduling constraint.
		}
	}
	return true // Confirm that every required tag is present with the requested value.
}

// workerLessLoaded orders workers by utilization ratio and then by stable id for deterministic reservations.
func workerLessLoaded(candidate *WorkerNode, current *WorkerNode) bool {
	candidateLoad := int64(candidate.ActiveLoad) * int64(current.Capacity) // Cross-multiply candidate utilization to avoid floating-point ordering drift.
	currentLoad := int64(current.ActiveLoad) * int64(candidate.Capacity)   // Cross-multiply current utilization using the same positive capacities.
	if candidateLoad != currentLoad {                                      // Prefer the worker with the strictly lower utilization ratio.
		return candidateLoad < currentLoad // Compare exact integer products without division by zero.
	}
	return candidate.ID < current.ID // Break equal-load ties deterministically by worker identifier.
}
