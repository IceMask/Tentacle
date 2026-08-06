package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"mcp_for_appium/internal/audit"
	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/telemetry"

	"github.com/google/uuid"
)

const (
	numShards             = 8
	consumerGroup         = "orchestrator-group"
	consumerName          = "orchestrator-consumer"
	batchSize             = 10
	blockTime             = 5 * time.Second
	claimMinIdleTime      = 30 * time.Second
	maxRetries            = 3
	retryBackoffBase      = 200 * time.Millisecond
	retryBackoffJitter    = 1000 * time.Millisecond
	streamTrimMaxLen      = 10000
	streamTrimApprox      = true
	cancelledTracePrefix  = "cancelled:trace:"
	cancelledTraceTTL     = time.Hour
	retryKeyPrefix        = "retries:trace:"
	retryKeyTTL           = 10 * time.Minute
	inflightTracePrefix   = "inflight:trace:"
	inflightTraceTTL      = 2 * time.Hour
	dispatchLockPrefix    = "dispatch:trace:"
	dispatchLockTTL       = 45 * time.Second
	dispatchUnlockTimeout = 2 * time.Second
	inflightMonitorTick   = 5 * time.Second
	executionLeaseGrace   = 15 * time.Second
	workerRPCTimeout      = 10 * time.Second
)

type Dispatcher struct {
	cache             *redis.Cache
	registry          *WorkerRegistry
	executor          PlanExecutor
	finalizer         TraceFinalizer
	logger            *slog.Logger
	stopCh            chan struct{}
	workerClients     map[string]*rpc.WorkerClient
	workerClientMu    sync.Mutex
	planTimeout       time.Duration
	heartbeatInterval time.Duration
	rpcSecurity       config.RPCSecurityConfig
	auditRecorder     audit.Recorder
}

// SetAuditRecorder installs the optional append-only sink used for distributed lease and timeout audit events.
func (d *Dispatcher) SetAuditRecorder(recorder audit.Recorder) {
	d.auditRecorder = recorder // Store the process-wired recorder before dispatch loops begin serving queue messages.
}

// TraceFinalizer records dispatcher-owned terminal outcomes for queued traces before the queue message is ACKed.
type TraceFinalizer interface {
	FinalizeQueuedTrace(ctx context.Context, traceID string, nextStatus string, terminalReason string, message string, cause error, retryCount int64) error
}

// distributedAttemptOwner records and promotes distributed trace attempts around the worker-acceptance boundary.
type distributedAttemptOwner interface {
	ReserveDispatchedTraceAttempt(ctx context.Context, traceID string) (int64, error)
	MarkDispatchedTraceRunning(ctx context.Context, traceID string, attempt int64) (bool, error)
}

// inflightTraceAssignment stores the worker and deadline metadata needed to cancel or time out one distributed trace after the queue message has been ACKed.
type inflightTraceAssignment struct {
	Address            string `json:"address"`
	WorkerID           string `json:"workerId,omitempty"`
	Attempt            int64  `json:"attempt,omitempty"`
	DispatchedAtUnix   int64  `json:"dispatchedAtUnix"`
	LastRenewedAtUnix  int64  `json:"lastRenewedAtUnix,omitempty"`
	LeaseExpiresAtUnix int64  `json:"leaseExpiresAtUnix,omitempty"`
	DeadlineAtUnix     int64  `json:"deadlineAtUnix,omitempty"`
}

// NewDispatcher executes this operation.
func NewDispatcher(cache *redis.Cache, registry *WorkerRegistry, executor PlanExecutor, planTimeout time.Duration, workerHeartbeatInterval time.Duration, rpcSecurity config.RPCSecurityConfig) *Dispatcher {
	if workerHeartbeatInterval <= 0 { // Fall back to the registry heartbeat constant when the caller omits an explicit worker heartbeat interval.
		workerHeartbeatInterval = heartbeatInterval // Keep lease derivation aligned with the registry default cadence used elsewhere in the distributed runtime.
	}
	return &Dispatcher{
		cache:             cache,
		registry:          registry,
		executor:          executor,
		logger:            telemetry.Logger(),
		stopCh:            make(chan struct{}),
		workerClients:     make(map[string]*rpc.WorkerClient),
		planTimeout:       planTimeout,
		heartbeatInterval: workerHeartbeatInterval,
		rpcSecurity:       rpcSecurity,
	}
}

// shardFor returns shard index for a project using consistent hashing
func (d *Dispatcher) shardFor(projectID string) int {
	h := fnv.New32a()
	h.Write([]byte(projectID))
	return int(h.Sum32() % numShards)
}

// streamName returns Redis stream name for a shard
func (d *Dispatcher) streamName(shard int) string {
	return fmt.Sprintf("queue:shard:%d", shard)
}

// EnqueuePlan adds a plan to the appropriate shard queue
func (d *Dispatcher) EnqueuePlan(ctx context.Context, projectID string, sessionID string, traceID string, plan json.RawMessage) error {
	shard := d.shardFor(projectID)
	stream := d.streamName(shard)

	values := map[string]interface{}{
		"trace_id":    traceID,
		"project_id":  projectID,
		"session_id":  sessionID,
		"plan":        string(plan),
		"enqueued_at": time.Now().Unix(),
		"retry_count": 0,
	}

	_, err := d.cache.XAdd(ctx, stream, values, streamTrimMaxLen)
	if err != nil {
		d.logger.ErrorContext(ctx, "failed to enqueue plan",
			"trace_id", traceID,
			"project_id", projectID,
			"shard", shard,
			"error", err)
		return errors.Wrap(errors.CodeInternal, "failed to enqueue plan", err)
	}

	d.logger.InfoContext(ctx, "plan enqueued",
		"trace_id", traceID,
		"project_id", projectID,
		"shard", shard)

	return nil
}

// StartDispatchLoop starts consuming from all shards and dispatching to workers
func (d *Dispatcher) StartDispatchLoop(ctx context.Context) {
	// Ensure consumer groups exist for all shards
	for i := 0; i < numShards; i++ {
		stream := d.streamName(i)
		if err := d.cache.XGroupCreateMkStream(ctx, stream, consumerGroup); err != nil {
			d.logger.WarnContext(ctx, "consumer group may already exist", "stream", stream, "error", err)
		}
	}

	// Start dispatcher goroutines for each shard
	for i := 0; i < numShards; i++ {
		shard := i
		go d.dispatchShard(ctx, shard)
	}
	if d.executor == nil && d.planTimeout > 0 { // Start the distributed in-flight timeout monitor only when this dispatcher is delegating work to external workers and timeouts are configured.
		go d.monitorInflight(ctx) // Run one background sweep loop so accepted distributed traces cannot remain active forever without a terminal outcome.
	}

	d.logger.Info("dispatch loop started", "num_shards", numShards)
}

// Stop gracefully stops the dispatcher
func (d *Dispatcher) Stop() {
	close(d.stopCh)
	d.logger.Info("dispatcher stopped")
}

// dispatchShard consumes from a single shard and dispatches to workers
func (d *Dispatcher) dispatchShard(ctx context.Context, shard int) {
	stream := d.streamName(shard)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			// Claim abandoned messages
			d.claimAbandoned(ctx, stream)
		default:
			// Read new messages
			d.readAndDispatch(ctx, stream)
		}
	}
}

// readAndDispatch reads messages from stream and dispatches to workers
func (d *Dispatcher) readAndDispatch(ctx context.Context, stream string) {
	messages, err := d.cache.XReadGroup(ctx, consumerGroup, consumerName, stream, ">", batchSize, blockTime)
	if err != nil {
		if err != context.DeadlineExceeded && err.Error() != "redis: nil" {
			d.logger.ErrorContext(ctx, "failed to read from stream", "stream", stream, "error", err)
		}
		return
	}

	for _, msg := range messages {
		d.dispatchMessage(ctx, stream, msg)
	}
}

// claimAbandoned claims messages that have been idle too long (worker crash)
func (d *Dispatcher) claimAbandoned(ctx context.Context, stream string) {
	messages, err := d.cache.XAutoClaim(ctx, stream, consumerGroup, consumerName, claimMinIdleTime, "0-0", batchSize)
	if err != nil {
		d.logger.ErrorContext(ctx, "failed to autoclaim", "stream", stream, "error", err)
		return
	}

	for _, msg := range messages {
		d.logger.WarnContext(ctx, "claiming abandoned message", "stream", stream, "message_id", msg.ID)
		d.dispatchMessage(ctx, stream, msg)
	}
}

// dispatchMessage dispatches a single message to a worker
func (d *Dispatcher) dispatchMessage(ctx context.Context, stream string, msg redis.StreamMessage) {
	traceID := stringValue(msg.Values["trace_id"])
	projectID := stringValue(msg.Values["project_id"])
	sessionID := stringValue(msg.Values["session_id"])
	planStr := stringValue(msg.Values["plan"])
	retryCount := intValue(msg.Values["retry_count"])
	enqueuedAt := intValue(msg.Values["enqueued_at"])

	d.logger.InfoContext(ctx, "dispatching plan",
		"trace_id", traceID,
		"project_id", projectID,
		"retry_count", retryCount)

	if enqueuedAt > 0 {
		wait := time.Since(time.Unix(enqueuedAt, 0)).Seconds()
		telemetry.QueueWaitSeconds.WithLabelValues("", projectID).Observe(wait)
	}

	// Check if this trace was cancelled while sitting in the queue
	if cancelled, _ := d.cache.Get(ctx, cancelledTracePrefix+traceID); cancelled == "1" {
		d.logger.InfoContext(ctx, "skipping cancelled trace",
			"trace_id", traceID)
		if err := d.finalizeQueuedTrace(ctx, traceID, "cancelled", "cancelled", "trace cancelled before execution", nil, 0); err != nil { // Persist the cancelled terminal outcome before ACK so queued cancels cannot disappear without closing the trace.
			return // Leave the message pending so auto-claim can retry terminalization after transient storage failures.
		}
		d.ackMessage(ctx, stream, msg.ID) // ACK only after the queued-cancel terminal outcome has been durably recorded or confirmed already terminal.
		d.cleanupRetryState(ctx, traceID) // Drop any stale retry counter once the queued trace has reached a durable terminal state.
		return
	}

	var dispatchErr error

	if d.executor != nil {
		telemetry.ConcurrencyInUse.WithLabelValues("", projectID).Inc()
		dispatchErr = d.executor.ExecuteDispatchedPlan(ctx, traceID, sessionID, json.RawMessage(planStr))
		telemetry.ConcurrencyInUse.WithLabelValues("", projectID).Dec()
	} else {
		dispatchErr = d.dispatchToWorker(ctx, traceID, projectID, sessionID, planStr)
	}

	if dispatchErr != nil {
		d.handleDispatchError(ctx, stream, msg.ID, traceID, dispatchErr)
		return
	}

	d.ackMessage(ctx, stream, msg.ID) // ACK only after the dispatch attempt has completed successfully so the message does not reappear unnecessarily.
	d.cleanupRetryState(ctx, traceID) // Drop any stale retry counter once the dispatch has succeeded.
}

// dispatchToWorker assigns a worker and sends the plan via gRPC.
func (d *Dispatcher) dispatchToWorker(ctx context.Context, traceID, projectID, sessionID, planStr string) error {
	owner, ok := d.finalizer.(distributedAttemptOwner) // Detect whether the wired finalizer can reserve and promote distributed attempts around worker acceptance.
	if !ok {                                           // Reject distributed dispatch when no attempt owner is wired because stale-result protection would be impossible.
		return errors.New(errors.CodeInternal, "distributed attempt owner is not configured") // Surface the missing attempt owner as an internal wiring failure.
	}
	dispatchLockKey := dispatchLockPrefix + traceID                                        // Namespace the per-trace lock separately from the longer-lived execution lease.
	dispatchLockToken := uuid.NewString()                                                  // Generate one unguessable ownership value so an expired lock holder cannot delete a successor's lock.
	locked, err := d.cache.SetNX(ctx, dispatchLockKey, dispatchLockToken, dispatchLockTTL) // Acquire distributed serialization before incrementing the trace attempt or selecting a worker.
	if err != nil {                                                                        // Fail closed when Redis cannot establish unique dispatch ownership.
		return errors.Wrap(errors.CodeStoreWrite, "failed to acquire distributed dispatch lock", err) // Keep the queue message retryable without sending any worker RPC.
	}
	if !locked { // Treat a concurrent dispatcher for the same trace as transient scheduling pressure.
		return errors.New(errors.CodeSchedNoWorker, "trace dispatch is already in progress") // Preserve the duplicate queue delivery for a later benign retry.
	}
	defer func() { // Release only this exact lock token after every dispatch outcome.
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), dispatchUnlockTimeout)    // Detach lock cleanup from a cancelled request while keeping Redis I/O bounded.
		defer releaseCancel()                                                                            // Release the detached cleanup timer after the ownership-guarded delete returns.
		released, releaseErr := d.cache.CompareAndDelete(releaseCtx, dispatchLockKey, dispatchLockToken) // Prevent an expired owner from deleting a newer dispatch lock.
		if releaseErr != nil {                                                                           // Log infrastructure failure while allowing the short lock TTL to recover automatically.
			d.logger.WarnContext(ctx, "failed to release distributed dispatch lock", "trace_id", traceID, "error", releaseErr) // Surface cleanup failure without changing the completed dispatch outcome.
		} else if !released { // Record when the lock already expired or changed ownership before cleanup.
			d.logger.InfoContext(ctx, "distributed dispatch lock no longer owned at release", "trace_id", traceID) // Keep ownership drift observable for timing diagnosis.
		}
	}()
	attempt, err := owner.ReserveDispatchedTraceAttempt(ctx, traceID) // Reserve the next distributed attempt before the worker RPC so the worker can receive a stable ownership token.
	if err != nil {                                                   // Surface attempt-reservation failures before any worker RPC is sent.
		return err // Preserve the wrapped storage error so retry logic can decide whether to requeue or fail.
	}
	if attempt == 0 { // Treat a lost reservation race as a benign no-op because another lifecycle transition already moved the trace away from pending.
		d.logger.InfoContext(ctx, "skipping distributed dispatch because trace is no longer pending", "trace_id", traceID, "project_id", projectID) // Record the benign race so operators can correlate skipped dispatches with concurrent cancels or terminal transitions.
		return nil                                                                                                                                  // Report success so the queue message can be ACKed after another lifecycle transition already won.
	}
	reservationID := distributedReservationID(traceID, attempt)                      // Derive one exact capacity token from the durably reserved trace attempt.
	w := d.registry.AssignWorker(ctx, projectID, map[string]string{}, reservationID) // Atomically select a worker and reserve one local slot for this exact attempt.
	if w == nil {                                                                    // Return a transient scheduling result while preserving the monotonic attempt number for a later retry.
		d.logger.WarnContext(ctx, "no available worker", "trace_id", traceID, "project_id", projectID, "attempt", attempt) // Surface fleet capacity exhaustion for operator diagnosis.
		return errors.New(errors.CodeSchedNoWorker, "no available worker")                                                 // Leave the queue message pending so auto-claim can retry after capacity returns.
	}
	releaseReservation := true // Roll back this exact local slot on every path that does not produce a durable running attempt.
	defer func() {             // Centralize capacity rollback so new error returns cannot accidentally leak a worker slot.
		if releaseReservation { // Keep the reservation only after durable running-state promotion succeeds.
			d.registry.ReleaseWorkerReservation(reservationID) // Release this exact trace attempt idempotently after dispatch failure or a fast terminal callback.
		}
	}()

	client, err := d.getWorkerClient(w.Address) // Reuse or dial the selected worker's internal gRPC client after capacity is reserved.
	if err != nil {                             // Treat worker dial failures as transient fleet availability problems.
		return errors.Wrap(errors.CodeSchedNoWorker, "failed to connect to worker", err) // Preserve the dial cause for server logs while allowing queue retry.
	}

	workerSessionID, err := d.resolveWorkerSessionID(ctx, sessionID) // Resolve the worker-facing Appium session identifier before dispatch so the worker can attach to the real Appium session instead of the platform session id.
	if err != nil {                                                  // Surface session-resolution failures before any worker RPC is sent because distributed execution cannot proceed safely without the Appium session id.
		return err // Preserve the wrapped cache lookup failure so upstream retry and terminalization logic can react consistently.
	}
	dispatchedAt := time.Now().UTC()       // Capture one dispatch timestamp before lease persistence and the worker RPC.
	assignment := inflightTraceAssignment{ // Build the provisional ownership lease before any remote execution can begin.
		Address:            w.Address,                                 // Record the worker address so later cancel RPCs can reach the same worker instance.
		WorkerID:           w.ID,                                      // Record the worker identifier so callback renewals and timeout logs can pinpoint the same assignment.
		Attempt:            attempt,                                   // Record the reserved execution attempt so callback ownership checks can reject stale results deterministically.
		DispatchedAtUnix:   dispatchedAt.UnixMilli(),                  // Capture the dispatch timestamp so operators can diagnose how long the trace has been in flight.
		LastRenewedAtUnix:  dispatchedAt.UnixMilli(),                  // Seed the lease-renewal timestamp immediately so the first worker callback can refresh the same lease instead of racing a missing key.
		LeaseExpiresAtUnix: d.inflightLeaseDeadlineUnix(dispatchedAt), // Capture the ownership-lease expiry derived from the worker heartbeat cadence and grace window.
		DeadlineAtUnix:     d.inflightDeadlineUnix(dispatchedAt),      // Capture the absolute timeout deadline derived from the configured plan timeout for later watchdog sweeps.
	} // Complete the ownership metadata that every fast callback must validate against.
	rawAssignment, err := d.persistInflightAssignment(ctx, traceID, assignment) // Require durable Redis ownership before allowing the worker to accept execution.
	if err != nil {                                                             // Stop before the worker RPC whenever ownership creation fails or conflicts.
		d.recordAudit(ctx, "execution.lease_create", traceID, "failure", "store_write", assignment) // Audit the fail-closed lease outcome without recording backend error text.
		return err                                                                                  // Leave the queued trace pending for retry because no worker RPC was sent.
	}

	rpcCtx, rpcCancel := context.WithTimeout(ctx, workerRPCTimeout)  // Bound worker acceptance so a stalled peer cannot pin queue dispatch indefinitely.
	resp, err := client.ExecutePlan(rpcCtx, &rpc.ExecutePlanRequest{ // Ask the worker to admit this exact leased execution attempt.
		TraceID:   traceID,                  // Identify the persisted trace whose callback state the worker will own.
		SessionID: workerSessionID,          // Attach execution to the resolved live Appium session.
		Attempt:   attempt,                  // Carry the monotonic ownership token used by every callback guard.
		Plan:      json.RawMessage(planStr), // Forward the already accepted plan JSON without reparsing it in the dispatcher.
	})
	rpcCancel()     // Release the per-acceptance timer immediately after the worker RPC returns.
	if err != nil { // Roll back the provisional lease when the worker never acknowledged admission.
		d.cleanupInflightTraceIfOwned(ctx, traceID, rawAssignment)                         // Remove only the unchanged provisional lease because a fast renewal can prove remote acceptance.
		return errors.Wrap(errors.CodeSchedNoWorker, "worker ExecutePlan RPC failed", err) // Keep transport and timeout failures retryable through the scheduler path.
	}
	if resp == nil { // Reject malformed gRPC success responses that omit the admission result entirely.
		d.cleanupInflightTraceIfOwned(ctx, traceID, rawAssignment)                                   // Remove only the unchanged provisional lease because no valid acceptance decision exists.
		return errors.New(errors.CodeSchedNoWorker, "worker returned an empty ExecutePlan response") // Retry on another dispatch instead of assuming remote execution began.
	}
	if resp.Status == "busy" { // Treat authoritative worker-side capacity rejection as transient scheduling pressure.
		d.cleanupInflightTraceIfOwned(ctx, traceID, rawAssignment)           // Remove the exact unused ownership lease before the queue message is retried.
		return errors.New(errors.CodeSchedNoWorker, "worker is at capacity") // Preserve the queue message for later scheduling without marking the plan invalid.
	}
	if resp.Status != "accepted" { // Treat all explicit non-capacity rejections as terminal request validation failures.
		d.cleanupInflightTraceIfOwned(ctx, traceID, rawAssignment)        // Remove the exact provisional lease because the worker rejected the reserved attempt.
		return errors.New(errors.CodePlanInvalid, "worker rejected plan") // Avoid propagating arbitrary worker response text into external terminal events.
	}

	attemptRecorded, err := owner.MarkDispatchedTraceRunning(ctx, traceID, attempt) // Promote the reserved trace attempt to running only after the worker has acknowledged acceptance.
	if err != nil {                                                                 // Best-effort cancel the accepted worker trace when durable running-state promotion fails after acceptance.
		d.cancelAcceptedWorker(ctx, client, traceID, w.ID, attempt, "running-state persistence error") // Stop remote work before this failed acceptance bookkeeping can be retried as a duplicate execution.
		d.cleanupInflightTraceIfOwned(ctx, traceID, rawAssignment)                                     // Remove the lease only if no fast callback renewed or released it during promotion failure.
		return err                                                                                     // Preserve the wrapped storage error so upstream handling can surface the failed acceptance bookkeeping.
	}
	if !attemptRecorded { // Cancel stale worker acceptance when another lifecycle transition already moved the reserved trace out of pending before running-state promotion committed.
		d.cancelAcceptedWorker(ctx, client, traceID, w.ID, attempt, "stale distributed acceptance") // Stop any run that did not already finish through a fast pending-state callback.
		d.cleanupInflightTraceIfOwned(ctx, traceID, rawAssignment)                                  // Remove only the unchanged lease because a fast callback may already have released it.
		return nil                                                                                  // Treat the stale acceptance as a successful no-op because the trace has already moved to another lifecycle state elsewhere.
	}
	releaseReservation = false // Keep capacity charged to this exact attempt until completion, cancellation, timeout, or orphan cleanup releases it.

	d.logger.InfoContext(ctx, "dispatched to worker", "trace_id", traceID, "worker_id", w.ID, "attempt", attempt) // Record the accepted and durably promoted distributed assignment.
	d.recordAudit(ctx, "execution.dispatched", traceID, "success", "", assignment)                                // Append safe routing, attempt, and deadline metadata for execution audit.
	return nil                                                                                                    // Allow the queue message to be acknowledged after durable running-state promotion.
}

// cancelAcceptedWorker best-effort cancels one remotely accepted plan through a bounded RPC during dispatcher rollback.
func (d *Dispatcher) cancelAcceptedWorker(ctx context.Context, client *rpc.WorkerClient, traceID string, workerID string, attempt int64, reason string) {
	cancelCtx, cancel := context.WithTimeout(ctx, workerRPCTimeout)                         // Bound cleanup so an unreachable worker cannot stall queue ownership indefinitely.
	response, err := client.CancelPlan(cancelCtx, &rpc.CancelPlanRequest{TraceID: traceID}) // Ask the selected worker to stop the remotely accepted trace.
	cancel()                                                                                // Release the rollback RPC timer immediately after the call returns.
	if err != nil {                                                                         // Keep cancellation best-effort because the original persistence or lifecycle result remains authoritative.
		d.logger.WarnContext(ctx, "failed to cancel accepted distributed trace", "trace_id", traceID, "worker_id", workerID, "attempt", attempt, "reason", reason, "error", err) // Surface cleanup failure for operator diagnosis.
		return                                                                                                                                                                   // Leave watchdog and worker lease loss as the fallback remote stop mechanisms.
	}
	status := ""         // Normalize nil responses into an empty status for safe structured logging.
	if response != nil { // Read the worker response only when the gRPC client returned one.
		status = response.Status // Preserve the stable cancellation outcome without logging arbitrary response messages.
	}
	d.logger.InfoContext(ctx, "cancelled accepted distributed trace", "trace_id", traceID, "worker_id", workerID, "attempt", attempt, "reason", reason, "cancel_status", status) // Record the best-effort rollback result.
}

// distributedReservationID derives the exact local capacity token shared by dispatch, callback, cancellation, and watchdog cleanup paths.
func distributedReservationID(traceID string, attempt int64) string {
	return fmt.Sprintf("%s:%d", traceID, attempt) // Combine the immutable trace id and monotonic attempt without relying on worker load counters alone.
}

// resolveWorkerSessionID maps one platform session identifier to the underlying Appium session identifier required by distributed workers.
func (d *Dispatcher) resolveWorkerSessionID(ctx context.Context, sessionID string) (string, error) {
	if strings.TrimSpace(sessionID) == "" { // Reject empty session identifiers because no Appium session mapping can be resolved without a concrete platform session id.
		return "", errors.New(errors.CodeSessionNotFound, "session not found") // Surface missing sessions as the same stable session-not-found error used elsewhere in the orchestrator.
	}

	appiumSessionID, err := d.cache.Get(ctx, appiumSessionKeyPrefix+sessionID) // Load the Appium session mapping created during StartSession so the worker can attach to the existing automation session.
	if err != nil {                                                            // Distinguish missing Redis keys from other cache failures before returning to the dispatcher.
		if err.Error() == "redis: nil" { // Convert missing mappings into one stable session-not-found result instead of leaking Redis-specific details.
			return "", errors.New(errors.CodeSessionNotFound, "session not found") // Surface the missing Appium mapping through the repository's standard session-not-found contract.
		}
		return "", errors.Wrap(errors.CodeStoreRead, "failed to load appium session mapping", err) // Surface Redis lookup failures as storage read errors because distributed execution cannot continue without the mapping.
	}
	if strings.TrimSpace(appiumSessionID) == "" { // Reject empty mapping values because they cannot identify a real Appium session for the worker.
		return "", errors.New(errors.CodeSessionNotFound, "session not found") // Surface empty mappings through the same stable session-not-found contract.
	}

	return appiumSessionID, nil // Return the real Appium session identifier so the worker can attach to the live automation session.
}

// monitorInflight periodically sweeps distributed in-flight traces so accepted plans that never report completion are eventually closed.
func (d *Dispatcher) monitorInflight(ctx context.Context) {
	ticker := time.NewTicker(d.inflightMonitorInterval()) // Create one periodic ticker so timeout checks run in the background without busy looping.
	defer ticker.Stop()                                   // Release the ticker resources when the dispatcher stops or the parent context is canceled.

	for { // Keep checking active distributed traces until shutdown or parent-context cancellation stops the dispatcher.
		select {
		case <-ctx.Done():
			return // Stop the monitor once the service context is canceled during shutdown.
		case <-d.stopCh:
			return // Stop the monitor once the dispatcher itself is asked to shut down.
		case <-ticker.C:
			d.checkInflightTimeouts(ctx) // Sweep the persisted distributed assignments and close any trace whose deadline has passed.
		}
	}
}

// inflightMonitorInterval returns the watchdog cadence used to sweep timed-out distributed traces.
func (d *Dispatcher) inflightMonitorInterval() time.Duration {
	if d.planTimeout > 0 && d.planTimeout < inflightMonitorTick { // Use a tighter sweep cadence for very small plan timeouts so tests and short-lived plans do not wait an extra full interval.
		interval := d.planTimeout / 4       // Sample several times inside the configured timeout window so expiration is detected promptly even for short plans.
		if interval < 25*time.Millisecond { // Clamp the minimum interval so the watchdog never degenerates into an expensive busy poll.
			return 25 * time.Millisecond // Return one small but bounded interval that still reacts quickly in tests.
		}
		return interval // Return the derived shorter interval for short configured plan timeout values.
	}

	return inflightMonitorTick // Return the default production-friendly cadence for normal multi-second or multi-minute plan timeouts.
}

// inflightDeadlineUnix returns the Unix-millisecond deadline used to time out one distributed trace, or zero when plan timeouts are disabled.
func (d *Dispatcher) inflightDeadlineUnix(dispatchedAt time.Time) int64 {
	if d.planTimeout <= 0 { // Skip deadline materialization when the dispatcher was created without a positive plan timeout.
		return 0 // Return zero so the watchdog treats the assignment as not timeout-managed.
	}

	return dispatchedAt.Add(d.planTimeout).UnixMilli() // Convert the configured plan timeout into one absolute deadline for the distributed assignment metadata.
}

// inflightLeaseDeadlineUnix returns the Unix-millisecond ownership-lease expiry for one accepted distributed trace.
func (d *Dispatcher) inflightLeaseDeadlineUnix(dispatchedAt time.Time) int64 {
	return dispatchedAt.Add(d.inflightLeaseDuration()).UnixMilli() // Convert the derived worker-ownership lease duration into one absolute expiry used by orphan detection.
}

// inflightLeaseDuration derives the distributed ownership lease from the worker heartbeat cadence plus one fixed grace window.
func (d *Dispatcher) inflightLeaseDuration() time.Duration {
	interval := d.heartbeatInterval // Start from the configured worker heartbeat cadence so lease expiry aligns with liveness expectations.
	if interval <= 0 {              // Fall back to the registry default when the caller omitted an explicit heartbeat cadence.
		interval = heartbeatInterval // Keep lease derivation aligned with the worker-registry heartbeat default used elsewhere in the distributed runtime.
	}

	return (interval * 3) + executionLeaseGrace // Give each worker three missed heartbeats plus one grace window before ownership is considered orphaned.
}

// persistInflightAssignment stores one distributed worker ownership lease and fails closed when Redis cannot durably accept it.
func (d *Dispatcher) persistInflightAssignment(ctx context.Context, traceID string, assignment inflightTraceAssignment) (string, error) {
	payload, err := json.Marshal(assignment) // Encode the accepted worker assignment into one Redis-storable JSON payload for cancel and timeout lookups.
	if err != nil {                          // Reject malformed metadata before any worker RPC can begin remote execution.
		return "", errors.Wrap(errors.CodeInternal, "failed to marshal inflight trace assignment", err) // Preserve the serialization cause under a stable internal code.
	}
	created, err := d.cache.SetNX(ctx, inflightTracePrefix+traceID, string(payload), inflightTraceTTL) // Create ownership only when no earlier accepted or provisional lease already exists for this trace.
	if err != nil {                                                                                    // Fail closed when Redis cannot persist the initial distributed ownership lease.
		return "", errors.Wrap(errors.CodeStoreWrite, "failed to persist inflight trace assignment", err) // Fail closed because unleased remote execution cannot be cancelled or ownership-validated safely.
	}
	if !created { // Reject dispatch when another ownership lease already protects this trace.
		return "", errors.New(errors.CodeStateConflict, "inflight trace assignment already exists") // Keep the queue message retryable without overwriting the existing owner.
	}
	return string(payload), nil // Return the exact stored value so rollback can atomically delete only this owner.
}

// recordAudit appends one best-effort sanitized distributed execution event without changing queue or lease control flow.
func (d *Dispatcher) recordAudit(ctx context.Context, action string, traceID string, result string, reason string, assignment inflightTraceAssignment) {
	if d.auditRecorder == nil { // Skip audit work when process wiring did not install an append-only sink.
		return // Preserve dispatcher behavior in protocol-only tests and minimal deployments.
	}
	metadata := map[string]interface{}{ // Build a safe metadata set that excludes plan JSON, Appium identifiers, credentials, and backend errors.
		"workerId":           assignment.WorkerID,           // Identify the assigned worker for fleet and incident correlation.
		"attempt":            assignment.Attempt,            // Preserve the monotonic distributed ownership token.
		"dispatchedAtUnix":   assignment.DispatchedAtUnix,   // Preserve when the current assignment began.
		"leaseExpiresAtUnix": assignment.LeaseExpiresAtUnix, // Preserve the ownership deadline used by orphan detection.
		"deadlineAtUnix":     assignment.DeadlineAtUnix,     // Preserve the absolute plan timeout deadline when configured.
	}
	event := audit.Event{TraceID: traceID, ActorID: assignment.WorkerID, ActorType: "worker", Action: action, ResourceType: "trace", ResourceID: traceID, Result: result, Reason: reason, Metadata: metadata, CreatedAt: time.Now().UTC()} // Normalize the distributed operation into the repository audit contract.
	if err := d.auditRecorder.Record(ctx, event); err != nil {                                                                                                                                                                             // Keep audit persistence best-effort after the authoritative scheduling state has already been decided.
		d.logger.WarnContext(ctx, "failed to record distributed execution audit event", "action", action, "trace_id", traceID, "worker_id", assignment.WorkerID, "error", err) // Surface sink failure without logging plan or credential data.
	}
}

// checkInflightTimeouts closes accepted distributed traces whose watchdog deadline has elapsed without any completion callback.
func (d *Dispatcher) checkInflightTimeouts(ctx context.Context) {
	keys, err := d.cache.Keys(ctx, inflightTracePrefix+"*") // Load the current set of persisted distributed assignments so the watchdog can inspect every accepted trace.
	if err != nil {                                         // Surface Redis lookup failures and try again on the next sweep instead of terminating the monitor loop.
		d.logger.ErrorContext(ctx, "failed to list inflight traces", // Emit the Redis lookup failure so operators can correlate missing timeout cleanup with cache issues.
			"error", err)
		return // Keep the background monitor alive so later sweeps can recover automatically once Redis is reachable again.
	}

	nowUnix := time.Now().UnixMilli() // Capture one wall-clock value so every timeout decision in this sweep uses the same comparison point.
	for _, key := range keys {        // Examine every persisted distributed assignment that still exists in Redis at the start of this sweep.
		traceID := key[len(inflightTracePrefix):]      // Recover the trace identifier from the Redis key suffix so terminalization and cleanup target the correct trace.
		rawAssignment, getErr := d.cache.Get(ctx, key) // Load the stored distributed assignment payload for this trace from Redis.
		if getErr != nil {                             // Skip assignments that disappeared mid-sweep while logging real cache failures for investigation.
			if getErr.Error() != "redis: nil" { // Ignore a disappearing key because a concurrent cancel or cleanup may have removed it legitimately.
				d.logger.WarnContext(ctx, "failed to load inflight trace assignment", // Emit the cache read failure so operators can diagnose missing timeout cleanup.
					"trace_id", traceID,
					"error", getErr)
			}
			continue // Move on to the next assignment because this trace no longer has reliable metadata for timeout evaluation.
		}

		assignment, parseErr := parseInflightTraceAssignment(rawAssignment) // Decode either the new JSON payload or the legacy plain-address value into one normalized assignment struct.
		if parseErr != nil {                                                // Skip malformed payloads while logging them so the monitor can keep processing healthy entries.
			d.logger.WarnContext(ctx, "failed to decode inflight trace assignment", // Emit malformed assignment payloads because they disable timeout cleanup for the affected trace.
				"trace_id", traceID,
				"error", parseErr)
			continue // Keep the sweep alive for other traces even when one assignment payload is malformed.
		}
		if assignment.LeaseExpiresAtUnix > 0 && assignment.LeaseExpiresAtUnix <= nowUnix { // Finalize traces whose ownership lease has expired before any terminal result arrived.
			leaseErr := errors.New(errors.CodeStateConflict, "distributed execution ownership lease expired")                                                                                      // Build one stable lease-expiry error so terminal payloads capture the orphaned distributed outcome clearly.
			if finalizerErr := d.finalizeQueuedTrace(ctx, traceID, "failed", "orphaned", "trace failed because distributed execution ownership lease expired", leaseErr, 0); finalizerErr != nil { // Persist the orphaned terminal outcome before dropping lease metadata.
				continue // Leave the assignment key in place so the next sweep can retry orphan terminalization after transient storage failures.
			}
			d.cleanupInflightTraceIfOwned(ctx, traceID, rawAssignment)                                 // Remove only the exact expired ownership lease read by this watchdog sweep.
			d.registry.ReleaseWorkerReservation(distributedReservationID(traceID, assignment.Attempt)) // Release the exact local capacity token after durable orphan terminalization.
			d.cleanupRetryState(ctx, traceID)                                                          // Drop any stale retry counter so later traces with the same identifier never inherit orphaned dispatch state.
			d.recordAudit(ctx, "execution.orphaned", traceID, "failure", "lease_expired", assignment)  // Append a sanitized ownership-loss audit event for investigation.
			d.logger.WarnContext(ctx, "distributed trace orphaned",                                    // Emit the orphan closure so operators can correlate it with the worker assignment that stopped renewing its lease.
				"trace_id", traceID,
				"worker_id", assignment.WorkerID,
				"address", assignment.Address,
				"attempt", assignment.Attempt)
			continue // Move on to the next in-flight trace after closing the orphaned distributed assignment.
		}
		if assignment.DeadlineAtUnix == 0 || assignment.DeadlineAtUnix > nowUnix { // Ignore assignments that either predate the watchdog deadline metadata or have not yet exceeded their plan-timeout deadline.
			continue // Leave active or legacy assignments untouched until a later sweep or another lifecycle event handles them.
		}

		timeoutErr := errors.New(errors.CodeTimeoutPlan, "distributed execution exceeded plan timeout")                                                                                       // Build the stable timeout error so terminal events carry the requirement-level plan-timeout code.
		if finalizerErr := d.finalizeQueuedTrace(ctx, traceID, "failed", "timeout", "trace failed because distributed execution exceeded plan timeout", timeoutErr, 0); finalizerErr != nil { // Persist the timeout terminal outcome before dropping watchdog metadata.
			continue // Leave the assignment key in place so the next sweep can retry terminalization after transient storage failures.
		}
		d.cleanupInflightTraceIfOwned(ctx, traceID, rawAssignment)                                 // Remove only the exact timed-out ownership lease read by this watchdog sweep.
		d.registry.ReleaseWorkerReservation(distributedReservationID(traceID, assignment.Attempt)) // Release the exact local capacity token after durable timeout terminalization.
		d.cleanupRetryState(ctx, traceID)                                                          // Drop any stale retry counter so later traces with the same identifier never inherit this timed-out dispatch state.
		d.recordAudit(ctx, "execution.timed_out", traceID, "failure", "plan_timeout", assignment)  // Append a sanitized distributed timeout audit event.
		d.logger.WarnContext(ctx, "distributed trace timed out",                                   // Emit the timeout closure so operators can correlate the terminal event with the stalled worker assignment.
			"trace_id", traceID,
			"worker_id", assignment.WorkerID,
			"address", assignment.Address,
			"attempt", assignment.Attempt)
	}
}

// parseInflightTraceAssignment normalizes either the new JSON payload or the legacy plain-address value into one assignment struct.
func parseInflightTraceAssignment(raw string) (inflightTraceAssignment, error) {
	if raw == "" { // Reject empty payloads explicitly because neither cancellation nor timeout cleanup can proceed without worker metadata.
		return inflightTraceAssignment{}, errors.New(errors.CodeInternal, "inflight trace assignment is empty") // Return a stable internal error so callers can log malformed watchdog state consistently.
	}
	if raw[0] != '{' { // Preserve compatibility with the older plain-address Redis value written before assignment metadata became structured JSON.
		return inflightTraceAssignment{Address: raw}, nil // Treat the raw string as a legacy worker address so cancellation still works for older assignments.
	}

	var assignment inflightTraceAssignment                           // Allocate the normalized destination struct used by the JSON decoder.
	if err := json.Unmarshal([]byte(raw), &assignment); err != nil { // Decode the structured assignment payload into the normalized in-memory representation.
		return inflightTraceAssignment{}, errors.Wrap(errors.CodeInternal, "failed to decode inflight trace assignment", err) // Surface malformed assignment payloads through the repository-standard error wrapper.
	}
	if assignment.Address == "" { // Reject structured payloads that omit the worker address because cancel propagation depends on it.
		return inflightTraceAssignment{}, errors.New(errors.CodeInternal, "inflight trace assignment address is empty") // Return a stable internal error so callers can log the malformed payload cleanly.
	}

	return assignment, nil // Return the normalized assignment metadata for timeout or cancellation handling.
}

// cleanupInflightTraceIfOwned removes one provisional assignment only when its exact original Redis value still owns the trace.
func (d *Dispatcher) cleanupInflightTraceIfOwned(ctx context.Context, traceID string, rawAssignment string) {
	removed, err := d.cache.CompareAndDelete(ctx, inflightTracePrefix+traceID, rawAssignment) // Prevent rollback after a lost RPC response from deleting a lease already renewed by the worker.
	if err != nil {                                                                           // Log Redis failures while allowing lease expiry and watchdog cleanup to recover later.
		d.logger.WarnContext(ctx, "failed to delete owned inflight trace assignment", "trace_id", traceID, "error", err) // Surface ownership-cleanup failure without changing the dispatch error classification.
		return                                                                                                           // Leave the lease intact because safe ownership deletion could not be established.
	}
	if !removed { // Record benign ownership loss when a callback already renewed or released the provisional value.
		d.logger.InfoContext(ctx, "inflight trace assignment changed before rollback", "trace_id", traceID) // Keep fast callback and response-loss races observable.
	}
}

// loadInflightTraceAssignment loads one persisted distributed execution lease for callback validation, renewal, or cancellation routing.
func (d *Dispatcher) loadInflightTraceAssignment(ctx context.Context, traceID string) (inflightTraceAssignment, string, bool, error) {
	rawAssignment, err := d.cache.Get(ctx, inflightTracePrefix+traceID) // Load the persisted distributed assignment so callback ownership can be validated against Redis lease state.
	if err != nil {                                                     // Treat missing keys as a clean "not found" signal while surfacing real cache failures to the caller.
		if err.Error() == "redis: nil" { // Translate the Redis nil sentinel into a stable absent-assignment result for higher-layer stale-result handling.
			return inflightTraceAssignment{}, "", false, nil // Report that no active distributed lease exists for the requested trace.
		}
		return inflightTraceAssignment{}, "", false, errors.Wrap(errors.CodeStoreRead, "failed to load inflight trace assignment", err) // Surface Redis read failures through the standard storage-read contract.
	}

	assignment, parseErr := parseInflightTraceAssignment(rawAssignment) // Decode the stored distributed assignment payload into the normalized in-memory lease model.
	if parseErr != nil {                                                // Surface malformed Redis payloads because distributed ownership cannot be validated safely without them.
		return inflightTraceAssignment{}, "", false, parseErr // Preserve the wrapped decode failure so callers can log or surface the broken lease state.
	}

	return assignment, rawAssignment, true, nil // Return decoded metadata plus the exact Redis value required by atomic ownership updates.
}

// assignmentMatches returns true when one persisted distributed lease still belongs to the supplied worker and execution attempt.
func assignmentMatches(assignment inflightTraceAssignment, workerID string, attempt int64) bool {
	return assignment.WorkerID == workerID && assignment.Attempt == attempt // Match both worker and attempt so stale callbacks cannot claim a new owner's trace state accidentally.
}

// RenewExecutionLease refreshes one distributed execution lease when the supplied worker and attempt still own the trace.
func (d *Dispatcher) RenewExecutionLease(ctx context.Context, traceID string, workerID string, attempt int64) (bool, error) {
	assignment, rawAssignment, found, err := d.loadInflightTraceAssignment(ctx, traceID) // Load current metadata and its exact Redis value before validating ownership or refreshing expiry.
	if err != nil {                                                                      // Surface Redis or payload failures because lease renewal cannot proceed safely without current assignment state.
		return false, err // Preserve the wrapped storage error for the caller.
	}
	if !found || !assignmentMatches(assignment, workerID, attempt) { // Reject renewal when the trace no longer has an active lease for this worker attempt.
		return false, nil // Report a stale or already-cleaned lease without treating it as an infrastructure failure.
	}

	now := time.Now()                                                                          // Capture one consistent renewal timestamp so the persisted renewal and expiry fields stay in sync.
	if assignment.LeaseExpiresAtUnix > 0 && assignment.LeaseExpiresAtUnix <= now.UnixMilli() { // Reject renewals that arrive only after the orchestrator already considers the lease expired.
		return false, nil // Report the lease as stale so the worker can stop treating itself as the active owner of this trace.
	}

	assignment.LastRenewedAtUnix = now.UnixMilli()                   // Record the successful renewal time so operators can see recent distributed ownership activity.
	assignment.LeaseExpiresAtUnix = d.inflightLeaseDeadlineUnix(now) // Extend ownership expiry from the current renewal moment.
	nextPayload, err := json.Marshal(assignment)                     // Encode the refreshed lease before the atomic ownership comparison.
	if err != nil {                                                  // Surface unexpected assignment serialization failures without touching the current lease.
		return false, errors.Wrap(errors.CodeInternal, "failed to marshal renewed inflight trace assignment", err) // Preserve the encoding cause under a stable internal code.
	}
	replaced, err := d.cache.CompareAndSet(ctx, inflightTracePrefix+traceID, rawAssignment, string(nextPayload), inflightTraceTTL) // Refresh only when the exact lease read above still owns the key.
	if err != nil {                                                                                                                // Require durable renewal so workers never receive a false ownership confirmation during Redis failure.
		d.recordAudit(ctx, "execution.lease_renew", traceID, "failure", "store_write", assignment)         // Append a stable fail-closed lease-renewal audit record.
		return false, errors.Wrap(errors.CodeStoreWrite, "failed to renew inflight trace assignment", err) // Surface Redis script failure for transport handling.
	}
	if !replaced { // Reject a renewal that lost ownership after its initial read.
		return false, nil // Tell the worker to stop because a newer lease or cleanup won the race.
	}

	return true, nil // Report that the distributed lease still belongs to this worker attempt and was refreshed successfully.
}

// ReleaseExecutionLease removes one distributed execution lease only when the supplied worker and attempt still own the trace.
func (d *Dispatcher) ReleaseExecutionLease(ctx context.Context, traceID string, workerID string, attempt int64) (bool, error) {
	assignment, rawAssignment, found, err := d.loadInflightTraceAssignment(ctx, traceID) // Load current metadata and its exact Redis value before deciding whether cleanup should proceed.
	if err != nil {                                                                      // Surface Redis or payload failures because lease cleanup cannot be validated safely without the current assignment state.
		return false, err // Preserve the wrapped storage error for the caller.
	}
	if !found || !assignmentMatches(assignment, workerID, attempt) { // Reject cleanup when the trace no longer has an active lease for this worker attempt.
		return false, nil // Report a stale or already-cleaned lease without treating it as an infrastructure failure.
	}

	released, err := d.cache.CompareAndDelete(ctx, inflightTracePrefix+traceID, rawAssignment) // Delete only the exact lease validated for this worker attempt.
	if err != nil {                                                                            // Require successful Redis deletion before declaring the ownership lease released.
		return false, errors.Wrap(errors.CodeStoreWrite, "failed to release inflight trace assignment", err) // Preserve retryable cleanup state when Redis rejects deletion.
	}
	if !released { // Treat a value change after the initial read as lost ownership rather than deleting a successor.
		return false, nil // Report stale cleanup so the caller leaves the newer lease untouched.
	}
	d.registry.ReleaseWorkerReservation(distributedReservationID(traceID, attempt)) // Release the exact local capacity token only after durable lease cleanup succeeds.
	return true, nil                                                                // Report that the distributed lease and matching local reservation were removed successfully.
}

// handleDispatchError decides whether to ACK (terminal or retries exhausted) or
// leave the message unACKed for auto-claim retry.
func (d *Dispatcher) handleDispatchError(ctx context.Context, stream, msgID, traceID string, err error) {
	d.logger.ErrorContext(ctx, "dispatch error",
		"trace_id", traceID,
		"error", err)

	if isTerminalError(err) {
		if finalizerErr := d.finalizeQueuedTrace(ctx, traceID, "failed", "dispatch_error", "trace failed because dispatch returned a terminal error", err, 0); finalizerErr != nil { // Persist a durable failed terminal outcome before removing the queue message permanently.
			return // Leave the message pending so auto-claim can retry terminalization after transient storage failures.
		}
		d.logger.WarnContext(ctx, "ACKing terminal error — will not retry",
			"trace_id", traceID,
			"error", err)
		d.ackMessage(ctx, stream, msgID)  // ACK only after the terminal failure has been durably recorded.
		d.cleanupRetryState(ctx, traceID) // Drop any stale retry counter once the terminal failure has been recorded.
		return
	}

	// Transient error — increment retry counter; ACK only if exhausted.
	count, incErr := d.cache.Incr(ctx, retryKeyPrefix+traceID, retryKeyTTL)
	if incErr != nil {
		d.logger.ErrorContext(ctx, "failed to increment retry counter",
			"trace_id", traceID,
			"error", incErr)
		// Leave unACKed; auto-claim will pick it up.
		return
	}

	if count >= int64(maxRetries) {
		if finalizerErr := d.finalizeQueuedTrace(ctx, traceID, "failed", "dispatch_retries_exhausted", "trace failed because dispatch retries were exhausted", err, count); finalizerErr != nil { // Persist a durable failed terminal outcome before removing the exhausted queue message permanently.
			return // Leave the message pending so auto-claim can retry terminalization after transient storage failures.
		}
		d.logger.WarnContext(ctx, "retries exhausted — ACKing message",
			"trace_id", traceID,
			"retries", count)
		d.ackMessage(ctx, stream, msgID)  // ACK only after the exhausted retry path has been durably closed in storage.
		d.cleanupRetryState(ctx, traceID) // Drop the retry counter once the terminal failure has been recorded.
		return
	}

	d.logger.InfoContext(ctx, "transient error — leaving for retry",
		"trace_id", traceID,
		"retries", count)
	// Don't ACK; auto-claim will re-deliver after claimMinIdleTime.
}

// isTerminalError returns true for errors that should never be retried.
func isTerminalError(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	terminalCodes := []errors.ErrorCode{
		errors.CodePlanInvalid,
		errors.CodeSessionDead,
		errors.CodeSessionBroken,
		errors.CodeStepUnsupported,
		errors.CodeSchemaInvalid,
	}
	for _, code := range terminalCodes {
		if errors.IsCode(err, code) {
			return true
		}
	}
	return false
}

// stringValue executes this operation.
func stringValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

// intValue executes this operation.
func intValue(v interface{}) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case string:
		i, _ := strconv.ParseInt(t, 10, 64)
		return i
	case []byte:
		i, _ := strconv.ParseInt(string(t), 10, 64)
		return i
	default:
		return 0
	}
}

// CancelPlan sets a cancellation marker in Redis so the next time the message is
// dequeued it will be ACKed and skipped without execution.
func (d *Dispatcher) CancelPlan(ctx context.Context, traceID string) error {
	key := cancelledTracePrefix + traceID
	if err := d.cache.Set(ctx, key, "1", cancelledTraceTTL); err != nil {
		d.logger.ErrorContext(ctx, "failed to set cancellation marker",
			"trace_id", traceID,
			"error", err)
		return errors.Wrap(errors.CodeInternal, "failed to set cancellation marker", err)
	}
	d.logger.InfoContext(ctx, "cancellation marker set",
		"trace_id", traceID)
	return nil
}

// CancelDispatchedPlan forwards cancel requests to the assigned worker when the
// trace is currently known to be in-flight in distributed mode.
// Returns true when a worker cancel RPC was successfully issued.
func (d *Dispatcher) CancelDispatchedPlan(ctx context.Context, traceID string) (bool, error) {
	rawAssignment, err := d.cache.Get(ctx, inflightTracePrefix+traceID)
	if err != nil {
		if err.Error() == "redis: nil" {
			return false, nil
		}
		return false, errors.Wrap(errors.CodeStoreRead, "failed to get inflight trace assignment", err)
	}
	assignment, err := parseInflightTraceAssignment(rawAssignment) // Decode the stored worker assignment so cancel propagation supports both structured metadata and legacy plain-address values.
	if err != nil {                                                // Surface malformed assignment metadata because cancel cannot be routed safely without a valid worker address.
		return false, err // Preserve the decoding error so callers can log or surface the broken watchdog state.
	}

	client, err := d.getWorkerClient(assignment.Address)
	if err != nil {
		return false, errors.Wrap(errors.CodeSchedNoWorker, "failed to connect to assigned worker", err)
	}

	rpcCtx, rpcCancel := context.WithTimeout(ctx, workerRPCTimeout)                  // Bound cancellation so an unreachable worker cannot stall the caller indefinitely.
	resp, err := client.CancelPlan(rpcCtx, &rpc.CancelPlanRequest{TraceID: traceID}) // Forward cancellation to the worker that owns the persisted assignment.
	rpcCancel()                                                                      // Release the per-cancellation timer immediately after the worker RPC returns.
	if err != nil {                                                                  // Surface transport and deadline failures without deleting ownership metadata prematurely.
		return false, errors.Wrap(errors.CodeSchedNoWorker, "worker CancelPlan RPC failed", err) // Keep the lease available for retry or watchdog cleanup.
	}
	if resp == nil { // Reject malformed successful RPC responses that omit a cancellation status.
		return false, errors.New(errors.CodeInternal, "worker returned an empty CancelPlan response") // Preserve lease state because cancellation outcome is unknown.
	}

	switch resp.Status { // Normalize the worker's stable cancellation result into lease and capacity cleanup.
	case "cancelled":
		cleanupErr := d.cleanupCancelledAssignment(ctx, traceID, rawAssignment, assignment.Attempt) // Remove only the cancelled owner's lease and exact local reservation.
		return true, cleanupErr                                                                     // Report that a live worker execution was cancelled while preserving any Redis cleanup failure.
	case "not_found":
		cleanupErr := d.cleanupCancelledAssignment(ctx, traceID, rawAssignment, assignment.Attempt) // Remove only the stale owner's lease and exact local reservation.
		return false, cleanupErr                                                                    // Report no live remote cancellation while preserving any Redis cleanup failure.
	default:
		return false, errors.New(errors.CodeInternal, "worker returned unexpected cancel status") // Avoid propagating arbitrary worker-controlled status text into higher-layer errors.
	}
}

// cleanupCancelledAssignment atomically removes the lease value routed to the cancelled worker and releases its exact local capacity token.
func (d *Dispatcher) cleanupCancelledAssignment(ctx context.Context, traceID string, rawAssignment string, attempt int64) error {
	removed, err := d.cache.CompareAndDelete(ctx, inflightTracePrefix+traceID, rawAssignment) // Prevent a stale cancellation response from deleting a newer trace owner.
	d.registry.ReleaseWorkerReservation(distributedReservationID(traceID, attempt))           // Release the cancelled attempt's exact local token regardless of later lease replacement.
	if err != nil {                                                                           // Surface Redis cleanup failures after local capacity has been reconciled.
		return errors.Wrap(errors.CodeStoreWrite, "failed to remove cancelled inflight trace assignment", err) // Preserve retryable stale Redis state for operator handling.
	}
	if !removed { // Record benign ownership change without deleting the successor lease.
		d.logger.InfoContext(ctx, "cancelled inflight assignment no longer owned at cleanup", "trace_id", traceID, "attempt", attempt) // Keep cross-attempt cancellation races observable.
	}
	return nil // Confirm that cleanup either removed this exact lease or safely left a successor untouched.
}

// finalizeQueuedTrace records one dispatcher-owned terminal outcome before the queue message is ACKed.
func (d *Dispatcher) finalizeQueuedTrace(ctx context.Context, traceID string, nextStatus string, terminalReason string, message string, cause error, retryCount int64) error {
	if d.finalizer == nil { // Allow legacy callers to keep the previous ACK-only behavior when no terminalization helper was wired in.
		return nil // Return success so dispatcher behavior remains backward compatible for callers that do not provide a finalizer.
	}
	if err := d.finalizer.FinalizeQueuedTrace(ctx, traceID, nextStatus, terminalReason, message, cause, retryCount); err != nil { // Delegate the durable terminalization to the orchestrator service before the queue message is removed.
		d.logger.ErrorContext(ctx, "failed to finalize queued trace", // Emit the failure so operators can correlate a stuck message with the storage-layer problem.
			"trace_id", traceID,
			"next_status", nextStatus,
			"terminal_reason", terminalReason,
			"error", err)
		return err // Preserve the finalizer failure so callers can avoid ACKing the queue message prematurely.
	}

	return nil // Report success once the terminal outcome has been durably recorded or confirmed already terminal.
}

// ackMessage acknowledges one Redis stream message and logs any failure without changing the caller's control flow.
func (d *Dispatcher) ackMessage(ctx context.Context, stream string, msgID string) {
	if err := d.cache.XAck(ctx, stream, consumerGroup, msgID); err != nil { // Attempt to ACK the consumed message once the caller has decided it should leave the queue permanently.
		d.logger.ErrorContext(ctx, "failed to ack message", // Surface ACK failures so operators can diagnose duplicate delivery or consumer-group issues.
			"stream", stream,
			"message_id", msgID,
			"error", err)
	}
}

// cleanupRetryState removes the per-trace dispatch retry counter after a message has reached a durable terminal outcome or a successful dispatch.
func (d *Dispatcher) cleanupRetryState(ctx context.Context, traceID string) {
	if err := d.cache.Del(ctx, retryKeyPrefix+traceID); err != nil { // Drop the retry counter so future traces with the same identifier never inherit stale retry state.
		d.logger.WarnContext(ctx, "failed to delete dispatch retry counter", // Surface cleanup failures without treating them as fatal because the trace already reached a stable outcome.
			"trace_id", traceID,
			"error", err)
	}
}

// HealthSummary returns queue and in-flight counts used by the orchestrator health endpoint.
func (d *Dispatcher) HealthSummary(ctx context.Context) (queueHealthSummary, error) {
	summary := queueHealthSummary{Shards: numShards} // Seed the queue snapshot with the fixed shard count so health output always reports how many streams were inspected.
	for i := 0; i < numShards; i++ {                 // Inspect every dispatcher shard so queue health reflects the whole stream set instead of a single shard.
		stream := d.streamName(i)                  // Resolve the Redis stream name for the current shard before querying retained and pending counts.
		retained, err := d.cache.XLen(ctx, stream) // Read the retained entry count so health output can reveal whether the stream is growing unexpectedly.
		if err != nil {                            // Stop immediately when Redis cannot report the retained count for one shard because the queue snapshot is then incomplete.
			return queueHealthSummary{}, errors.Wrap(errors.CodeStoreRead, "failed to inspect queue stream length", err) // Surface the queue-inspection failure through the standard storage-read error contract.
		}
		summary.RetainedMessages += retained // Accumulate the retained entries across all shards so operators can see total stream depth at a glance.

		pending, err := d.cache.XPendingCount(ctx, stream, consumerGroup) // Read the consumer-group pending count so health output can reveal stuck claimed work.
		if err != nil {                                                   // Ignore missing-group errors but surface any other Redis failure because it hides queue state.
			if !isMissingConsumerGroupError(err) { // Treat missing groups as zero pending entries because a newly started dispatcher may not have created them yet.
				return queueHealthSummary{}, errors.Wrap(errors.CodeStoreRead, "failed to inspect queue pending count", err) // Surface real Redis failures through the standard storage-read error contract.
			}
		} else {
			summary.PendingMessages += pending // Accumulate the pending entries across all shards once Redis has reported the count successfully.
		}
	}

	keys, err := d.cache.Keys(ctx, inflightTracePrefix+"*") // Load every in-flight assignment key so health output can report how many distributed traces are currently active.
	if err != nil {                                         // Stop immediately when Redis cannot list the in-flight assignment keys because the execution-health snapshot is incomplete.
		return queueHealthSummary{}, errors.Wrap(errors.CodeStoreRead, "failed to inspect inflight trace assignments", err) // Surface the key-listing failure through the standard storage-read error contract.
	}
	summary.InflightTraces = len(keys) // Record the current number of distributed traces that are still tracked as in-flight after queue ACK.

	return summary, nil // Return the completed queue and in-flight snapshot for health-check reporting.
}

// isMissingConsumerGroupError returns true when a queue-inspection error only means the Redis stream group has not been created yet.
func isMissingConsumerGroupError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP") // Match the Redis consumer-group missing error so health checks can treat never-started streams as zero pending instead of hard-failing.
}

// getWorkerClient returns a cached gRPC client for the given worker address,
// creating one on first use.
func (d *Dispatcher) getWorkerClient(address string) (*rpc.WorkerClient, error) {
	d.workerClientMu.Lock()
	defer d.workerClientMu.Unlock()

	if c, ok := d.workerClients[address]; ok {
		return c, nil
	}
	c, err := rpc.NewWorkerClient(address, d.rpcSecurity)
	if err != nil {
		return nil, err
	}
	d.workerClients[address] = c
	return c, nil
}
