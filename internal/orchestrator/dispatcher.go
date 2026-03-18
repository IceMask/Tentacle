package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/telemetry"
)

const (
	numShards            = 8
	consumerGroup        = "orchestrator-group"
	consumerName         = "orchestrator-consumer"
	batchSize            = 10
	blockTime            = 5 * time.Second
	claimMinIdleTime     = 30 * time.Second
	maxRetries           = 3
	retryBackoffBase     = 200 * time.Millisecond
	retryBackoffJitter   = 1000 * time.Millisecond
	streamTrimMaxLen     = 10000
	streamTrimApprox     = true
	cancelledTracePrefix = "cancelled:trace:"
	cancelledTraceTTL    = time.Hour
	retryKeyPrefix       = "retries:trace:"
	retryKeyTTL          = 10 * time.Minute
	inflightTracePrefix  = "inflight:trace:"
	inflightTraceTTL     = 2 * time.Hour
	inflightMonitorTick  = 5 * time.Second
)

type Dispatcher struct {
	cache          *redis.Cache
	registry       *WorkerRegistry
	executor       PlanExecutor
	finalizer      TraceFinalizer
	logger         *slog.Logger
	stopCh         chan struct{}
	workerClients  map[string]*rpc.WorkerClient
	workerClientMu sync.Mutex
	planTimeout    time.Duration
}

// TraceFinalizer records dispatcher-owned terminal outcomes for queued traces before the queue message is ACKed.
type TraceFinalizer interface {
	FinalizeQueuedTrace(ctx context.Context, traceID string, nextStatus string, message string, cause error, retryCount int64) error
}

// traceRunningMarker records that a distributed trace has left the queue and is now actively executing on a worker.
type traceRunningMarker interface {
	MarkDispatchedTraceRunning(ctx context.Context, traceID string) error
}

// inflightTraceAssignment stores the worker and deadline metadata needed to cancel or time out one distributed trace after the queue message has been ACKed.
type inflightTraceAssignment struct {
	Address          string `json:"address"`
	WorkerID         string `json:"workerId,omitempty"`
	DispatchedAtUnix int64  `json:"dispatchedAtUnix"`
	DeadlineAtUnix   int64  `json:"deadlineAtUnix,omitempty"`
}

// NewDispatcher executes this operation.
func NewDispatcher(cache *redis.Cache, registry *WorkerRegistry, executor PlanExecutor, planTimeout time.Duration) *Dispatcher {
	return &Dispatcher{
		cache:         cache,
		registry:      registry,
		executor:      executor,
		logger:        telemetry.Logger(),
		stopCh:        make(chan struct{}),
		workerClients: make(map[string]*rpc.WorkerClient),
		planTimeout:   planTimeout,
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
		if err := d.finalizeQueuedTrace(ctx, traceID, "cancelled", "trace cancelled before execution", nil, 0); err != nil { // Persist the cancelled terminal outcome before ACK so queued cancels cannot disappear without closing the trace.
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
	w := d.registry.AssignWorker(ctx, projectID, map[string]string{})
	if w == nil {
		d.logger.WarnContext(ctx, "no available worker",
			"trace_id", traceID,
			"project_id", projectID)
		// Return a transient scheduling error so upstream retry logic keeps
		// the message pending for later auto-claim re-dispatch.
		return errors.New(errors.CodeSchedNoWorker, "no available worker")
	}

	client, err := d.getWorkerClient(w.Address)
	if err != nil {
		return errors.Wrap(errors.CodeSchedNoWorker, "failed to connect to worker", err)
	}

	resp, err := client.ExecutePlan(ctx, &rpc.ExecutePlanRequest{
		TraceID:   traceID,
		SessionID: sessionID,
		Plan:      json.RawMessage(planStr),
	})
	if err != nil {
		return errors.Wrap(errors.CodeInternal, "worker ExecutePlan RPC failed", err)
	}
	if resp.Status != "accepted" {
		return errors.New(errors.CodePlanInvalid, "worker rejected plan: "+resp.Message)
	}

	d.logger.InfoContext(ctx, "dispatched to worker",
		"trace_id", traceID,
		"worker_id", w.ID)
	d.markDispatchedTraceRunning(ctx, traceID)                         // Best-effort mark the accepted trace as running so polling clients stop seeing it as merely queued once the worker has already started.
	d.persistInflightAssignment(ctx, traceID, inflightTraceAssignment{ // Best-effort persist the worker assignment and timeout deadline so distributed cancel and timeout sweeps can find the trace after queue ACK.
		Address:          w.Address,                          // Record the worker address so later cancel RPCs can reach the same worker instance.
		WorkerID:         w.ID,                               // Record the worker identifier so timeout logs can pinpoint the stalled worker assignment.
		DispatchedAtUnix: time.Now().UnixMilli(),             // Capture the dispatch timestamp so operators can diagnose how long the trace has been in flight.
		DeadlineAtUnix:   d.inflightDeadlineUnix(time.Now()), // Capture the absolute timeout deadline derived from the configured plan timeout for later watchdog sweeps.
	}) // Persist the accepted distributed assignment metadata without risking duplicate dispatch if Redis bookkeeping fails after acceptance.
	return nil
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

// persistInflightAssignment stores one accepted distributed worker assignment and logs any failure without triggering a dangerous queue retry.
func (d *Dispatcher) persistInflightAssignment(ctx context.Context, traceID string, assignment inflightTraceAssignment) {
	payload, err := json.Marshal(assignment) // Encode the accepted worker assignment into one Redis-storable JSON payload for cancel and timeout lookups.
	if err != nil {                          // Surface impossible-but-real serialization failures without changing control flow after the worker already accepted the plan.
		d.logger.ErrorContext(ctx, "failed to marshal inflight trace assignment", // Emit the failure loudly because timeout and cancellation bookkeeping will be incomplete for this accepted trace.
			"trace_id", traceID,
			"worker_id", assignment.WorkerID,
			"error", err)
		return // Preserve the accepted dispatch outcome because retrying after worker acceptance could duplicate execution.
	}
	if err := d.cache.Set(ctx, inflightTracePrefix+traceID, string(payload), inflightTraceTTL); err != nil { // Persist the worker assignment with a bounded TTL so stale distributed traces do not remain forever after watchdog failure.
		d.logger.WarnContext(ctx, "failed to persist inflight trace assignment", // Surface the Redis failure because later cancel and timeout sweeps may not be able to find the accepted trace.
			"trace_id", traceID,
			"worker_id", assignment.WorkerID,
			"error", err)
	}
}

// markDispatchedTraceRunning promotes one accepted distributed trace from pending to running when the finalizer exposes that capability.
func (d *Dispatcher) markDispatchedTraceRunning(ctx context.Context, traceID string) {
	marker, ok := d.finalizer.(traceRunningMarker) // Detect whether the wired finalizer can also mark queued traces as running after worker acceptance.
	if !ok {                                       // Skip the status promotion when legacy callers wired only the terminal-finalization interface.
		return // Preserve backward compatibility for callers that have not yet implemented running-state promotion.
	}
	if err := marker.MarkDispatchedTraceRunning(ctx, traceID); err != nil { // Best-effort promote the trace state without risking duplicate dispatch on storage failure.
		d.logger.WarnContext(ctx, "failed to mark distributed trace running", // Emit the failure so operators can diagnose traces that stay pending after worker acceptance.
			"trace_id", traceID,
			"error", err)
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
		if assignment.DeadlineAtUnix == 0 || assignment.DeadlineAtUnix > nowUnix { // Ignore assignments that either predate the watchdog metadata or have not yet exceeded their plan-timeout deadline.
			continue // Leave active or legacy assignments untouched until a later sweep or another lifecycle event handles them.
		}

		timeoutErr := errors.New(errors.CodeTimeoutPlan, "distributed execution exceeded plan timeout")                                                                            // Build the stable timeout error so terminal events carry the requirement-level plan-timeout code.
		if finalizerErr := d.finalizeQueuedTrace(ctx, traceID, "failed", "trace failed because distributed execution exceeded plan timeout", timeoutErr, 0); finalizerErr != nil { // Persist the timeout terminal outcome before dropping watchdog metadata.
			continue // Leave the assignment key in place so the next sweep can retry terminalization after transient storage failures.
		}
		d.cleanupInflightTrace(ctx, traceID)                     // Remove the accepted-assignment bookkeeping once the trace has reached a durable terminal timeout state.
		d.cleanupRetryState(ctx, traceID)                        // Drop any stale retry counter so later traces with the same identifier never inherit this timed-out dispatch state.
		d.logger.WarnContext(ctx, "distributed trace timed out", // Emit the timeout closure so operators can correlate the terminal event with the stalled worker assignment.
			"trace_id", traceID,
			"worker_id", assignment.WorkerID,
			"address", assignment.Address)
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

// cleanupInflightTrace removes one accepted distributed assignment after cancellation, completion, or timeout terminalization.
func (d *Dispatcher) cleanupInflightTrace(ctx context.Context, traceID string) {
	if err := d.cache.Del(ctx, inflightTracePrefix+traceID); err != nil { // Delete the Redis assignment key so future cancel or timeout sweeps do not see a stale in-flight trace.
		d.logger.WarnContext(ctx, "failed to delete inflight trace assignment", // Surface cleanup failures without changing control flow because the trace already reached a stable outcome.
			"trace_id", traceID,
			"error", err)
	}
}

// handleDispatchError decides whether to ACK (terminal or retries exhausted) or
// leave the message unACKed for auto-claim retry.
func (d *Dispatcher) handleDispatchError(ctx context.Context, stream, msgID, traceID string, err error) {
	d.logger.ErrorContext(ctx, "dispatch error",
		"trace_id", traceID,
		"error", err)

	if isTerminalError(err) {
		if finalizerErr := d.finalizeQueuedTrace(ctx, traceID, "failed", "trace failed because dispatch returned a terminal error", err, 0); finalizerErr != nil { // Persist a durable failed terminal outcome before removing the queue message permanently.
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
		if finalizerErr := d.finalizeQueuedTrace(ctx, traceID, "failed", "trace failed because dispatch retries were exhausted", err, count); finalizerErr != nil { // Persist a durable failed terminal outcome before removing the exhausted queue message permanently.
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

	resp, err := client.CancelPlan(ctx, &rpc.CancelPlanRequest{TraceID: traceID})
	if err != nil {
		return false, errors.Wrap(errors.CodeInternal, "worker CancelPlan RPC failed", err)
	}

	switch resp.Status {
	case "cancelled":
		d.cleanupInflightTrace(ctx, traceID) // Remove the in-flight assignment because the worker has acknowledged cancellation for this trace.
		return true, nil
	case "not_found":
		d.cleanupInflightTrace(ctx, traceID) // Remove the stale assignment because the worker no longer knows about this supposedly in-flight trace.
		return false, nil
	default:
		return false, errors.New(errors.CodeInternal, "worker returned unexpected cancel status: "+resp.Status)
	}
}

// finalizeQueuedTrace records one dispatcher-owned terminal outcome before the queue message is ACKed.
func (d *Dispatcher) finalizeQueuedTrace(ctx context.Context, traceID string, nextStatus string, message string, cause error, retryCount int64) error {
	if d.finalizer == nil { // Allow legacy callers to keep the previous ACK-only behavior when no terminalization helper was wired in.
		return nil // Return success so dispatcher behavior remains backward compatible for callers that do not provide a finalizer.
	}
	if err := d.finalizer.FinalizeQueuedTrace(ctx, traceID, nextStatus, message, cause, retryCount); err != nil { // Delegate the durable terminalization to the orchestrator service before the queue message is removed.
		d.logger.ErrorContext(ctx, "failed to finalize queued trace", // Emit the failure so operators can correlate a stuck message with the storage-layer problem.
			"trace_id", traceID,
			"next_status", nextStatus,
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

// getWorkerClient returns a cached gRPC client for the given worker address,
// creating one on first use.
func (d *Dispatcher) getWorkerClient(address string) (*rpc.WorkerClient, error) {
	d.workerClientMu.Lock()
	defer d.workerClientMu.Unlock()

	if c, ok := d.workerClients[address]; ok {
		return c, nil
	}
	c, err := rpc.NewWorkerClient(address)
	if err != nil {
		return nil, err
	}
	d.workerClients[address] = c
	return c, nil
}
