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
)

type Dispatcher struct {
	cache          *redis.Cache
	registry       *WorkerRegistry
	executor       PlanExecutor
	logger         *slog.Logger
	stopCh         chan struct{}
	workerClients  map[string]*rpc.WorkerClient
	workerClientMu sync.Mutex
}

func NewDispatcher(cache *redis.Cache, registry *WorkerRegistry, executor PlanExecutor) *Dispatcher {
	return &Dispatcher{
		cache:         cache,
		registry:      registry,
		executor:      executor,
		logger:        telemetry.Logger(),
		stopCh:        make(chan struct{}),
		workerClients: make(map[string]*rpc.WorkerClient),
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
		_ = d.cache.XAck(ctx, stream, consumerGroup, msg.ID)
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

	if err := d.cache.XAck(ctx, stream, consumerGroup, msg.ID); err != nil {
		d.logger.ErrorContext(ctx, "failed to ack message",
			"stream", stream,
			"message_id", msg.ID,
			"error", err)
	}
}

// dispatchToWorker assigns a worker and sends the plan via gRPC.
func (d *Dispatcher) dispatchToWorker(ctx context.Context, traceID, projectID, sessionID, planStr string) error {
	w := d.registry.AssignWorker(ctx, projectID, map[string]string{})
	if w == nil {
		d.logger.WarnContext(ctx, "no available worker",
			"trace_id", traceID,
			"project_id", projectID)
		// Return nil so we do NOT ACK — message stays for auto-claim retry.
		return nil
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
	return nil
}

// handleDispatchError decides whether to ACK (terminal or retries exhausted) or
// leave the message unACKed for auto-claim retry.
func (d *Dispatcher) handleDispatchError(ctx context.Context, stream, msgID, traceID string, err error) {
	d.logger.ErrorContext(ctx, "dispatch error",
		"trace_id", traceID,
		"error", err)

	if isTerminalError(err) {
		d.logger.WarnContext(ctx, "ACKing terminal error — will not retry",
			"trace_id", traceID,
			"error", err)
		_ = d.cache.XAck(ctx, stream, consumerGroup, msgID)
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
		d.logger.WarnContext(ctx, "retries exhausted — ACKing message",
			"trace_id", traceID,
			"retries", count)
		_ = d.cache.XAck(ctx, stream, consumerGroup, msgID)
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
