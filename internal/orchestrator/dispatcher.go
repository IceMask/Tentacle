package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/telemetry"
)

const (
	numShards          = 8
	consumerGroup      = "orchestrator-group"
	consumerName       = "orchestrator-consumer"
	batchSize          = 10
	blockTime          = 5 * time.Second
	claimMinIdleTime   = 30 * time.Second
	maxRetries         = 3
	retryBackoffBase   = 200 * time.Millisecond
	retryBackoffJitter = 1000 * time.Millisecond
	streamTrimMaxLen   = 10000
	streamTrimApprox   = true
)

type Dispatcher struct {
	cache    *redis.Cache
	registry *WorkerRegistry
	logger   *slog.Logger
	stopCh   chan struct{}
}

func NewDispatcher(cache *redis.Cache, registry *WorkerRegistry) *Dispatcher {
	return &Dispatcher{
		cache:    cache,
		registry: registry,
		logger:   telemetry.Logger(),
		stopCh:   make(chan struct{}),
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
func (d *Dispatcher) EnqueuePlan(ctx context.Context, projectID string, traceID string, plan json.RawMessage) error {
	shard := d.shardFor(projectID)
	stream := d.streamName(shard)

	values := map[string]interface{}{
		"trace_id":    traceID,
		"project_id":  projectID,
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
	planStr := stringValue(msg.Values["plan"])
	retryCount := intValue(msg.Values["retry_count"])

	d.logger.InfoContext(ctx, "dispatching plan",
		"trace_id", traceID,
		"project_id", projectID,
		"retry_count", retryCount)

	// Get available worker
	worker := d.registry.AssignWorker(ctx, projectID, map[string]string{})
	if worker == nil {
		d.logger.WarnContext(ctx, "no available worker",
			"trace_id", traceID,
			"project_id", projectID)
		// Don't ACK - will be retried or auto-claimed
		return
	}

	// TODO: Call worker gRPC ExecutePlan
	// For now, simulate dispatch
	d.logger.InfoContext(ctx, "dispatched to worker",
		"trace_id", traceID,
		"worker_id", worker.ID)

	// In real implementation, only ACK after successful RPC call or max retries
	if err := d.cache.XAck(ctx, stream, consumerGroup, msg.ID); err != nil {
		d.logger.ErrorContext(ctx, "failed to ack message",
			"stream", stream,
			"message_id", msg.ID,
			"error", err)
	}

	// Placeholder: would increment metrics here
	_ = planStr // Use planStr in actual RPC call
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

// CancelPlan cancels a pending plan in the queue
func (d *Dispatcher) CancelPlan(ctx context.Context, traceID string) error {
	// Search all shards for the trace
	for i := 0; i < numShards; i++ {
		// This is simplified - in production you'd maintain a trace->message_id index
		d.logger.InfoContext(ctx, "attempting to cancel plan in shard",
			"trace_id", traceID,
			"shard", i,
			"stream", d.streamName(i))
		// TODO: implement actual cancellation logic
	}
	return nil
}
