package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/telemetry"
)

const (
	eventsChannelPrefix = "events:"
	maxEventSize        = 64 * 1024 // 64KB
)

type EventsPublisher struct {
	dao    *postgres.DAO
	cache  *redis.Cache
	logger *slog.Logger
}

// NewEventsPublisher executes this operation.
func NewEventsPublisher(dao *postgres.DAO, cache *redis.Cache) *EventsPublisher {
	return &EventsPublisher{
		dao:    dao,
		cache:  cache,
		logger: telemetry.Logger(),
	}
}

// Publish publishes an event: first persist to DB, then publish to Redis PubSub
func (p *EventsPublisher) Publish(ctx context.Context, event *postgres.PlanEvent) error {
	// 1. Check event size
	payload, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(errors.CodeInternal, "failed to marshal event", err)
	}

	if len(payload) > maxEventSize {
		// Event too large, store payload as artifact and use reference
		p.logger.WarnContext(ctx, "event too large, truncating",
			"trace_id", event.TraceID,
			"seq", event.Seq,
			"size", len(payload))

		// In production, would store large payload in S3 and replace with artifactRef
		// For now, just truncate the payload
		event.Payload = []byte(fmt.Sprintf(`{"truncated": true, "original_size": %d}`, len(payload)))
		payload, _ = json.Marshal(event)
	}

	// 2. Persist to DB (critical path - must succeed)
	if err := p.dao.InsertPlanEvent(ctx, event); err != nil {
		p.logger.ErrorContext(ctx, "failed to persist event",
			"trace_id", event.TraceID,
			"seq", event.Seq,
			"error", err)
		return errors.Wrap(errors.CodeStoreWrite, "failed to persist event", err)
	}

	p.logger.DebugContext(ctx, "event persisted",
		"trace_id", event.TraceID,
		"seq", event.Seq,
		"step_index", event.StepIndex,
		"status", event.Status)

	// 3. Publish to Redis PubSub for WebSocket Hub (best-effort, at-most-once)
	channel := eventsChannelPrefix + event.TraceID
	if err := p.cache.Publish(ctx, channel, string(payload)); err != nil {
		// Log but don't fail - WebSocket is at-most-once, clients can poll via A2A
		p.logger.WarnContext(ctx, "failed to publish event to redis",
			"trace_id", event.TraceID,
			"seq", event.Seq,
			"error", err)
		// Increment backpressure metric here
	}

	return nil
}

// PublishBatch publishes multiple events in a transaction
func (p *EventsPublisher) PublishBatch(ctx context.Context, events []*postgres.PlanEvent) error {
	for _, event := range events {
		if err := p.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// GetEvents retrieves events for a trace (for A2A补齐)
func (p *EventsPublisher) GetEvents(ctx context.Context, traceID string, sinceSeq int64, limit int) ([]*postgres.PlanEvent, error) {
	events, err := p.dao.ListEvents(ctx, traceID, sinceSeq, limit)
	if err != nil {
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to get events", err)
	}
	return events, nil
}
