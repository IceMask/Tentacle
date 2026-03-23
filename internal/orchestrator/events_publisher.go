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

// PublishBatch persists the full event batch atomically and only publishes to Redis after the database commit succeeds.
func (p *EventsPublisher) PublishBatch(ctx context.Context, events []*postgres.PlanEvent) error {
	payloads := make([]string, 0, len(events))                // Preserve the normalized payload for each event so Redis publish happens only after the database commit succeeds.
	err := p.dao.WithTx(ctx, func(tx *postgres.TxDAO) error { // Insert the full batch in one transaction so readers never observe a partially persisted sequence.
		for _, event := range events { // Normalize and insert every event under the same transaction boundary.
			payload, err := p.prepareEventPayload(ctx, event) // Reuse the same payload normalization rules as the single-event path before persisting the row.
			if err != nil {                                   // Abort the batch immediately when any event cannot be serialized safely.
				return err // Propagate the original serialization failure so the transaction rolls back automatically.
			}
			if err := tx.InsertPlanEvent(ctx, event); err != nil { // Persist the normalized event inside the still-open transaction.
				return err // Stop on the first insert failure so the transaction rolls back the entire batch.
			}
			payloads = append(payloads, payload) // Keep the committed payload snapshot for the later Redis publish loop.
		}
		return nil // Signal that all event rows were inserted successfully so DAO.WithTx can commit them atomically.
	})
	if err != nil { // Return the transactional failure before attempting any best-effort Redis publish.
		return err // Preserve the original DAO or serialization error for the caller.
	}

	for index, event := range events { // Publish each already-committed event best-effort to the websocket fan-out channel.
		channel := eventsChannelPrefix + event.TraceID                         // Route the message to the per-trace pubsub channel consumed by the websocket hub.
		if err := p.cache.Publish(ctx, channel, payloads[index]); err != nil { // Publish the normalized payload that matches the row already stored in Postgres.
			p.logger.WarnContext(ctx, "failed to publish event to redis", // Record the pubsub failure without failing the batch because A2A polling can recover it.
				"trace_id", event.TraceID,
				"seq", event.Seq,
				"error", err)
		}
	}

	return nil // Report success once the batch is durable and all best-effort publish attempts have been made.
}

// prepareEventPayload normalizes an event payload for durable storage and later pubsub fan-out.
func (p *EventsPublisher) prepareEventPayload(ctx context.Context, event *postgres.PlanEvent) (string, error) {
	payload, err := json.Marshal(event) // Serialize the full event first so the size check sees the exact wire payload.
	if err != nil {                     // Stop immediately when the event cannot be marshaled to JSON.
		return "", errors.Wrap(errors.CodeInternal, "failed to marshal event", err) // Return the same internal error contract used by the single-event publish path.
	}

	if len(payload) > maxEventSize { // Apply the same truncation behavior used by Publish when the event exceeds the websocket payload budget.
		p.logger.WarnContext(ctx, "event too large, truncating", // Emit an operational warning so oversized payloads can be investigated later.
			"trace_id", event.TraceID,
			"seq", event.Seq,
			"size", len(payload))
		event.Payload = []byte(fmt.Sprintf(`{"truncated": true, "original_size": %d}`, len(payload))) // Replace the oversized payload with a compact marker before persistence.
		payload, _ = json.Marshal(event)                                                              // Re-marshal the normalized event so the Redis publish matches the stored row.
	}

	return string(payload), nil // Return the normalized payload string for the post-commit Redis publish step.
}

// GetEvents retrieves events for a trace so A2A consumers can fill websocket delivery gaps.
func (p *EventsPublisher) GetEvents(ctx context.Context, traceID string, sinceSeq int64, limit int) ([]*postgres.PlanEvent, error) {
	events, err := p.dao.ListEvents(ctx, traceID, sinceSeq, limit)
	if err != nil {
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to get events", err)
	}
	return events, nil
}
