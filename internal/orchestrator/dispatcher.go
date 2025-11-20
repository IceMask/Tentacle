package orchestrator

import (
	"context"
	"fmt"
	"github.com/mcp/mobile-worker/internal/storage/redis"
)

type Dispatcher struct {
	redisClient *redis.Client
}

func NewDispatcher(redisClient *redis.Client) *Dispatcher {
	return &Dispatcher{redisClient: redisClient}
}

func (d *Dispatcher) Dispatch(ctx context.Context, tenantId string, projectId string, payload map[string]interface{}) error {
	streamKey := fmt.Sprintf("stream:%s:%s", tenantId, projectId)
	_, err := d.redisClient.StreamAdd(ctx, streamKey, payload)
	return err
}
