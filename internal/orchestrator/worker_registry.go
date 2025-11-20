package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/mcp/mobile-worker/internal/storage/redis"
)

type WorkerRegistry struct {
	redisClient *redis.Client
}

func NewWorkerRegistry(redisClient *redis.Client) *WorkerRegistry {
	return &WorkerRegistry{redisClient: redisClient}
}

func (r *WorkerRegistry) Register(ctx context.Context, workerId string, info map[string]interface{}) error {
	key := fmt.Sprintf("worker:%s", workerId)
	// Use a pipeline or separate calls to set info and expiration
	// For simplicity, we just set the hash here. In production, we'd want a heartbeat mechanism to keep it alive.
	for k, v := range info {
		if err := r.redisClient.HashSet(ctx, key, k, v); err != nil {
			return err
		}
	}
	// Set expiration for heartbeat
	_, err := r.redisClient.SetNX(ctx, fmt.Sprintf("worker:%s:heartbeat", workerId), time.Now().Unix(), 10*time.Second)
	return err
}
