package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/telemetry"
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
)

type WorkerRegistry struct {
	cache   *redis.Cache
	logger  *slog.Logger
	workers sync.Map // workerID -> *WorkerNode (in-memory cache)
	stopCh  chan struct{}
}

type WorkerNode struct {
	ID          string            `json:"id"`
	Address     string            `json:"address"`
	Tags        map[string]string `json:"tags"`
	Capacity    int               `json:"capacity"`
	ActiveLoad  int               `json:"active_load"`
	Status      string            `json:"status"` // healthy|degraded|offline
	LastSeen    time.Time         `json:"last_seen"`
	MissedBeats int               `json:"missed_beats"`
}

// NewWorkerRegistry executes this operation.
func NewWorkerRegistry(cache *redis.Cache) *WorkerRegistry {
	return &WorkerRegistry{
		cache:  cache,
		logger: telemetry.Logger(),
		stopCh: make(chan struct{}),
	}
}

// Register registers a worker node
func (r *WorkerRegistry) Register(ctx context.Context, worker *WorkerNode) error {
	worker.Status = "healthy"
	worker.LastSeen = time.Now()
	worker.MissedBeats = 0

	data, err := json.Marshal(worker)
	if err != nil {
		return errors.Wrap(errors.CodeInternal, "failed to marshal worker", err)
	}

	// Store in Redis Hash
	if err := r.cache.HSet(ctx, workerHashKey, map[string]interface{}{
		worker.ID: string(data),
	}); err != nil {
		return errors.Wrap(errors.CodeStoreWrite, "failed to register worker", err)
	}

	// Set heartbeat key with TTL
	hbKey := fmt.Sprintf(workerHeartbeatKey, worker.ID)
	if err := r.cache.Set(ctx, hbKey, "1", heartbeatTTL); err != nil {
		return errors.Wrap(errors.CodeStoreWrite, "failed to set heartbeat", err)
	}

	// Cache in memory
	r.workers.Store(worker.ID, worker)

	r.logger.InfoContext(ctx, "worker registered",
		"worker_id", worker.ID,
		"address", worker.Address,
		"capacity", worker.Capacity)

	return nil
}

// Heartbeat updates worker heartbeat
func (r *WorkerRegistry) Heartbeat(ctx context.Context, workerID string, activeLoad int) error {
	// Update heartbeat key
	hbKey := fmt.Sprintf(workerHeartbeatKey, workerID)
	if err := r.cache.Set(ctx, hbKey, "1", heartbeatTTL); err != nil {
		return errors.Wrap(errors.CodeStoreWrite, "failed to update heartbeat", err)
	}

	// Update worker in registry
	val, ok := r.workers.Load(workerID)
	if !ok {
		// Worker not in cache, reload from Redis
		return r.syncWorkerFromRedis(ctx, workerID)
	}

	worker := val.(*WorkerNode)
	worker.LastSeen = time.Now()
	worker.ActiveLoad = activeLoad
	worker.MissedBeats = 0
	if worker.Status == "degraded" {
		worker.Status = "healthy"
		r.logger.InfoContext(ctx, "worker recovered", "worker_id", workerID)
	}

	// Update Redis
	data, _ := json.Marshal(worker)
	if err := r.cache.HSet(ctx, workerHashKey, map[string]interface{}{
		workerID: string(data),
	}); err != nil {
		return err
	}

	return nil
}

// AssignWorker assigns a worker based on project and tags
func (r *WorkerRegistry) AssignWorker(ctx context.Context, projectID string, requiredTags map[string]string) *WorkerNode {
	var candidates []*WorkerNode

	// Collect healthy workers matching tags
	r.workers.Range(func(key, value interface{}) bool {
		worker := value.(*WorkerNode)
		if worker.Status != "healthy" {
			return true
		}

		// Check if worker has capacity
		if worker.ActiveLoad >= worker.Capacity {
			return true
		}

		// Check tags match
		if !r.tagsMatch(worker.Tags, requiredTags) {
			return true
		}

		candidates = append(candidates, worker)
		return true
	})

	if len(candidates) == 0 {
		r.logger.WarnContext(ctx, "no available workers", "project_id", projectID)
		return nil
	}

	// Sort by load (least loaded first)
	sort.Slice(candidates, func(i, j int) bool {
		loadI := float64(candidates[i].ActiveLoad) / float64(candidates[i].Capacity)
		loadJ := float64(candidates[j].ActiveLoad) / float64(candidates[j].Capacity)
		return loadI < loadJ
	})

	selected := candidates[0]
	r.logger.InfoContext(ctx, "worker assigned",
		"worker_id", selected.ID,
		"project_id", projectID,
		"load", selected.ActiveLoad,
		"capacity", selected.Capacity)

	return selected
}

// ListAvailable returns all available workers
func (r *WorkerRegistry) ListAvailable(ctx context.Context) ([]*WorkerNode, error) {
	var workers []*WorkerNode
	r.workers.Range(func(key, value interface{}) bool {
		worker := value.(*WorkerNode)
		if worker.Status == "healthy" {
			workers = append(workers, worker)
		}
		return true
	})
	return workers, nil
}

// HealthSummary returns the current in-memory worker health counts used by the orchestrator health endpoint.
func (r *WorkerRegistry) HealthSummary() workerRegistryHealthSummary {
	summary := workerRegistryHealthSummary{} // Allocate the mutable summary that will accumulate worker counts across the registry snapshot.
	r.workers.Range(func(key, value interface{}) bool {
		worker := value.(*WorkerNode) // Read one cached worker entry so this snapshot can classify its current health state.
		summary.Total++               // Count every cached worker regardless of health state so callers can see total fleet size.
		switch worker.Status {        // Bucket the worker by its current health state so health output can report healthy, degraded, and offline capacity separately.
		case "healthy":
			summary.Healthy++ // Count workers that are currently considered healthy and assignable.
		case "degraded":
			summary.Degraded++ // Count workers that are still known but have missed enough heartbeats to lose full trust.
		case "offline":
			summary.Offline++ // Count workers that have missed enough heartbeats to be treated as offline.
		default:
			summary.Unknown++ // Count any unexpected state explicitly so health output reveals registry-state drift instead of hiding it.
		}
		return true // Continue iterating until every cached worker has been included in the summary snapshot.
	})

	return summary // Return the completed registry snapshot for health-check reporting.
}

// StartMonitor starts monitoring worker heartbeats
func (r *WorkerRegistry) StartMonitor(ctx context.Context) {
	go r.monitorLoop(ctx)
	r.logger.Info("worker registry monitor started")
}

// Stop stops the monitor
func (r *WorkerRegistry) Stop() {
	close(r.stopCh)
	r.logger.Info("worker registry stopped")
}

// monitorLoop periodically checks worker heartbeats
func (r *WorkerRegistry) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.checkHeartbeats(ctx)
		}
	}
}

// checkHeartbeats checks all workers for missed heartbeats
func (r *WorkerRegistry) checkHeartbeats(ctx context.Context) {
	now := time.Now()

	r.workers.Range(func(key, value interface{}) bool {
		workerID := key.(string)
		worker := value.(*WorkerNode)

		// Check if heartbeat key exists
		hbKey := fmt.Sprintf(workerHeartbeatKey, workerID)
		_, err := r.cache.Get(ctx, hbKey)

		if err != nil {
			// Heartbeat missed
			worker.MissedBeats++

			if worker.MissedBeats >= degradeAfterMisses && worker.Status == "healthy" {
				worker.Status = "degraded"
				r.logger.WarnContext(ctx, "worker degraded",
					"worker_id", workerID,
					"missed_beats", worker.MissedBeats)
			}

			if worker.MissedBeats >= heartbeatMissMax {
				worker.Status = "offline"
				r.logger.ErrorContext(ctx, "worker offline",
					"worker_id", workerID,
					"missed_beats", worker.MissedBeats)
			}

			// Remove if offline too long
			if worker.Status == "offline" && now.Sub(worker.LastSeen) > removeAfterSeconds*time.Second {
				r.removeWorker(ctx, workerID)
			}
		}

		return true
	})
}

// removeWorker removes a worker from registry
func (r *WorkerRegistry) removeWorker(ctx context.Context, workerID string) {
	r.workers.Delete(workerID)
	if err := r.cache.HDel(ctx, workerHashKey, workerID); err != nil {
		r.logger.ErrorContext(ctx, "failed to remove worker from redis",
			"worker_id", workerID,
			"error", err)
	}
	r.logger.InfoContext(ctx, "worker removed", "worker_id", workerID)
}

// syncWorkerFromRedis reloads worker from Redis
func (r *WorkerRegistry) syncWorkerFromRedis(ctx context.Context, workerID string) error {
	data, err := r.cache.HGetAll(ctx, workerHashKey)
	if err != nil {
		return err
	}

	workerData, ok := data[workerID]
	if !ok {
		return errors.New(errors.CodeWorkerNotFound, "worker not found in registry")
	}

	var worker WorkerNode
	if err := json.Unmarshal([]byte(workerData), &worker); err != nil {
		return errors.Wrap(errors.CodeInternal, "failed to unmarshal worker", err)
	}

	r.workers.Store(workerID, &worker)
	return nil
}

// tagsMatch checks if worker tags match required tags
func (r *WorkerRegistry) tagsMatch(workerTags, requiredTags map[string]string) bool {
	for k, v := range requiredTags {
		if workerTags[k] != v {
			return false
		}
	}
	return true
}

// GetWorker returns a worker by ID
func (r *WorkerRegistry) GetWorker(workerID string) *WorkerNode {
	val, ok := r.workers.Load(workerID)
	if !ok {
		return nil
	}
	return val.(*WorkerNode)
}
