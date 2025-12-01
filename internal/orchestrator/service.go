package orchestrator

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/storage/s3"
	"mcp_for_appium/internal/telemetry"

	"github.com/google/uuid"
)

type Service struct {
	cfg        config.OrchestratorConfig
	dao        *postgres.DAO
	cache      *redis.Cache
	s3         *s3.Client
	dispatcher *Dispatcher
	registry   *WorkerRegistry
	publisher  *EventsPublisher
	logger     *slog.Logger
}

func NewService(cfg config.OrchestratorConfig, dao *postgres.DAO, cache *redis.Cache, s3 *s3.Client) *Service {
	registry := NewWorkerRegistry(cache)
	dispatcher := NewDispatcher(cache, registry)
	publisher := NewEventsPublisher(dao, cache)

	svc := &Service{
		cfg:        cfg,
		dao:        dao,
		cache:      cache,
		s3:         s3,
		dispatcher: dispatcher,
		registry:   registry,
		publisher:  publisher,
		logger:     telemetry.Logger(),
	}

	return svc
}

// Start starts the orchestrator service
func (s *Service) Start(ctx context.Context) error {
	s.logger.Info("starting orchestrator service")

	// Start dispatcher loop
	s.dispatcher.StartDispatchLoop(ctx)

	// Start worker registry monitor
	s.registry.StartMonitor(ctx)

	s.logger.Info("orchestrator service started")
	return nil
}

// Stop stops the orchestrator service
func (s *Service) Stop() {
	s.logger.Info("stopping orchestrator service")
	s.dispatcher.Stop()
	s.registry.Stop()
	s.logger.Info("orchestrator service stopped")
}

func (s *Service) StartSession(ctx context.Context, projectID string, caps map[string]interface{}) (*postgres.Session, error) {
	// 1. Create Session in DB
	sessID := uuid.New().String()
	capsJSON, _ := json.Marshal(caps)

	sess := &postgres.Session{
		ID:           sessID,
		ProjectID:    projectID,
		Status:       "created",
		Capabilities: capsJSON,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	if err := s.dao.CreateSession(ctx, sess); err != nil {
		return nil, errors.Wrap(errors.CodeInternal, "failed to create session", err)
	}

	return sess, nil
}

func (s *Service) ExecutePlan(ctx context.Context, sessionID string, plan json.RawMessage) (string, error) {
	// 1. Validate Session
	sess, err := s.dao.GetSession(ctx, sessionID)
	if err != nil {
		return "", err
	}
	if sess.Status == "ended" {
		return "", errors.New(errors.CodeSessionDead, "session ended")
	}

	// 2. Create Trace
	traceID := uuid.New().String()
	trace := &postgres.Trace{
		ID:        traceID,
		SessionID: sessionID,
		ProjectID: sess.ProjectID,
		Status:    "pending",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := s.dao.CreateTrace(ctx, trace); err != nil {
		return "", errors.Wrap(errors.CodeInternal, "failed to create trace", err)
	}

	// 3. Enqueue Plan
	if err := s.dispatcher.EnqueuePlan(ctx, sess.ProjectID, traceID, plan); err != nil {
		return "", errors.Wrap(errors.CodeInternal, "failed to enqueue plan", err)
	}

	return traceID, nil
}

func (s *Service) GetEvents(ctx context.Context, traceID string, sinceSeq int64) ([]*postgres.PlanEvent, error) {
	return s.dao.ListEvents(ctx, traceID, sinceSeq, 100)
}

func (s *Service) GetArtifacts(ctx context.Context, traceID string) ([]*postgres.Artifact, error) {
	return s.dao.ListArtifacts(ctx, traceID)
}
