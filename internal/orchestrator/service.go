package orchestrator

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/storage/s3"
	"mcp_for_appium/internal/telemetry"
	"mcp_for_appium/internal/worker"
	"mcp_for_appium/internal/worker/appium"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

type Service struct {
	cfg        config.OrchestratorConfig
	dao        *postgres.DAO
	cache      *redis.Cache
	s3         *s3.Client
	dispatcher *Dispatcher
	registry   *WorkerRegistry
	publisher  *EventsPublisher
	appiumURL  string
	appiumMu   sync.Mutex
	appiumMap  map[string]*appium.Client
	planMu     sync.Mutex
	planCancel map[string]context.CancelFunc
	snapshotMu sync.Mutex
	snapshots  map[string]*snapshotCache
	logger     *slog.Logger
}

func NewService(cfg config.OrchestratorConfig, workerCfg config.WorkerConfig, dao *postgres.DAO, cache *redis.Cache, s3 *s3.Client) *Service {
	registry := NewWorkerRegistry(cache)
	publisher := NewEventsPublisher(dao, cache)

	svc := &Service{
		cfg:        cfg,
		dao:        dao,
		cache:      cache,
		s3:         s3,
		registry:   registry,
		publisher:  publisher,
		appiumURL:  workerCfg.AppiumURL,
		appiumMap:  make(map[string]*appium.Client),
		planCancel: make(map[string]context.CancelFunc),
		snapshots:  make(map[string]*snapshotCache),
		logger:     telemetry.Logger(),
	}

	dispatcher := NewDispatcher(cache, registry, svc)
	svc.dispatcher = dispatcher

	return svc
}

// Registry returns the worker registry (used to wire up the gRPC server).
func (s *Service) Registry() *WorkerRegistry {
	return s.registry
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
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "StartSession")
	defer span.End()

	if s.appiumURL == "" {
		return nil, errors.New(errors.CodeConfigMissing, "appium_url is empty")
	}

	app := appium.NewClient(s.appiumURL)
	if _, err := app.StartSession(ctx, caps); err != nil {
		return nil, errors.Wrap(errors.CodeInternal, "failed to start appium session", err)
	}

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
		_ = app.DeleteSession(ctx)
		return nil, errors.Wrap(errors.CodeInternal, "failed to create session", err)
	}

	s.appiumMu.Lock()
	s.appiumMap[sessID] = app
	s.appiumMu.Unlock()

	return sess, nil
}

func (s *Service) ExecutePlan(ctx context.Context, sessionID string, plan json.RawMessage) (string, error) {
	return s.ExecutePlanWithTrace(ctx, sessionID, "", plan)
}

func (s *Service) ExecutePlanWithTrace(ctx context.Context, sessionID string, traceID string, plan json.RawMessage) (string, error) {
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "ExecutePlan")
	defer span.End()

	// 1. Validate Session
	sess, err := s.dao.GetSession(ctx, sessionID)
	if err != nil {
		return "", err
	}
	if sess.Status == "ended" {
		return "", errors.New(errors.CodeSessionDead, "session ended")
	}

	// 2. Create Trace
	if traceID == "" {
		traceID = uuid.New().String()
	}
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
	if err := s.dispatcher.EnqueuePlan(ctx, sess.ProjectID, sessionID, traceID, plan); err != nil {
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

func (s *Service) GetSession(ctx context.Context, sessionID string) (*postgres.Session, error) {
	return s.dao.GetSession(ctx, sessionID)
}

func (s *Service) EndSession(ctx context.Context, sessionID string) error {
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "EndSession")
	defer span.End()

	sess, err := s.dao.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if sess.Status == "ended" {
		return nil
	}

	s.appiumMu.Lock()
	app := s.appiumMap[sessionID]
	delete(s.appiumMap, sessionID)
	s.appiumMu.Unlock()

	if app != nil {
		_ = app.DeleteSession(ctx)
	}

	if err := s.dao.EndSession(ctx, sessionID, time.Now()); err != nil {
		return errors.Wrap(errors.CodeStoreWrite, "failed to end session", err)
	}

	return nil
}

func (s *Service) CancelPlan(ctx context.Context, traceID string) error {
	s.planMu.Lock()
	cancel := s.planCancel[traceID]
	delete(s.planCancel, traceID)
	s.planMu.Unlock()

	if cancel != nil {
		cancel()
	}

	_ = s.dispatcher.CancelPlan(ctx, traceID)
	if err := s.dao.UpdateTraceStatus(ctx, traceID, "cancelled"); err != nil {
		return errors.Wrap(errors.CodeStoreWrite, "failed to update trace status", err)
	}

	return nil
}

func (s *Service) GetTrace(ctx context.Context, traceID string) (*postgres.Trace, []*postgres.PlanEvent, error) {
	trace, err := s.dao.GetTrace(ctx, traceID)
	if err != nil {
		return nil, nil, err
	}

	events, err := s.dao.ListEvents(ctx, traceID, 0, 1000)
	if err != nil {
		return nil, nil, errors.Wrap(errors.CodeStoreRead, "failed to list events", err)
	}

	return trace, events, nil
}

func (s *Service) GetSemanticSnapshot(ctx context.Context, sessionID string, sinceRev string) (map[string]interface{}, error) {
	app, err := s.getAppiumClient(sessionID)
	if err != nil {
		return nil, err
	}

	s.snapshotMu.Lock()
	entry := s.snapshots[sessionID]
	if entry != nil && time.Since(entry.cachedAt) <= s.cfg.SnapshotTTL && sinceRev != "" && sinceRev == entry.rev {
		s.snapshotMu.Unlock()
		return map[string]interface{}{
			"changed":     false,
			"rev":         entry.rev,
			"snapshotRef": entry.ref,
		}, nil
	}
	s.snapshotMu.Unlock()

	source, err := app.PageSource(ctx)
	if err != nil {
		return nil, errors.Wrap(errors.CodeInternal, "failed to get page source", err)
	}

	rev := uuid.New().String()
	ref := "snapshot:" + rev

	s.snapshotMu.Lock()
	s.snapshots[sessionID] = &snapshotCache{
		rev:      rev,
		ref:      ref,
		cachedAt: time.Now(),
		source:   source,
	}
	s.snapshotMu.Unlock()

	return map[string]interface{}{
		"changed":     true,
		"rev":         rev,
		"snapshotRef": ref,
		"tree": map[string]interface{}{
			"source": source,
		},
	}, nil
}

func (s *Service) TakeScreenshot(ctx context.Context, sessionID string, traceID string, includeThumb bool) (map[string]interface{}, error) {
	if traceID == "" {
		return nil, errors.New(errors.CodePlanInvalid, "traceId is required for takeScreenshot")
	}

	app, err := s.getAppiumClient(sessionID)
	if err != nil {
		return nil, err
	}

	data, err := app.Screenshot(ctx)
	if err != nil {
		return nil, errors.Wrap(errors.CodeInternal, "failed to take screenshot", err)
	}

	fullRef, err := s.storeArtifact(ctx, traceID, "screenshot_full.png", "image/png", data)
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"full": fullRef,
	}
	if includeThumb {
		thumbRef, err := s.storeArtifact(ctx, traceID, "screenshot_thumb.png", "image/png", data)
		if err != nil {
			return nil, err
		}
		result["thumb"] = thumbRef
	}

	return result, nil
}

func (s *Service) HealthCheck(ctx context.Context) map[string]interface{} {
	status := "healthy"
	issues := []string{}
	checks := map[string]string{}
	healthValue := 2.0

	if err := s.dao.Ping(ctx); err != nil {
		status = "degraded"
		healthValue = 1.0
		checks["database"] = "error"
		issues = append(issues, "postgres")
	} else {
		checks["database"] = "ok"
	}

	if err := s.cache.Ping(ctx); err != nil {
		status = "degraded"
		healthValue = 1.0
		checks["redis"] = "error"
		issues = append(issues, "redis")
	} else {
		checks["redis"] = "ok"
	}

	telemetry.HealthStatus.WithLabelValues("orchestrator").Set(healthValue)

	return map[string]interface{}{
		"status": status,
		"issues": issues,
		"checks": checks,
	}
}

func (s *Service) getAppiumClient(sessionID string) (*appium.Client, error) {
	s.appiumMu.Lock()
	app := s.appiumMap[sessionID]
	s.appiumMu.Unlock()
	if app == nil {
		return nil, errors.New(errors.CodeSessionNotFound, "session not found")
	}
	return app, nil
}

type snapshotCache struct {
	rev      string
	ref      string
	source   string
	cachedAt time.Time
}

func (s *Service) storeArtifact(ctx context.Context, traceID string, key string, contentType string, data []byte) (map[string]interface{}, error) {
	artifactID := uuid.New().String()
	objectKey := traceID + "/" + artifactID + "/" + key

	if err := s.s3.PutObject(ctx, objectKey, contentType, data); err != nil {
		return nil, errors.Wrap(errors.CodeStorageArtifact, "failed to upload artifact", err)
	}

	if err := s.dao.InsertArtifact(ctx, &postgres.Artifact{
		ID:        artifactID,
		TraceID:   traceID,
		Key:       objectKey,
		Type:      contentType,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
		Metadata:  []byte(`{}`),
	}); err != nil {
		return nil, errors.Wrap(errors.CodeStoreWrite, "failed to store artifact metadata", err)
	}

	url, err := s.s3.PresignGet(ctx, objectKey, 15*time.Minute)
	if err != nil {
		return nil, errors.Wrap(errors.CodeStorageArtifact, "failed to presign artifact", err)
	}

	return map[string]interface{}{
		"key":  objectKey,
		"url":  url,
		"size": len(data),
	}, nil
}

func (s *Service) ExecuteDispatchedPlan(ctx context.Context, traceID string, sessionID string, plan json.RawMessage) error {
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "ExecuteDispatchedPlan")
	defer span.End()

	runCtx, cancel := context.WithTimeout(ctx, s.cfg.PlanTimeout)
	s.planMu.Lock()
	s.planCancel[traceID] = cancel
	s.planMu.Unlock()
	defer func() {
		cancel()
		s.planMu.Lock()
		delete(s.planCancel, traceID)
		s.planMu.Unlock()
	}()

	if err := s.dao.UpdateTraceStatus(runCtx, traceID, "running"); err != nil {
		return errors.Wrap(errors.CodeStoreWrite, "failed to update trace status", err)
	}

	steps, err := worker.ParsePlan(plan)
	if err != nil {
		_ = s.dao.UpdateTraceStatus(runCtx, traceID, "failed")
		return err
	}

	app, err := s.getAppiumClient(sessionID)
	if err != nil {
		_ = s.dao.UpdateTraceStatus(runCtx, traceID, "failed")
		return err
	}

	seq := int64(0)
	exec := worker.NewExecutor(app, s.cfg.StepTimeout, s.cfg.AutoWaitMax, func(ev worker.StepEvent) {
		seq++
		payload, _ := json.Marshal(map[string]interface{}{
			"message":      ev.Message,
			"metrics":      ev.Metrics,
			"artifactRefs": ev.ArtifactRefs,
			"phase":        ev.Phase,
		})
		_ = s.publisher.Publish(runCtx, &postgres.PlanEvent{
			TraceID:   traceID,
			Seq:       seq,
			StepIndex: ev.StepIndex,
			Status:    ev.Status,
			Payload:   payload,
			CreatedAt: time.Now(),
		})
	})

	start := time.Now()
	err = exec.Execute(runCtx, steps)
	elapsed := time.Since(start).Seconds()
	if err != nil {
		if code, ok := errors.CodeOf(err); ok {
			telemetry.ErrorCodeTotal.WithLabelValues(string(code), "orchestrator").Inc()
		}
		if errors.IsCode(err, errors.CodeSessionDead) || errors.IsCode(err, errors.CodeSessionBroken) {
			_ = s.dao.UpdateTraceStatus(runCtx, traceID, "failed")
			telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
			return err
		}
		if runCtx.Err() == context.Canceled || runCtx.Err() == context.DeadlineExceeded {
			_ = s.dao.UpdateTraceStatus(runCtx, traceID, "cancelled")
			telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
			return err
		}
		telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
		_ = s.dao.UpdateTraceStatus(runCtx, traceID, "failed")
		return err
	}

	telemetry.ExecuteLatency.WithLabelValues("", "", "passed").Observe(elapsed)
	return s.dao.UpdateTraceStatus(runCtx, traceID, "completed")
}
