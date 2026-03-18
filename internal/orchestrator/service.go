package orchestrator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/devicefarm"
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

const (
	appiumSessionKeyPrefix     = "session:appium:"
	appiumSessionKeyTTL        = 24 * time.Hour
	deviceFarmCacheTTL         = 24 * time.Hour
	deviceFarmUploadMetaTTL    = 2 * time.Hour
	deviceFarmLastAppPrefix    = "devicefarm:last:app:"
	deviceFarmLastTestPrefix   = "devicefarm:last:testpkg:"
	deviceFarmLastPoolPrefix   = "devicefarm:last:pool:"
	deviceFarmUploadMetaPrefix = "devicefarm:uploadmeta:"
)

type deviceFarmUploadMeta struct {
	ProjectARN string `json:"projectArn"`
	Type       string `json:"type"`
}

type Service struct {
	cfg          config.OrchestratorConfig
	dao          *postgres.DAO
	cache        *redis.Cache
	s3           *s3.Client
	dispatcher   *Dispatcher
	registry     *WorkerRegistry
	publisher    *EventsPublisher
	appiumURL    string
	appiumMu     sync.Mutex
	appiumMap    map[string]*appium.Client
	planMu       sync.Mutex
	planCancel   map[string]context.CancelFunc
	snapshotMu   sync.Mutex
	snapshots    map[string]*snapshotCache
	logger       *slog.Logger
	execMode     string
	deviceFarm   *devicefarm.Client
	dfMode       string
	dfProjectARN string
}

// NewService executes this operation.
func NewService(cfg config.OrchestratorConfig, workerCfg config.WorkerConfig, awsCfg config.AWSConfig, dfCfg config.DeviceFarmConfig, dao *postgres.DAO, cache *redis.Cache, s3 *s3.Client) *Service {
	mode := normalizeExecutionMode(cfg.ExecutionMode)
	cfg.ExecutionMode = mode
	registry := NewWorkerRegistry(cache)
	publisher := NewEventsPublisher(dao, cache)

	svc := &Service{
		cfg:          cfg,
		dao:          dao,
		cache:        cache,
		s3:           s3,
		registry:     registry,
		publisher:    publisher,
		appiumURL:    workerCfg.AppiumURL,
		appiumMap:    make(map[string]*appium.Client),
		planCancel:   make(map[string]context.CancelFunc),
		snapshots:    make(map[string]*snapshotCache),
		logger:       telemetry.Logger(),
		execMode:     mode,
		dfMode:       strings.ToLower(strings.TrimSpace(dfCfg.Mode)),
		dfProjectARN: strings.TrimSpace(dfCfg.ProjectARN),
	}

	// Initialize Device Farm SDK client when run_api mode is explicitly enabled.
	if svc.dfMode == "run_api" {
		dfClient, err := devicefarm.NewClient(context.Background(), awsCfg, dfCfg)
		if err != nil {
			svc.logger.Warn("failed to init device farm client", "error", err)
		} else {
			svc.deviceFarm = dfClient
		}
	}

	var executor PlanExecutor
	if mode == ExecutionModeMonolith {
		executor = svc
	}
	dispatcher := NewDispatcher(cache, registry, executor, cfg.PlanTimeout) // Pass the configured plan timeout into dispatcher so distributed in-flight traces can be closed when workers never report a result.
	dispatcher.finalizer = svc                                              // Wire the service's durable terminalization helper into dispatcher-owned terminal paths before the service starts processing queue messages.
	svc.dispatcher = dispatcher

	return svc
}

// ExecutionMode executes this operation.
func (s *Service) ExecutionMode() string {
	return s.execMode
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

// StartSession executes this operation.
func (s *Service) StartSession(ctx context.Context, projectID string, caps map[string]interface{}) (*postgres.Session, error) {
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "StartSession")
	defer span.End()
	startedAt := time.Now()                                                                                       // Capture method start for duration logging.
	s.logger.InfoContext(ctx, "orchestrator StartSession begin", "project_id", projectID, "caps_keys", len(caps)) // Log method entry with high-signal inputs.

	if s.appiumURL == "" {
		s.logger.ErrorContext(ctx, "orchestrator StartSession failed", "project_id", projectID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", "appium_url is empty") // Log configuration failures explicitly.
		return nil, errors.New(errors.CodeConfigMissing, "appium_url is empty")
	}

	app := appium.NewClient(s.appiumURL)
	appiumSessionID, err := app.StartSession(ctx, caps)
	if err != nil {
		s.logger.ErrorContext(ctx, "orchestrator StartSession failed", "project_id", projectID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log upstream Appium failures with method latency.
		return nil, errors.WrapPreservingCode("failed to start appium session", err)                                                                               // Preserve the downstream Appium timeout/not-found code so gateway transports keep the correct external status mapping.
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
		s.logger.ErrorContext(ctx, "orchestrator StartSession failed", "project_id", projectID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log persistence failure before returning.
		return nil, errors.Wrap(errors.CodeInternal, "failed to create session", err)
	}

	if err := s.cache.Set(ctx, appiumSessionKeyPrefix+sessID, appiumSessionID, appiumSessionKeyTTL); err != nil {
		s.logger.WarnContext(ctx, "failed to persist appium session mapping",
			"session_id", sessID,
			"appium_session_id", appiumSessionID,
			"error", err)
	}

	s.appiumMu.Lock()
	s.appiumMap[sessID] = app
	s.appiumMu.Unlock()

	s.logger.InfoContext(ctx, "orchestrator StartSession done", "project_id", projectID, "session_id", sessID, "appium_session_id", appiumSessionID, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful method completion and key identifiers.
	return sess, nil
}

// ExecutePlan executes this operation.
func (s *Service) ExecutePlan(ctx context.Context, sessionID string, plan json.RawMessage) (string, error) {
	return s.ExecutePlanWithTrace(ctx, sessionID, "", plan)
}

// ExecutePlanWithTrace executes this operation.
func (s *Service) ExecutePlanWithTrace(ctx context.Context, sessionID string, traceID string, plan json.RawMessage) (string, error) {
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "ExecutePlan")
	defer span.End()
	startedAt := time.Now()                                                                                                            // Capture method start for duration logging.
	s.logger.InfoContext(ctx, "orchestrator ExecutePlan begin", "session_id", sessionID, "trace_id", traceID, "plan_bytes", len(plan)) // Log entry to execution scheduling path.

	// 1. Validate Session
	sess, err := s.dao.GetSession(ctx, sessionID)
	if err != nil {
		s.logger.ErrorContext(ctx, "orchestrator ExecutePlan failed", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log session lookup failure.
		return "", err
	}
	if sess.Status == "ended" {
		s.logger.ErrorContext(ctx, "orchestrator ExecutePlan failed", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", "session ended") // Log invalid session state before returning.
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
		s.logger.ErrorContext(ctx, "orchestrator ExecutePlan failed", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log trace creation failures.
		return "", errors.Wrap(errors.CodeInternal, "failed to create trace", err)
	}

	// 3. Enqueue Plan
	if err := s.dispatcher.EnqueuePlan(ctx, sess.ProjectID, sessionID, traceID, plan); err != nil {
		s.logger.ErrorContext(ctx, "orchestrator ExecutePlan failed", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log enqueue failures for queue-level troubleshooting.
		return "", errors.Wrap(errors.CodeInternal, "failed to enqueue plan", err)
	}

	s.logger.InfoContext(ctx, "orchestrator ExecutePlan done", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful method completion.
	return traceID, nil
}

// GetEvents executes this operation.
func (s *Service) GetEvents(ctx context.Context, traceID string, sinceSeq int64) ([]*postgres.PlanEvent, error) {
	return s.dao.ListEvents(ctx, traceID, sinceSeq, 100)
}

// GetArtifacts executes this operation.
func (s *Service) GetArtifacts(ctx context.Context, traceID string) ([]*postgres.Artifact, error) {
	return s.dao.ListArtifacts(ctx, traceID)
}

// GetSession executes this operation.
func (s *Service) GetSession(ctx context.Context, sessionID string) (*postgres.Session, error) {
	return s.dao.GetSession(ctx, sessionID)
}

// EndSession marks the persisted session as ended exactly once, then releases the cached Appium session and Redis mapping.
func (s *Service) EndSession(ctx context.Context, sessionID string) error {
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "EndSession")
	defer span.End()
	startedAt := time.Now()                                                             // Capture method start for duration logging.
	s.logger.InfoContext(ctx, "orchestrator EndSession begin", "session_id", sessionID) // Log session shutdown method entry.

	ended, err := s.markSessionEnded(ctx, sessionID, time.Now()) // Persist the terminal session state first so concurrent callers observe idempotent completion from the database record.
	if err != nil {                                              // Stop before touching the in-memory Appium session map when the persisted session transition fails.
		s.logger.ErrorContext(ctx, "orchestrator EndSession failed", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log lookup failures before returning.
		return err
	}
	if !ended { // Treat repeated end-session requests as successful no-ops once another caller has already completed the transition.
		s.logger.InfoContext(ctx, "orchestrator EndSession done", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds(), "already_ended", true) // Log idempotent completion path.
		return nil
	}

	s.appiumMu.Lock()
	app := s.appiumMap[sessionID]
	delete(s.appiumMap, sessionID)
	s.appiumMu.Unlock()

	if app != nil {
		_ = app.DeleteSession(ctx)
	}
	if err := s.cache.Del(ctx, appiumSessionKeyPrefix+sessionID); err != nil {
		s.logger.WarnContext(ctx, "failed to delete appium session mapping",
			"session_id", sessionID,
			"error", err)
	}

	s.logger.InfoContext(ctx, "orchestrator EndSession done", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful session termination.
	return nil
}

// CancelPlan cancels local execution, propagates cancellation to distributed workers when needed, and marks active traces as cancelled without clobbering terminal states.
func (s *Service) CancelPlan(ctx context.Context, traceID string) error {
	s.planMu.Lock()
	cancel := s.planCancel[traceID]
	delete(s.planCancel, traceID)
	s.planMu.Unlock()

	if cancel != nil {
		cancel()
	}

	if s.execMode == ExecutionModeDistributed {
		cancelled, err := s.dispatcher.CancelDispatchedPlan(ctx, traceID)
		if err != nil {
			s.logger.WarnContext(ctx, "failed to propagate cancel to worker",
				"trace_id", traceID,
				"error", err)
		} else if cancelled {
			s.logger.InfoContext(ctx, "cancel propagated to worker", "trace_id", traceID)
		}
	}

	_ = s.dispatcher.CancelPlan(ctx, traceID)
	cancelled, err := s.finalizeTrace(ctx, traceID, []string{"pending", "running"}, "cancelled", "trace cancelled", nil, 0) // Persist the cancelled terminal state together with one final trace event so replay consumers see a durable terminal marker.
	if err != nil {                                                                                                         // Stop immediately when the transactional terminalization helper fails.
		return err // Preserve the wrapped storage error produced by the optimistic transition helper.
	}
	if !cancelled { // Keep cancel idempotent when another worker or caller has already moved the trace to a terminal state.
		s.logger.InfoContext(ctx, "cancel ignored because trace is already terminal", "trace_id", traceID) // Emit a low-noise informational log so operators can distinguish ignored cancels from failures.
	}

	return nil
}

// GetTrace executes this operation.
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

// GetSemanticSnapshot executes this operation.
func (s *Service) GetSemanticSnapshot(ctx context.Context, sessionID string, sinceRev string) (map[string]interface{}, error) {
	app, err := s.getAppiumClient(ctx, sessionID)
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

// TakeScreenshot executes this operation.
func (s *Service) TakeScreenshot(ctx context.Context, sessionID string, traceID string, includeThumb bool) (map[string]interface{}, error) {
	if traceID == "" {
		return nil, errors.New(errors.CodePlanInvalid, "traceId is required for takeScreenshot")
	}

	app, err := s.getAppiumClient(ctx, sessionID)
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

// getAppiumClient executes this operation.
func (s *Service) getAppiumClient(ctx context.Context, sessionID string) (*appium.Client, error) {
	s.appiumMu.Lock()
	app := s.appiumMap[sessionID]
	s.appiumMu.Unlock()
	if app == nil {
		appiumSessionID, err := s.cache.Get(ctx, appiumSessionKeyPrefix+sessionID)
		if err != nil {
			if err.Error() == "redis: nil" {
				return nil, errors.New(errors.CodeSessionNotFound, "session not found")
			}
			return nil, errors.Wrap(errors.CodeStoreRead, "failed to load appium session mapping", err)
		}
		if appiumSessionID == "" {
			return nil, errors.New(errors.CodeSessionNotFound, "session not found")
		}

		restored := appium.NewClient(s.appiumURL)
		restored.AttachSession(appiumSessionID)

		s.appiumMu.Lock()
		if existing := s.appiumMap[sessionID]; existing != nil {
			app = existing
		} else {
			s.appiumMap[sessionID] = restored
			app = restored
		}
		s.appiumMu.Unlock()
	}
	return app, nil
}

type snapshotCache struct {
	rev      string
	ref      string
	source   string
	cachedAt time.Time
}

// storeArtifact executes this operation.
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

// ExecuteDispatchedPlan runs a queued plan for an existing session and drives the trace through guarded lifecycle transitions from pending to running to a terminal state.
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

	transitioned, err := s.transitionTraceStatus(runCtx, traceID, []string{"pending"}, "running") // Move the trace into running only if no concurrent cancel or terminal transition has already won.
	if err != nil {                                                                               // Surface storage-layer compare-and-swap failures before plan parsing begins.
		return err // Preserve the wrapped storage error from the optimistic transition helper.
	}
	if !transitioned { // Reject execution when the trace is no longer in the expected pending state.
		return errors.New(errors.CodeStateConflict, "trace is not pending") // Return a stable state-conflict error instead of overwriting another terminal state.
	}

	steps, err := worker.ParsePlan(plan)
	if err != nil {
		_, _ = s.finalizeTrace(runCtx, traceID, []string{"running"}, "failed", "trace failed before execution started", err, 0) // Persist the failed terminal state and matching final event before returning the plan-parse error.
		return err
	}

	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		_, _ = s.finalizeTrace(runCtx, traceID, []string{"running"}, "failed", "trace failed before acquiring the session client", err, 0) // Persist the failed terminal state and final event without overwriting a concurrent cancellation result.
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
			_, _ = s.finalizeTrace(runCtx, traceID, []string{"running"}, "failed", "trace failed because the session became unusable", err, 0) // Persist the failed terminal state and final event without clobbering a concurrent cancellation or other terminal transition.
			telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
			return err
		}
		if runCtx.Err() == context.Canceled || runCtx.Err() == context.DeadlineExceeded {
			_, _ = s.finalizeTrace(runCtx, traceID, []string{"running"}, "cancelled", "trace cancelled during execution", err, 0) // Persist cancellation as the winning terminal state together with one final trace event.
			telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
			return err
		}
		telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
		_, _ = s.finalizeTrace(runCtx, traceID, []string{"running"}, "failed", "trace failed during execution", err, 0) // Persist the failed terminal state and final event without clobbering a concurrent terminal state update.
		return err
	}

	telemetry.ExecuteLatency.WithLabelValues("", "", "passed").Observe(elapsed)
	_, err = s.finalizeTrace(runCtx, traceID, []string{"running"}, "completed", "trace completed", nil, 0) // Persist the completed terminal state together with one final trace event when this executor still owns the running state.
	return err                                                                                             // Return any storage-layer failure while treating a lost race to another terminal state as a successful no-op.
}

// Interactive element operations

// FindElement executes this operation.
func (s *Service) FindElement(ctx context.Context, sessionID, strategy, selector string) (string, error) {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return "", err
	}
	return app.FindElement(ctx, strategy, selector)
}

// ClickElement executes this operation.
func (s *Service) ClickElement(ctx context.Context, sessionID, elementID string) error {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return err
	}
	return app.Click(ctx, elementID)
}

// SendKeysToElement executes this operation.
func (s *Service) SendKeysToElement(ctx context.Context, sessionID, elementID, text string) error {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return err
	}
	return app.SendKeys(ctx, elementID, text)
}

// ClearElement executes this operation.
func (s *Service) ClearElement(ctx context.Context, sessionID, elementID string) error {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return err
	}
	return app.Clear(ctx, elementID)
}

// GetElementText executes this operation.
func (s *Service) GetElementText(ctx context.Context, sessionID, elementID string) (string, error) {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return "", err
	}
	return app.GetText(ctx, elementID)
}

// GetElementAttribute executes this operation.
func (s *Service) GetElementAttribute(ctx context.Context, sessionID, elementID, attribute string) (string, error) {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return "", err
	}
	return app.GetAttribute(ctx, elementID, attribute)
}

// IsElementDisplayed executes this operation.
func (s *Service) IsElementDisplayed(ctx context.Context, sessionID, elementID string) (bool, error) {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return false, err
	}
	return app.IsDisplayed(ctx, elementID)
}

// Tap executes this operation.
func (s *Service) Tap(ctx context.Context, sessionID string, x, y int) error {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return err
	}
	return app.Tap(ctx, x, y)
}

// Swipe executes this operation.
func (s *Service) Swipe(ctx context.Context, sessionID string, startX, startY, endX, endY, durationMs int) error {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return err
	}
	return app.Swipe(ctx, startX, startY, endX, endY, durationMs)
}

// LongPress executes this operation.
func (s *Service) LongPress(ctx context.Context, sessionID, elementID string, durationMs int) error {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return err
	}
	return app.LongPress(ctx, elementID, durationMs)
}

// PressBack executes this operation.
func (s *Service) PressBack(ctx context.Context, sessionID string) error {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return err
	}
	return app.Back(ctx)
}

// HideKeyboard executes this operation.
func (s *Service) HideKeyboard(ctx context.Context, sessionID string) error {
	app, err := s.getAppiumClient(ctx, sessionID)
	if err != nil {
		return err
	}
	return app.HideKeyboard(ctx)
}

// ScheduleDeviceFarmRun executes this operation.
func (s *Service) ScheduleDeviceFarmRun(ctx context.Context, req devicefarm.ScheduleRunRequest) (map[string]interface{}, error) {
	// Guard calls when device farm integration mode is not enabled.
	if s.dfMode != "run_api" {
		return nil, errors.New(errors.CodeConfigConflict, "devicefarm mode is not run_api")
	}
	// Require a ready client so callers get deterministic setup errors.
	if s.deviceFarm == nil {
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm client not initialized")
	}

	// Resolve stable project key first so cached ARNs can be looked up consistently.
	projectARN := strings.TrimSpace(req.ProjectARN)
	if projectARN == "" {
		projectARN = strings.TrimSpace(s.dfProjectARN)
	}
	req.ProjectARN = projectARN

	// Fill optional runtime arguments from cache to avoid repeated client prompts.
	if strings.TrimSpace(req.AppARN) == "" {
		req.AppARN = s.readDeviceFarmCachedARN(ctx, deviceFarmLastAppPrefix, projectARN)
	}
	if strings.TrimSpace(req.TestPackageARN) == "" {
		req.TestPackageARN = s.readDeviceFarmCachedARN(ctx, deviceFarmLastTestPrefix, projectARN)
	}
	if strings.TrimSpace(req.DevicePoolARN) == "" {
		req.DevicePoolARN = s.readDeviceFarmCachedARN(ctx, deviceFarmLastPoolPrefix, projectARN)
	}

	// Forward scheduling request to AWS Device Farm client.
	result, err := s.deviceFarm.ScheduleRun(ctx, req)
	if err != nil {
		return nil, err
	}

	// Persist latest successful ARNs to reduce required arguments on later calls.
	if projectARN != "" {
		if strings.TrimSpace(req.AppARN) != "" {
			s.writeDeviceFarmCachedARN(ctx, deviceFarmLastAppPrefix, projectARN, req.AppARN)
		}
		if strings.TrimSpace(req.TestPackageARN) != "" {
			s.writeDeviceFarmCachedARN(ctx, deviceFarmLastTestPrefix, projectARN, req.TestPackageARN)
		}
		if pool, ok := result["devicePoolArn"].(string); ok && strings.TrimSpace(pool) != "" {
			s.writeDeviceFarmCachedARN(ctx, deviceFarmLastPoolPrefix, projectARN, pool)
		} else if strings.TrimSpace(req.DevicePoolARN) != "" {
			s.writeDeviceFarmCachedARN(ctx, deviceFarmLastPoolPrefix, projectARN, req.DevicePoolARN)
		}
	}
	return result, nil
}

// CreateDeviceFarmUpload executes this operation.
func (s *Service) CreateDeviceFarmUpload(ctx context.Context, req devicefarm.CreateUploadRequest) (map[string]interface{}, error) {
	// Guard calls when device farm integration mode is not enabled.
	if s.dfMode != "run_api" {
		return nil, errors.New(errors.CodeConfigConflict, "devicefarm mode is not run_api")
	}
	// Require a ready client so callers get deterministic setup errors.
	if s.deviceFarm == nil {
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm client not initialized")
	}
	// Forward upload creation request to AWS Device Farm client.
	result, err := s.deviceFarm.CreateUpload(ctx, req)
	if err != nil {
		return nil, err
	}

	// Store upload metadata so GetDeviceFarmUpload can bind SUCCEEDED uploads back to project cache.
	if s.cache != nil {
		uploadARN, _ := result["uploadArn"].(string)
		projectARN, _ := result["projectArn"].(string)
		uploadType, _ := result["type"].(string)
		if strings.TrimSpace(uploadARN) != "" && strings.TrimSpace(projectARN) != "" && strings.TrimSpace(uploadType) != "" {
			meta := deviceFarmUploadMeta{
				ProjectARN: projectARN,
				Type:       strings.ToUpper(strings.TrimSpace(uploadType)),
			}
			if raw, marshalErr := json.Marshal(meta); marshalErr == nil {
				if err := s.cache.Set(ctx, deviceFarmUploadMetaPrefix+strings.TrimSpace(uploadARN), string(raw), deviceFarmUploadMetaTTL); err != nil {
					s.logger.WarnContext(ctx, "failed to cache device farm upload metadata", "upload_arn", uploadARN, "error", err)
				}
			}
		}
	}
	return result, nil
}

// GetDeviceFarmUpload executes this operation.
func (s *Service) GetDeviceFarmUpload(ctx context.Context, uploadARN string) (map[string]interface{}, error) {
	// Guard calls when device farm integration mode is not enabled.
	if s.dfMode != "run_api" {
		return nil, errors.New(errors.CodeConfigConflict, "devicefarm mode is not run_api")
	}
	// Require a ready client so callers get deterministic setup errors.
	if s.deviceFarm == nil {
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm client not initialized")
	}
	// Return the latest upload processing status from AWS.
	result, err := s.deviceFarm.GetUpload(ctx, uploadARN)
	if err != nil {
		return nil, err
	}

	// Cache SUCCEEDED app/test-package upload ARN so schedule requests can omit repeated values.
	status, _ := result["status"].(string)
	if strings.EqualFold(strings.TrimSpace(status), "SUCCEEDED") {
		s.cacheUploadAsLatestARN(ctx, strings.TrimSpace(uploadARN))
	}
	return result, nil
}

// GetDeviceFarmRun executes this operation.
func (s *Service) GetDeviceFarmRun(ctx context.Context, runARN string) (map[string]interface{}, error) {
	// Guard calls when device farm integration mode is not enabled.
	if s.dfMode != "run_api" {
		return nil, errors.New(errors.CodeConfigConflict, "devicefarm mode is not run_api")
	}
	// Require a ready client so callers get deterministic setup errors.
	if s.deviceFarm == nil {
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm client not initialized")
	}
	// Return the latest run status from AWS.
	return s.deviceFarm.GetRun(ctx, runARN)
}

// GetDeviceFarmRuntimeContext executes this operation.
func (s *Service) GetDeviceFarmRuntimeContext(ctx context.Context, projectARN string) (map[string]interface{}, error) {
	// Guard calls when device farm integration mode is not enabled.
	if s.dfMode != "run_api" {
		return nil, errors.New(errors.CodeConfigConflict, "devicefarm mode is not run_api")
	}

	// Resolve project ARN from argument first, then fallback to configured project.
	resolvedProjectARN := strings.TrimSpace(projectARN)
	if resolvedProjectARN == "" {
		resolvedProjectARN = strings.TrimSpace(s.dfProjectARN)
	}
	if resolvedProjectARN == "" {
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm project arn is required")
	}

	// Return currently known runtime context to help clients decide what still needs to be provided.
	return map[string]interface{}{
		"projectArn":     resolvedProjectARN,
		"appArn":         s.readDeviceFarmCachedARN(ctx, deviceFarmLastAppPrefix, resolvedProjectARN),
		"testPackageArn": s.readDeviceFarmCachedARN(ctx, deviceFarmLastTestPrefix, resolvedProjectARN),
		"devicePoolArn":  s.readDeviceFarmCachedARN(ctx, deviceFarmLastPoolPrefix, resolvedProjectARN),
	}, nil
}

// AdbShell executes this operation.
func (s *Service) AdbShell(ctx context.Context, deviceSerial string, command []string) (map[string]interface{}, error) {
	startedAt := time.Now()                                                                                                        // Capture method start for per-command latency logging.
	s.logger.InfoContext(ctx, "orchestrator AdbShell begin", "device_serial", strings.TrimSpace(deviceSerial), "command", command) // Log adb command invocation.

	// Ensure a non-empty command body before attempting adb invocation.
	if len(command) == 0 {
		s.logger.ErrorContext(ctx, "orchestrator AdbShell failed", "device_serial", strings.TrimSpace(deviceSerial), "duration_ms", time.Since(startedAt).Milliseconds(), "error", "adb command must not be empty") // Log empty command validation failure.
		return nil, errors.New(errors.CodePlanInvalid, "adb command must not be empty")
	}

	// Restrict shell entry points to a conservative command whitelist.
	allowed := map[string]bool{
		"getprop":  true,
		"dumpsys":  true,
		"pm":       true,
		"settings": true,
		"am":       true,
		"input":    true,
		"logcat":   true,
		"wm":       true,
		"svc":      true,
		"ime":      true,
		"monkey":   true,
	}
	// Reject non-whitelisted commands to reduce risk from arbitrary shell execution.
	if !allowed[command[0]] {
		s.logger.ErrorContext(ctx, "orchestrator AdbShell failed", "device_serial", strings.TrimSpace(deviceSerial), "duration_ms", time.Since(startedAt).Milliseconds(), "error", "adb command is not in allowed whitelist", "command_head", command[0]) // Log whitelist rejections with command head.
		return nil, errors.New(errors.CodePermissionDenied, "adb command is not in allowed whitelist")
	}

	// Build adb argv with optional target serial selection.
	args := []string{}
	if strings.TrimSpace(deviceSerial) != "" {
		args = append(args, "-s", strings.TrimSpace(deviceSerial))
	}
	// Route all requests through adb shell with the provided command tokens.
	args = append(args, "shell")
	args = append(args, command...)

	// Execute the command and collect merged stdout/stderr for diagnostics.
	out, err := exec.CommandContext(ctx, "adb", args...).CombinedOutput()
	if err != nil {
		s.logger.ErrorContext(ctx, "orchestrator AdbShell failed", "device_serial", strings.TrimSpace(deviceSerial), "duration_ms", time.Since(startedAt).Milliseconds(), "error", err, "output_bytes", len(out)) // Log execution failure and output size.
		return nil, errors.Wrap(errors.CodeInternal, "adb shell command failed: "+strings.TrimSpace(string(out)), err)
	}

	// Return raw command output so callers can parse device response text.
	result := map[string]interface{}{ // Build command success payload after subprocess completion.
		"success": true,
		"output":  string(out),
	}
	s.logger.InfoContext(ctx, "orchestrator AdbShell done", "device_serial", strings.TrimSpace(deviceSerial), "duration_ms", time.Since(startedAt).Milliseconds(), "output_bytes", len(out)) // Log successful command completion and output size.
	return result, nil
}

func deviceFarmProjectCacheSuffix(projectARN string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(projectARN)))
}

func (s *Service) deviceFarmCacheKey(prefix, projectARN string) string {
	return prefix + deviceFarmProjectCacheSuffix(projectARN)
}

func (s *Service) readDeviceFarmCachedARN(ctx context.Context, prefix, projectARN string) string {
	if s.cache == nil || strings.TrimSpace(projectARN) == "" {
		return ""
	}
	val, err := s.cache.Get(ctx, s.deviceFarmCacheKey(prefix, projectARN))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(val)
}

func (s *Service) writeDeviceFarmCachedARN(ctx context.Context, prefix, projectARN, arn string) {
	if s.cache == nil || strings.TrimSpace(projectARN) == "" || strings.TrimSpace(arn) == "" {
		return
	}
	if err := s.cache.Set(ctx, s.deviceFarmCacheKey(prefix, projectARN), strings.TrimSpace(arn), deviceFarmCacheTTL); err != nil {
		s.logger.WarnContext(ctx, "failed to write device farm arn cache", "prefix", prefix, "project_arn", projectARN, "error", err)
	}
}

func (s *Service) cacheUploadAsLatestARN(ctx context.Context, uploadARN string) {
	if s.cache == nil || strings.TrimSpace(uploadARN) == "" {
		return
	}

	raw, err := s.cache.Get(ctx, deviceFarmUploadMetaPrefix+uploadARN)
	if err != nil || strings.TrimSpace(raw) == "" {
		return
	}

	var meta deviceFarmUploadMeta
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return
	}

	switch strings.ToUpper(strings.TrimSpace(meta.Type)) {
	case "ANDROID_APP", "IOS_APP":
		s.writeDeviceFarmCachedARN(ctx, deviceFarmLastAppPrefix, meta.ProjectARN, uploadARN)
	case "APPIUM_NODE_TEST_PACKAGE", "APPIUM_JAVA_TEST_PACKAGE", "APPIUM_PYTHON_TEST_PACKAGE":
		s.writeDeviceFarmCachedARN(ctx, deviceFarmLastTestPrefix, meta.ProjectARN, uploadARN)
	}
}
