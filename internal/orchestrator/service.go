package orchestrator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	stdErrors "errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/devicefarm"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/startup"
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
	appiumSessionURLKeyPrefix  = "session:appium:url:"
	deviceFarmRemoteKeyPrefix  = "session:devicefarm:remote:"
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
	cfg            config.OrchestratorConfig
	rpcSecurity    config.RPCSecurityConfig
	dao            *postgres.DAO
	cache          *redis.Cache
	s3             *s3.Client
	dispatcher     *Dispatcher
	registry       *WorkerRegistry
	publisher      *EventsPublisher
	appiumURL      string
	appiumRuntime  *startup.LocalAppiumSupervisor
	appiumMu       sync.Mutex
	appiumMap      map[string]*appium.Client
	appiumLastUsed map[string]time.Time
	sessionEndMu   sync.Mutex
	planMu         sync.Mutex
	planCancel     map[string]context.CancelFunc
	snapshotMu     sync.Mutex
	snapshots      map[string]*snapshotCache
	cleanupCancel  context.CancelFunc
	cleanupWG      sync.WaitGroup
	logger         *slog.Logger
	execMode       string
	deviceFarm     deviceFarmRuntimeClient
	dfMode         string
	dfProjectARN   string
}

// NewService executes this operation.
func NewService(cfg config.OrchestratorConfig, rpcCfg config.RPCSecurityConfig, workerCfg config.WorkerConfig, awsCfg config.AWSConfig, dfCfg config.DeviceFarmConfig, dao *postgres.DAO, cache *redis.Cache, s3 *s3.Client) *Service {
	mode := normalizeExecutionMode(cfg.ExecutionMode)
	cfg.ExecutionMode = mode
	registry := NewWorkerRegistry(cache)
	auditRecorder := postgres.NewAuditRecorder(dao) // Reuse the authoritative append-only PostgreSQL sink for worker and lease lifecycle events.
	registry.SetAuditRecorder(auditRecorder)        // Wire worker registration, recovery, degradation, offline, and removal audit events before service startup.
	publisher := NewEventsPublisher(dao, cache)

	svc := &Service{
		cfg:            cfg,
		rpcSecurity:    rpcCfg,
		dao:            dao,
		cache:          cache,
		s3:             s3,
		registry:       registry,
		publisher:      publisher,
		appiumURL:      workerCfg.AppiumURL,
		appiumRuntime:  startup.NewLocalAppiumSupervisor(workerCfg.AppiumURL), // Bind one local Appium supervisor to the configured Appium URL so monolith StartSession can auto-start one loopback Appium service on demand.
		appiumMap:      make(map[string]*appium.Client),
		appiumLastUsed: make(map[string]time.Time),
		planCancel:     make(map[string]context.CancelFunc),
		snapshots:      make(map[string]*snapshotCache),
		logger:         telemetry.Logger(),
		execMode:       mode,
		dfMode:         normalizeDeviceFarmMode(dfCfg.Mode),
		dfProjectARN:   resolvedDeviceFarmProjectARN(dfCfg),
	}

	// Initialize the Device Farm SDK client when any supported Device Farm runtime mode is explicitly enabled.
	if svc.dfMode == "run_api" || svc.dfMode == "remote_access" {
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
	dispatcher := NewDispatcher(cache, registry, executor, cfg.PlanTimeout, workerCfg.HeartbeatInterval, rpcCfg) // Pass the configured plan timeout, worker heartbeat cadence, and RPC security into dispatcher so distributed leases and worker calls follow the selected runtime model.
	dispatcher.finalizer = svc                                                                                   // Wire the service's durable terminalization helper into dispatcher-owned terminal paths before the service starts processing queue messages.
	dispatcher.SetAuditRecorder(auditRecorder)                                                                   // Wire fail-closed lease, dispatch, orphan, and timeout audit events before queue processing begins.
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

	// Start background in-memory cache cleanup.
	s.startCleanupLoop(ctx)

	s.logger.Info("orchestrator service started")
	return nil
}

// Stop stops the orchestrator service
func (s *Service) Stop() {
	s.logger.Info("stopping orchestrator service")
	s.stopCleanupLoop()
	cancelledPlans := s.cancelTrackedPlans() // Cancel every tracked in-flight plan so shutdown does not leave stale cancel handles or live execution contexts behind.
	if s.dispatcher != nil {
		s.dispatcher.Stop()
	}
	if s.registry != nil {
		s.registry.Stop()
	}
	if err := s.stopManagedAppium(); err != nil { // Stop any locally managed Appium child process so monolith shutdown does not leave one orphaned automation server behind.
		s.logger.Warn("failed to stop managed local appium service", "error", err) // Surface managed Appium shutdown failures without masking the rest of the orchestrator shutdown flow.
	}
	removedAppiumClients, removedSnapshots := s.clearInMemoryCaches() // Drop every remaining cached Appium client and snapshot entry so shutdown leaves no in-memory residue behind.
	s.logger.Info("orchestrator service stopped", "cancelled_plans", cancelledPlans, "removed_appium_clients", removedAppiumClients, "removed_snapshots", removedSnapshots)
}

// StartSession executes this operation.
func (s *Service) StartSession(ctx context.Context, projectID string, caps map[string]interface{}) (*postgres.Session, error) {
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "StartSession")
	defer span.End()
	startedAt := time.Now()                                                                                       // Capture method start for duration logging.
	s.logger.InfoContext(ctx, "orchestrator StartSession begin", "project_id", projectID, "caps_keys", len(caps)) // Log method entry with high-signal inputs.

	target, err := s.resolveStartSessionTarget(ctx, projectID, caps) // Resolve the concrete Appium endpoint and sanitized capability payload for either local Appium or one Device Farm remote-access reservation.
	if err != nil {                                                  // Stop immediately when the local or Device Farm runtime target cannot be resolved safely.
		s.logger.ErrorContext(ctx, "orchestrator StartSession failed", "project_id", projectID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log runtime-target resolution failures explicitly.
		return nil, err                                                                                                                                            // Preserve the precise runtime-target resolution error for the caller.
	}
	if strings.TrimSpace(target.appiumURL) == "" {
		s.logger.ErrorContext(ctx, "orchestrator StartSession failed", "project_id", projectID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", "appium_url is empty") // Log configuration failures explicitly.
		return nil, errors.New(errors.CodeConfigMissing, "appium_url is empty")
	}
	if strings.TrimSpace(target.remoteAccessSessionARN) == "" { // Run the local Appium readiness and auto-start path only when the resolved session target is not backed by one Device Farm remote-access reservation.
		if err := s.ensureAppiumReady(ctx); err != nil { // Ensure the configured local Appium endpoint is reachable and auto-start one local loopback Appium service when needed before session creation begins.
			s.logger.ErrorContext(ctx, "orchestrator StartSession failed", "project_id", projectID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log Appium bootstrap failures explicitly so operators can see why session creation never reached the Appium new-session call.
			return nil, errors.WrapPreservingCode("failed to ensure appium service", err)                                                                              // Preserve the reachability or bootstrap error code so gateway transports surface the correct failure semantics to clients.
		}
	}

	app := appium.NewClient(target.appiumURL)                  // Construct one Appium client against the resolved local or Device Farm endpoint before creating the automation session.
	appiumSessionID, err := app.StartSession(ctx, target.caps) // Start the Appium session against the resolved endpoint using the sanitized capability payload.
	if err != nil {                                            // Stop immediately when the downstream Appium endpoint rejects or times out during new-session creation.
		s.cleanupFailedStartSession(ctx, target)                                                                                                                   // Best-effort stop any just-created Device Farm remote-access reservation so failed Appium setup does not leak AWS device slots.
		s.logger.ErrorContext(ctx, "orchestrator StartSession failed", "project_id", projectID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log Appium bootstrap failures explicitly so operators can see why session creation never reached the Appium new-session call.
		return nil, errors.WrapPreservingCode("failed to start appium session", err)                                                                               // Preserve the downstream Appium timeout/not-found code so gateway transports keep the correct external status mapping.
	}

	// 1. Create Session in DB
	sessID := uuid.New().String()
	capsJSON, _ := json.Marshal(caps)
	subject, _ := auth.SubjectFrom(ctx) // Read the authenticated subject once so the persisted session can inherit any tenant and subject ownership metadata attached by the gateway middleware.

	sess := &postgres.Session{
		ID:           sessID,
		ProjectID:    projectID,
		TenantID:     normalizedSubjectTenantID(subject),
		SubjectID:    normalizedSubjectID(subject),
		Status:       "created",
		Capabilities: capsJSON,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	if err := s.dao.CreateSession(ctx, sess); err != nil {
		_ = app.DeleteSession(ctx)
		s.cleanupFailedStartSession(ctx, target)                                                                                                                   // Best-effort stop any just-created Device Farm remote-access reservation because the platform session row never persisted successfully.
		s.logger.ErrorContext(ctx, "orchestrator StartSession failed", "project_id", projectID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log persistence failure before returning.
		return nil, errors.Wrap(errors.CodeInternal, "failed to create session", err)
	}

	if err := s.persistSessionAutomationState(ctx, sessID, appiumSessionID, target); err != nil { // Persist the Appium session id, concrete endpoint, and optional Device Farm reservation handle for later reuse and cleanup.
		s.logger.WarnContext(ctx, "failed to persist session automation state",
			"session_id", sessID,
			"appium_session_id", appiumSessionID,
			"appium_url", target.appiumURL,
			"remote_access_session_arn", target.remoteAccessSessionARN,
			"error", err)
	}

	s.appiumMu.Lock()
	s.appiumMap[sessID] = app
	s.appiumLastUsed[sessID] = time.Now()
	s.appiumMu.Unlock()

	s.logger.InfoContext(ctx, "orchestrator StartSession done", "project_id", projectID, "session_id", sessID, "appium_session_id", appiumSessionID, "appium_url", target.appiumURL, "remote_access_session_arn", target.remoteAccessSessionARN, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful method completion and key identifiers.
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
	sess, err := s.authorizeSessionAccess(ctx, sessionID)
	if err != nil {
		s.logger.ErrorContext(ctx, "orchestrator ExecutePlan failed", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log session lookup failure.
		return "", err
	}
	if sess.Status != "created" && sess.Status != "active" { // Reject ending and ended sessions before creating a trace or queue message.
		s.logger.ErrorContext(ctx, "orchestrator ExecutePlan failed", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", "session is not active") // Log invalid session state before returning.
		return "", errors.New(errors.CodeSessionDead, "session is not active")                                                                                                                             // Preserve the stable dead-session classification for every unusable lifecycle state.
	}

	// 2. Create Trace
	if traceID == "" {
		traceID = uuid.New().String()
	}
	trace := &postgres.Trace{
		ID:        traceID,
		SessionID: sessionID,
		ProjectID: sess.ProjectID,
		TenantID:  sess.TenantID,
		SubjectID: sess.SubjectID,
		Status:    "pending",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := s.dao.CreateOrRearmQueuedTrace(ctx, trace); err != nil { // Insert the new trace or safely rearm only a matching trace whose previous queue enqueue failed.
		s.logger.ErrorContext(ctx, "orchestrator ExecutePlan failed", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log trace creation failures.
		return "", errors.WrapPreservingCode("failed to prepare trace", err)                                                                                                           // Preserve state-conflict and storage classifications from the atomic trace preparation helper.
	}

	// 3. Enqueue Plan
	if err := s.dispatcher.EnqueuePlan(ctx, sess.ProjectID, sessionID, traceID, plan); err != nil {
		s.logger.ErrorContext(ctx, "orchestrator ExecutePlan failed", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log enqueue failures for queue-level troubleshooting.
		_, finalizationErr := s.finalizeTraceDetached(ctx, traceID, []string{"pending"}, "failed", "queue_enqueue_failed", "trace failed before queue acceptance", err, 0)             // Close the prepared trace even when the request context was cancelled by the enqueue failure.
		if finalizationErr != nil {                                                                                                                                                    // Surface both failures when PostgreSQL cannot record the queue rejection durably.
			return "", errors.Wrap(errors.CodeStoreWrite, "failed to enqueue plan and finalize trace", stdErrors.Join(err, finalizationErr)) // Preserve both root causes for server logs under a stable storage-write code.
		}
		return "", errors.Wrap(errors.CodeInternal, "failed to enqueue plan", err) // Return the enqueue failure after the trace has reached a retryable terminal state.
	}

	s.logger.InfoContext(ctx, "orchestrator ExecutePlan done", "session_id", sessionID, "trace_id", traceID, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful method completion.
	return traceID, nil
}

// GetEvents executes this operation.
func (s *Service) GetEvents(ctx context.Context, traceID string, sinceSeq int64) ([]*postgres.PlanEvent, error) {
	if _, err := s.authorizeTraceAccess(ctx, traceID); err != nil { // Validate trace ownership before exposing replayable event history to authenticated callers.
		return nil, err // Preserve the trace-not-found or permission-denied error so transports keep stable status mapping.
	}

	return s.dao.ListEvents(ctx, traceID, sinceSeq, 100)
}

// GetArtifacts executes this operation.
func (s *Service) GetArtifacts(ctx context.Context, traceID string) ([]*postgres.Artifact, error) {
	if _, err := s.authorizeTraceAccess(ctx, traceID); err != nil { // Validate trace ownership before exposing artifact metadata to authenticated callers.
		return nil, err // Preserve the trace-not-found or permission-denied error so transports keep stable status mapping.
	}

	return s.dao.ListArtifacts(ctx, traceID)
}

// GetSession executes this operation.
func (s *Service) GetSession(ctx context.Context, sessionID string) (*postgres.Session, error) {
	return s.authorizeSessionAccess(ctx, sessionID) // Reuse the shared session-authorization helper so every authenticated session read enforces persisted ownership.
}

// EndSession moves one session through ending, retries external cleanup safely, and commits ended only after every cleanup handle is released.
func (s *Service) EndSession(ctx context.Context, sessionID string) error {
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "EndSession")
	defer span.End()
	startedAt := time.Now()                                                             // Capture method start for duration logging.
	s.logger.InfoContext(ctx, "orchestrator EndSession begin", "session_id", sessionID) // Log session shutdown method entry.
	if _, err := s.authorizeSessionAccess(ctx, sessionID); err != nil {                 // Validate persisted session ownership before any authenticated caller can terminate the session.
		s.logger.ErrorContext(ctx, "orchestrator EndSession failed", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log authorization failures before returning.
		return err                                                                                                                                               // Preserve the authorization failure so transports keep the correct status mapping.
	}
	s.sessionEndMu.Lock()         // Serialize cleanup attempts so concurrent EndSession calls cannot delete the same Appium or Device Farm resource twice.
	defer s.sessionEndMu.Unlock() // Release the lifecycle lock after cleanup and durable finalization complete or fail.

	cleanupRequired, err := s.prepareSessionEnd(ctx, sessionID, time.Now()) // Persist cleanup-in-progress before any external resource operation begins.
	if err != nil {                                                         // Stop before touching the in-memory Appium session map when the persisted session transition fails.
		s.logger.ErrorContext(ctx, "orchestrator EndSession failed", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log lookup failures before returning.
		return err
	}
	if !cleanupRequired { // Treat repeated end-session requests as successful no-ops only after the persisted row is already ended.
		s.logger.InfoContext(ctx, "orchestrator EndSession done", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds(), "already_ended", true) // Log idempotent completion path.
		return nil
	}

	s.appiumMu.Lock()                                                       // Read the cached client under the same mutex used by interactive operations and cleanup eviction.
	app := s.appiumMap[sessionID]                                           // Preserve the live client so cleanup does not depend exclusively on Redis restoration.
	s.appiumMu.Unlock()                                                     // Release the cache mutex before performing network cleanup.
	if err := s.releaseSessionAutomation(ctx, sessionID, app); err != nil { // Release Appium, Device Farm, and persisted handles while retaining them on any external failure.
		s.logger.ErrorContext(ctx, "orchestrator EndSession cleanup failed", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Record the retryable cleanup failure while the row remains ending.
		return err                                                                                                                                                       // Preserve the cleanup error so the caller knows to retry the ending session.
	}
	finalizationContext, finalizationCancel := context.WithTimeout(context.WithoutCancel(ctx), detachedFinalizationTimeout) // Give the terminal database write a bounded chance to succeed after request cancellation.
	defer finalizationCancel()                                                                                              // Release the detached finalization timer when the method returns.
	if err := s.completeSessionEnd(finalizationContext, sessionID, time.Now()); err != nil {                                // Commit ended only after every external cleanup operation succeeded.
		s.logger.ErrorContext(ctx, "orchestrator EndSession finalization failed", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Record the retryable database finalization failure.
		return err                                                                                                                                                            // Leave the row in ending so a later call can retry idempotent cleanup and finalization.
	}
	s.appiumMu.Lock()                   // Evict the now-closed client only after durable terminal finalization succeeds.
	delete(s.appiumMap, sessionID)      // Remove the closed Appium client from the in-memory session map.
	delete(s.appiumLastUsed, sessionID) // Remove its idle-cleanup timestamp alongside the client.
	s.appiumMu.Unlock()                 // Release the cache mutex after terminal eviction completes.

	s.logger.InfoContext(ctx, "orchestrator EndSession done", "session_id", sessionID, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful session termination.
	return nil
}

// CancelPlan cancels local execution, propagates cancellation to distributed workers when needed, and marks active traces as cancelled without clobbering terminal states.
func (s *Service) CancelPlan(ctx context.Context, traceID string) error {
	if _, err := s.authorizeTraceAccess(ctx, traceID); err != nil { // Validate persisted trace ownership before any authenticated caller can cancel the trace.
		return err // Preserve the trace-not-found or permission-denied error so transports keep stable status mapping.
	}

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
	cancelled, err := s.finalizeTrace(ctx, traceID, []string{"pending", "running"}, "cancelled", "cancelled", "trace cancelled", nil, 0) // Persist the cancelled terminal state together with one final trace event so replay consumers see a durable terminal marker.
	if err != nil {                                                                                                                      // Stop immediately when the transactional terminalization helper fails.
		return err // Preserve the wrapped storage error produced by the optimistic transition helper.
	}
	if !cancelled { // Keep cancel idempotent when another worker or caller has already moved the trace to a terminal state.
		s.logger.InfoContext(ctx, "cancel ignored because trace is already terminal", "trace_id", traceID) // Emit a low-noise informational log so operators can distinguish ignored cancels from failures.
	}

	return nil
}

// GetTrace executes this operation.
func (s *Service) GetTrace(ctx context.Context, traceID string) (*postgres.Trace, []*postgres.PlanEvent, error) {
	trace, err := s.authorizeTraceAccess(ctx, traceID)
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
	trace, err := s.authorizeTraceAccess(ctx, traceID) // Validate persisted trace ownership and load its authoritative session binding before storing artifacts.
	if err != nil {                                    // Stop before touching Appium when the trace is missing or not owned by the caller.
		return nil, err // Preserve the trace-not-found or permission-denied error so transports keep stable status mapping.
	}
	if trace.SessionID != sessionID { // Reject cross-session artifact attachment even when the caller owns both resources.
		return nil, errors.New(errors.CodePlanInvalid, "trace does not belong to session") // Prevent screenshots from one automation session being stored under another trace.
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
		thumbData, err := buildScreenshotThumbnail(data, 640)
		if err != nil {
			return nil, err
		}
		thumbRef, err := s.storeArtifact(ctx, traceID, "screenshot_thumb.png", "image/png", thumbData)
		if err != nil {
			return nil, err
		}
		result["thumb"] = thumbRef
	}

	return result, nil
}

// getAppiumClient executes this operation.
func (s *Service) getAppiumClient(ctx context.Context, sessionID string) (*appium.Client, error) {
	session, err := s.authorizeSessionAccess(ctx, sessionID) // Validate persisted session ownership and load the authoritative lifecycle state before exposing automation handles.
	if err != nil {                                          // Reject missing or unauthorized sessions before inspecting in-memory state.
		return nil, err // Preserve the session-not-found or permission-denied error so transports keep stable status mapping.
	}
	if session.Status != "created" && session.Status != "active" { // Reject ending and ended sessions before any cached or restored Appium command can run.
		return nil, errors.New(errors.CodeSessionDead, "session is not active") // Preserve the stable dead-session classification throughout interactive and plan operations.
	}

	s.appiumMu.Lock()
	app := s.appiumMap[sessionID]
	if app != nil {
		s.appiumLastUsed[sessionID] = time.Now()
	}
	s.appiumMu.Unlock()
	if app == nil {
		restored, err := s.restoreAppiumClient(ctx, sessionID) // Rebuild one Appium client from the persisted per-session endpoint and Appium session mapping when the in-memory cache no longer has it.
		if err != nil {                                        // Stop immediately when the persisted session mappings cannot be restored into one live Appium client.
			return nil, err // Preserve the stable session-not-found or storage-read error returned by the restore helper.
		}

		s.appiumMu.Lock()
		if existing := s.appiumMap[sessionID]; existing != nil {
			app = existing
			s.appiumLastUsed[sessionID] = time.Now()
		} else {
			s.appiumMap[sessionID] = restored
			s.appiumLastUsed[sessionID] = time.Now()
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
		trace, lookupErr := s.dao.GetTrace(ctx, traceID) // Inspect the durable state so duplicate queue deliveries can be acknowledged without re-executing the plan.
		if lookupErr != nil {                            // Preserve storage and not-found failures because the dispatcher cannot classify the duplicate safely without the trace row.
			return lookupErr // Return the authoritative lookup failure so queue retry policy remains consistent.
		}
		switch trace.Status { // Treat an already accepted or terminal trace as a benign duplicate delivery.
		case "running", "completed", "failed", "cancelled":
			s.logger.InfoContext(ctx, "duplicate queued plan ignored", "trace_id", traceID, "status", trace.Status) // Record duplicate suppression without replaying device actions.
			return nil                                                                                              // Let the dispatcher ACK the duplicate message because another delivery already owns or completed the trace.
		default:
			return errors.New(errors.CodeStateConflict, "trace is not pending") // Preserve a stable conflict for unknown lifecycle states that require operator attention.
		}
	}

	steps, err := worker.ParsePlan(plan)
	if err != nil {
		_, finalizationErr := s.finalizeTraceDetached(ctx, traceID, []string{"running"}, "failed", "parse_error", "trace failed before execution started", err, 0) // Persist the failed terminal state even if the execution context already expired.
		return stdErrors.Join(err, finalizationErr)                                                                                                                // Return both parsing and persistence failures while preserving the original plan error when finalization succeeds.
	}

	app, err := s.getAppiumClient(runCtx, sessionID) // Bind session restoration and validation to the plan execution timeout.
	if err != nil {
		_, finalizationErr := s.finalizeTraceDetached(ctx, traceID, []string{"running"}, "failed", "session_unavailable", "trace failed before acquiring the session client", err, 0) // Persist the failed terminal state without relying on the possibly expired execution context.
		return stdErrors.Join(err, finalizationErr)                                                                                                                                   // Return both session and persistence failures when needed.
	}

	seq := int64(0)      // Allocate step-event sequence numbers monotonically within this newly accepted trace execution.
	var publishErr error // Capture the first durable event failure because executor callbacks cannot return errors directly.
	exec := worker.NewExecutor(app, s.cfg.StepTimeout, s.cfg.AutoWaitMax, func(ev worker.StepEvent) {
		if publishErr != nil { // Stop attempting later event writes after the first durable persistence failure.
			return // Preserve the first failure as the root cause that will terminalize the trace.
		}
		seq++                                                       // Reserve the next ordered event sequence before serializing this executor callback.
		payload, marshalErr := json.Marshal(map[string]interface{}{ // Serialize the complete step event payload before attempting persistence.
			"message":      ev.Message,
			"metrics":      ev.Metrics,
			"artifactRefs": ev.ArtifactRefs,
			"phase":        ev.Phase,
		})
		if marshalErr != nil { // Stop when one event payload cannot be represented as JSON.
			publishErr = errors.Wrap(errors.CodeInternal, "failed to marshal plan event", marshalErr) // Preserve the serialization cause for terminal diagnostics.
			return                                                                                    // Skip persistence because no valid event payload exists.
		}
		publishErr = s.publisher.Publish(runCtx, &postgres.PlanEvent{ // Persist every step event and retain the first failure for execution outcome handling.
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
	if publishErr != nil { // Treat a missing durable step event as a trace execution failure even when device actions completed.
		err = stdErrors.Join(err, publishErr) // Preserve any executor failure together with the first event persistence failure.
	}
	elapsed := time.Since(start).Seconds()
	if err != nil {
		if code, ok := errors.CodeOf(err); ok {
			telemetry.ErrorCodeTotal.WithLabelValues(string(code), "orchestrator").Inc()
		}
		if errors.IsCode(err, errors.CodeSessionDead) || errors.IsCode(err, errors.CodeSessionBroken) {
			_, finalizationErr := s.finalizeTraceDetached(ctx, traceID, []string{"running"}, "failed", "session_broken", "trace failed because the session became unusable", err, 0) // Persist the failed terminal state without relying on the failed execution context.
			telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
			return stdErrors.Join(err, finalizationErr) // Preserve execution and terminal persistence failures for dispatcher retry decisions.
		}
		if runCtx.Err() == context.Canceled || runCtx.Err() == context.DeadlineExceeded {
			_, finalizationErr := s.finalizeTraceDetached(ctx, traceID, []string{"running"}, "cancelled", "cancelled", "trace cancelled during execution", err, 0) // Persist cancellation after the execution context has already been cancelled.
			telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
			return stdErrors.Join(err, finalizationErr) // Preserve execution and terminal persistence failures for dispatcher retry decisions.
		}
		telemetry.ExecuteLatency.WithLabelValues("", "", "failed").Observe(elapsed)
		_, finalizationErr := s.finalizeTraceDetached(ctx, traceID, []string{"running"}, "failed", "execution_failed", "trace failed during execution", err, 0) // Persist the failed terminal state independently from the request context.
		return stdErrors.Join(err, finalizationErr)                                                                                                             // Preserve execution and terminal persistence failures for dispatcher retry decisions.
	}

	telemetry.ExecuteLatency.WithLabelValues("", "", "passed").Observe(elapsed)
	_, err = s.finalizeTraceDetached(ctx, traceID, []string{"running"}, "completed", "completed", "trace completed", nil, 0) // Persist completion with a fresh bounded context so a just-expired request cannot strand the trace in running.
	return err                                                                                                               // Return any storage-layer failure while treating a lost race to another terminal state as a successful no-op.
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

	validatedCommand, err := validateADBCommand(command) // Normalize the request tokens and enforce the reviewed adb subcommand and parameter policy before any subprocess starts.
	if err != nil {                                      // Stop immediately when the command family, subcommand, or parameters fall outside the reviewed safe surface.
		s.logger.ErrorContext(ctx, "orchestrator AdbShell failed", "device_serial", strings.TrimSpace(deviceSerial), "duration_ms", time.Since(startedAt).Milliseconds(), "error", err, "command_head", firstCommandToken(command)) // Log policy rejections with the first token for security diagnostics.
		return nil, err                                                                                                                                                                                                             // Preserve the stable validation or permission error returned by the adb command policy helper.
	}

	// Build adb argv with optional target serial selection.
	args := []string{}
	if strings.TrimSpace(deviceSerial) != "" {
		args = append(args, "-s", strings.TrimSpace(deviceSerial))
	}
	// Route all requests through adb shell with the provided command tokens.
	args = append(args, "shell")
	args = append(args, validatedCommand...)

	// Execute the command and collect merged stdout/stderr for diagnostics.
	out, err := adbCommandContext(ctx, "adb", args...).CombinedOutput()
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

// firstCommandToken returns the first token from one requested adb command for logging purposes.
func firstCommandToken(command []string) string {
	if len(command) == 0 { // Guard against empty slices so log calls never panic while formatting rejected requests.
		return "" // Return the empty string because no first token exists on an empty command slice.
	}
	return strings.TrimSpace(command[0]) // Return the trimmed first token so policy-rejection logs expose the actual requested command family.
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
