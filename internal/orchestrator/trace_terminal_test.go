// trace_terminal_test.go verifies that queued and local execution paths always persist one terminal trace state together with one final replayable event.
package orchestrator

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/postgres"
	redisstore "mcp_for_appium/internal/storage/redis"

	"github.com/alicebob/miniredis/v2"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	testDatabaseName = "mcp_mobile_worker" // testDatabaseName keeps the embedded PostgreSQL database name aligned with the repository defaults.
	testDatabaseUser = "postgres"          // testDatabaseUser defines the superuser name used by the embedded PostgreSQL test instance.
	testDatabasePass = "postgres"          // testDatabasePass defines the password paired with the embedded PostgreSQL test superuser.
	testDatabaseHost = "127.0.0.1"         // testDatabaseHost keeps the embedded PostgreSQL test instance bound to IPv4 loopback only.
)

// traceTerminalHarness owns the isolated PostgreSQL, Redis, DAO, cache, and service used by one trace-terminalization test.
type traceTerminalHarness struct {
	dao          *postgres.DAO
	cache        *redisstore.Cache
	service      *Service
	stopPostgres func()
	stopRedis    func()
}

// newTraceTerminalHarness starts isolated PostgreSQL and Redis dependencies, applies migrations, and constructs a service ready for terminal-state tests.
func newTraceTerminalHarness(t *testing.T) *traceTerminalHarness {
	ctx := context.Background()                                                        // Use one shared background context because the harness setup runs synchronously inside the current test.
	port := reserveLoopbackPort(t)                                                     // Reserve one loopback TCP port so the embedded PostgreSQL instance can bind without colliding with developer-managed services.
	tempDir := t.TempDir()                                                             // Allocate one isolated filesystem root so the embedded PostgreSQL runtime never shares state across tests.
	postgresInstance := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig(). // Construct the embedded PostgreSQL instance with fully isolated paths and credentials for this one test.
												Port(uint32(port)).                               // Bind the embedded database to the reserved loopback port for this isolated test instance.
												Database(testDatabaseName).                       // Create the same database name that the repository examples and DAO defaults already use.
												Username(testDatabaseUser).                       // Create the same superuser name used by the repository examples and migration replay tooling.
												Password(testDatabasePass).                       // Create the matching superuser password for the isolated test instance.
												RuntimePath(filepath.Join(tempDir, "runtime")).   // Store the PostgreSQL runtime files under the test temp directory so nothing leaks into the developer environment.
												DataPath(filepath.Join(tempDir, "data")).         // Store the PostgreSQL data directory under the test temp directory so every test starts from a clean cluster.
												BinariesPath(filepath.Join(tempDir, "binaries"))) // Store downloaded PostgreSQL binaries under the test temp directory so tests remain self-contained.
	if err := postgresInstance.Start(); err != nil { // Stop immediately when the embedded PostgreSQL server cannot start for this test.
		t.Fatalf("failed to start embedded postgres: %v", err) // Surface the startup failure because no persistence-path assertion can run without a live database.
	}
	stopPostgres := func() { // Capture the embedded PostgreSQL shutdown in one closure so test cleanup remains concise and deterministic.
		if err := postgresInstance.Stop(); err != nil { // Attempt to stop the embedded PostgreSQL instance once the test completes.
			t.Fatalf("failed to stop embedded postgres: %v", err) // Surface shutdown failures because they usually indicate a leaked connection or stuck background process.
		}
	}
	t.Cleanup(stopPostgres) // Ensure the embedded PostgreSQL instance always stops even when the test fails partway through.

	dsn := buildTestDSN(port)              // Build the DAO and migration DSN that points at the isolated embedded PostgreSQL instance.
	applyRepositoryMigrations(t, ctx, dsn) // Apply the committed repository migrations before the DAO touches the fresh embedded database.

	dao, err := postgres.NewDAO(ctx, config.PostgresConfig{DSN: dsn, MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute}) // Open the production DAO against the live embedded database so the test exercises real persistence code.
	if err != nil {                                                                                                                   // Stop immediately when the DAO cannot connect to the isolated embedded database.
		t.Fatalf("failed to create dao: %v", err) // Surface the DAO construction failure because none of the terminal-state assertions can proceed without it.
	}
	t.Cleanup(func() { dao.Close() }) // Ensure the DAO pool closes before the embedded PostgreSQL instance shuts down.

	redisServer := miniredis.RunT(t)                                                // Start one in-memory Redis server so dispatcher cancellation markers and retry counters use real Redis commands.
	cache, err := redisstore.NewCache(config.RedisConfig{Addr: redisServer.Addr()}) // Open the production Redis cache wrapper against the isolated in-memory Redis server.
	if err != nil {                                                                 // Stop immediately when the Redis cache wrapper cannot connect to the isolated in-memory Redis server.
		t.Fatalf("failed to create redis cache: %v", err) // Surface the Redis setup failure because dispatcher terminalization depends on cache operations.
	}
	stopRedis := func() { // Capture the Redis shutdown in one closure so test cleanup remains concise and deterministic.
		if err := cache.Close(); err != nil { // Close the production cache wrapper before the in-memory Redis server is torn down.
			t.Fatalf("failed to close redis cache: %v", err) // Surface cache-close failures because they usually indicate leaked resources in the test path.
		}
		redisServer.Close() // Stop the in-memory Redis server after the cache wrapper has released its connection.
	}
	t.Cleanup(stopRedis) // Ensure the Redis client and server always stop even when the test fails partway through.

	service := NewService(config.OrchestratorConfig{ // Construct the production orchestrator service with the minimum config required for terminalization-path tests.
		PlanTimeout:   time.Minute,            // Give local execution tests enough wall-clock time to reach their terminal helper paths.
		StepTimeout:   5 * time.Second,        // Use a positive per-step timeout because the worker executor requires one.
		AutoWaitMax:   500 * time.Millisecond, // Use a small positive auto-wait value because the worker executor requires one even when tests never hit Appium.
		SnapshotTTL:   time.Minute,            // Use a positive snapshot TTL because the orchestrator config expects one.
		ExecutionMode: ExecutionModeMonolith,  // Exercise the monolith execution path so dispatcher-owned and local terminalization share one service instance.
	}, config.WorkerConfig{
		AppiumURL: "http://127.0.0.1:4723", // Provide a syntactically valid Appium URL even though these tests avoid real Appium calls.
	}, config.AWSConfig{}, config.DeviceFarmConfig{Mode: "disabled"}, dao, cache, nil) // Disable Device Farm and S3 because the terminalization tests do not touch those integrations.

	return &traceTerminalHarness{dao: dao, cache: cache, service: service, stopPostgres: stopPostgres, stopRedis: stopRedis} // Return the fully wired harness so individual tests can seed traces and assert final states.
}

// reserveLoopbackPort reserves one IPv4 loopback TCP port and returns it so embedded PostgreSQL can bind deterministically during tests.
func reserveLoopbackPort(t *testing.T) int {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Ask the kernel for one unused IPv4 loopback port so the embedded database does not collide with other local services.
	if err != nil {                                    // Stop immediately when even loopback port reservation fails in the current environment.
		t.Fatalf("failed to reserve loopback port: %v", err) // Surface the port-reservation failure because the embedded database cannot start without a free port.
	}
	defer listener.Close() // Release the reserved port immediately so embedded PostgreSQL can bind it during the next setup step.

	return listener.Addr().(*net.TCPAddr).Port // Return the kernel-selected free TCP port for the embedded PostgreSQL instance.
}

// buildTestDSN returns the PostgreSQL DSN used by the embedded-database-backed orchestrator tests.
func buildTestDSN(port int) string {
	return "postgres://" + testDatabaseUser + ":" + testDatabasePass + "@" + testDatabaseHost + ":" + strconv.Itoa(port) + "/" + testDatabaseName + "?sslmode=disable" // Build one DSN string that points at the isolated embedded PostgreSQL test instance.
}

// applyRepositoryMigrations replays every committed PostgreSQL migration against the embedded database used by the current test.
func applyRepositoryMigrations(t *testing.T, ctx context.Context, dsn string) {
	conn, err := pgx.Connect(ctx, dsn) // Open one direct PostgreSQL connection used only for migration replay inside the current test setup.
	if err != nil {                    // Stop immediately when the embedded database cannot accept a direct pgx connection.
		t.Fatalf("failed to connect for migration replay: %v", err) // Surface the connection failure because the schema cannot be prepared without it.
	}
	defer conn.Close(ctx) // Release the direct migration connection before the DAO pool starts using the embedded database.

	migrationFiles, err := filepath.Glob(filepath.Join(repositoryRoot(t), "internal", "storage", "postgres", "migrations", "*.sql")) // Resolve every committed migration file shipped in the repository.
	if err != nil {                                                                                                                  // Stop immediately when the filesystem glob itself fails.
		t.Fatalf("failed to glob migration files: %v", err) // Surface the glob failure because the test cannot prepare the schema without the migration list.
	}
	sort.Strings(migrationFiles)                   // Replay the migrations in lexical order so the schema matches normal startup expectations.
	for _, migrationFile := range migrationFiles { // Apply every committed migration file to the isolated embedded database before the test seeds rows.
		sqlBytes, err := os.ReadFile(migrationFile) // Read the exact migration SQL committed in the repository working tree.
		if err != nil {                             // Stop immediately when a committed migration file cannot be read.
			t.Fatalf("failed to read migration file %s: %v", migrationFile, err) // Surface the unreadable file path because the schema setup cannot continue without it.
		}
		if _, err := conn.Exec(ctx, string(sqlBytes)); err != nil { // Execute the migration SQL exactly as committed against the live embedded database.
			t.Fatalf("failed to apply migration %s: %v", filepath.Base(migrationFile), err) // Surface the failing migration name because the schema setup depends on every migration succeeding.
		}
	}
}

// repositoryRoot returns the repository root directory by walking upward from this test file's location.
func repositoryRoot(t *testing.T) string {
	_, file, _, ok := runtime.Caller(0) // Read the current test file path so the repository root can be derived without depending on the process working directory.
	if !ok {                            // Stop immediately when the runtime cannot determine the current file path.
		t.Fatal("failed to resolve current test file path") // Surface the missing runtime caller metadata because migration discovery depends on it.
	}

	return filepath.Dir(filepath.Dir(filepath.Dir(file))) // Walk from internal/orchestrator/trace_terminal_test.go back up to the repository root directory.
}

// seedPendingTrace inserts one active session and one pending trace that terminalization tests can transition.
func (h *traceTerminalHarness) seedPendingTrace(t *testing.T) (string, string) {
	ctx := context.Background()   // Use one shared background context because the seed writes run synchronously inside the current test.
	now := time.Now().UTC()       // Capture one stable timestamp so the related session and trace rows stay chronologically consistent.
	sessionID := uuid.NewString() // Generate one unique session identifier so concurrent or repeated test runs never collide on primary keys.
	traceID := uuid.NewString()   // Generate one unique trace identifier linked to the seeded session.

	if err := h.dao.CreateSession(ctx, &postgres.Session{ID: sessionID, ProjectID: "test-project", Status: "created", Capabilities: []byte(`{"platformName":"Android"}`), CreatedAt: now, UpdatedAt: now}); err != nil { // Insert one active session row because trace execution and cancellation require a live session record.
		t.Fatalf("failed to seed session: %v", err) // Surface the seed failure because the test cannot exercise trace terminalization without a session row.
	}
	if err := h.dao.CreateTrace(ctx, &postgres.Trace{ID: traceID, SessionID: sessionID, ProjectID: "test-project", Status: "pending", CreatedAt: now, UpdatedAt: now}); err != nil { // Insert one pending trace row because every terminalization test starts from the pending lifecycle state.
		t.Fatalf("failed to seed trace: %v", err) // Surface the seed failure because the test cannot exercise terminal transitions without a trace row.
	}

	return sessionID, traceID // Return the seeded identifiers so the test can call the service or dispatcher paths under test.
}

// loadTraceAndEvents reads the current trace row plus its ordered events for terminalization assertions.
func (h *traceTerminalHarness) loadTraceAndEvents(t *testing.T, traceID string) (*postgres.Trace, []*postgres.PlanEvent) {
	trace, events, err := h.service.GetTrace(context.Background(), traceID) // Read the persisted trace and replayable events through the production service API.
	if err != nil {                                                         // Stop immediately when the service cannot read back the seeded trace state.
		t.Fatalf("failed to load trace %s: %v", traceID, err) // Surface the read failure because the test cannot assert terminalization without the persisted result.
	}

	return trace, events // Return the persisted trace row plus its replayable event sequence for assertions.
}

// decodeEventPayload unmarshals one persisted event payload into a generic map so tests can assert terminal metadata fields without a dedicated schema type.
func decodeEventPayload(t *testing.T, payload []byte) map[string]interface{} {
	decoded := make(map[string]interface{})                   // Allocate the destination map used by the generic JSON decode for assertions.
	if err := json.Unmarshal(payload, &decoded); err != nil { // Decode the persisted event payload into a generic map so assertions can inspect its terminal metadata.
		t.Fatalf("failed to decode event payload: %v", err) // Surface payload-decode failures because they mean the terminal event was not stored in valid JSON form.
	}

	return decoded // Return the decoded payload map for terminal-field assertions in the calling test.
}

// TestCancelPlanFinalizesPendingTrace verifies that API-level cancellation persists both the cancelled terminal state and one final replayable event.
func TestCancelPlanFinalizesPendingTrace(t *testing.T) {
	harness := newTraceTerminalHarness(t)     // Start one isolated orchestrator test harness so this test can exercise the real cancellation persistence path.
	_, traceID := harness.seedPendingTrace(t) // Seed one pending trace because cancellation must start from an active lifecycle state.

	if err := harness.service.CancelPlan(context.Background(), traceID); err != nil { // Execute the production cancellation path against the seeded pending trace.
		t.Fatalf("expected cancel to succeed, got error: %v", err) // Surface unexpected cancellation failures because the trace should reach a terminal state cleanly.
	}

	trace, events := harness.loadTraceAndEvents(t, traceID) // Read back the persisted trace row and replayable events after cancellation.
	if trace.Status != "cancelled" {                        // Fail the test when the trace row did not persist the cancelled terminal state.
		t.Fatalf("expected trace status cancelled, got %s", trace.Status) // Surface the unexpected trace state because task 4 requires a durable terminal status.
	}
	if len(events) != 1 { // Fail the test when cancellation did not append exactly one final replayable event.
		t.Fatalf("expected 1 terminal event, got %d", len(events)) // Surface the unexpected event count because task 4 requires one durable terminal marker.
	}
	if events[0].Status != "cancelled" { // Fail the test when the final event did not mirror the cancelled terminal state.
		t.Fatalf("expected terminal event status cancelled, got %s", events[0].Status) // Surface the unexpected event status because replay consumers depend on it.
	}
	if events[0].StepIndex != -1 { // Fail the test when the final event did not use the sentinel trace-level step index.
		t.Fatalf("expected terminal event step index -1, got %d", events[0].StepIndex) // Surface the unexpected step index because trace-level terminal events must be distinguishable from step events.
	}

	payload := decodeEventPayload(t, events[0].Payload) // Decode the persisted terminal payload so the test can inspect its trace-level metadata.
	if payload["terminalStatus"] != "cancelled" {       // Fail the test when the final event payload does not record the cancelled terminal status.
		t.Fatalf("expected terminal payload status cancelled, got %#v", payload["terminalStatus"]) // Surface the unexpected payload field because replay consumers may inspect the payload for terminal metadata.
	}
}

// TestExecuteDispatchedPlanFinalizesParseFailure verifies that local execution failures before any step runs still persist a failed terminal state and final event.
func TestExecuteDispatchedPlanFinalizesParseFailure(t *testing.T) {
	harness := newTraceTerminalHarness(t)             // Start one isolated orchestrator test harness so this test can exercise the real local execution persistence path.
	sessionID, traceID := harness.seedPendingTrace(t) // Seed one pending trace tied to an active session because local execution requires both records.

	if err := harness.service.ExecuteDispatchedPlan(context.Background(), traceID, sessionID, json.RawMessage(`{}`)); err == nil { // Execute the local plan path with an invalid plan so parsing fails before any Appium dependency is needed.
		t.Fatal("expected invalid plan execution to fail") // Surface the missing expected error because task 4 relies on the failure path to exercise terminal finalization.
	}

	trace, events := harness.loadTraceAndEvents(t, traceID) // Read back the persisted trace row and replayable events after the parse failure.
	if trace.Status != "failed" {                           // Fail the test when the trace row did not persist the failed terminal state.
		t.Fatalf("expected trace status failed, got %s", trace.Status) // Surface the unexpected trace state because task 4 requires a durable terminal failure state.
	}
	if len(events) != 1 { // Fail the test when the parse failure did not append exactly one final replayable event.
		t.Fatalf("expected 1 terminal event, got %d", len(events)) // Surface the unexpected event count because task 4 requires one durable terminal marker.
	}
	if events[0].Status != "failed" { // Fail the test when the final event did not mirror the failed terminal state.
		t.Fatalf("expected terminal event status failed, got %s", events[0].Status) // Surface the unexpected event status because replay consumers depend on it.
	}

	payload := decodeEventPayload(t, events[0].Payload) // Decode the persisted terminal payload so the test can inspect its failure metadata.
	if payload["terminalStatus"] != "failed" {          // Fail the test when the final event payload does not record the failed terminal status.
		t.Fatalf("expected terminal payload status failed, got %#v", payload["terminalStatus"]) // Surface the unexpected payload field because replay consumers may inspect the payload for terminal metadata.
	}
	if payload["errorCode"] != "E.PLAN.INVALID" { // Fail the test when the final event payload does not preserve the structured plan-validation error code.
		t.Fatalf("expected terminal payload error code E.PLAN.INVALID, got %#v", payload["errorCode"]) // Surface the unexpected error code because operators rely on it for diagnosis.
	}
}

// TestHandleDispatchErrorFinalizesAfterRetriesExhausted verifies that retry exhaustion closes a queued trace with one failed terminal state and one final replayable event before ACK.
func TestHandleDispatchErrorFinalizesAfterRetriesExhausted(t *testing.T) {
	harness := newTraceTerminalHarness(t)     // Start one isolated orchestrator test harness so this test can exercise the real dispatcher terminalization path.
	_, traceID := harness.seedPendingTrace(t) // Seed one pending trace because dispatcher retry exhaustion must start from an active queued lifecycle state.

	dispatchErr := errors.New(errors.CodeSchedNoWorker, "no available worker") // Construct the same transient scheduling error used when no worker is currently assignable.
	for attempt := 1; attempt < maxRetries; attempt++ {                        // Drive the dispatcher through every transient retry before the configured exhaustion threshold.
		harness.service.dispatcher.handleDispatchError(context.Background(), "queue:shard:0", "1-0", traceID, dispatchErr) // Reuse the production retry handler so the test covers the same Redis counter and terminalization logic as the live dispatcher.
		trace, events := harness.loadTraceAndEvents(t, traceID)                                                            // Read back the persisted trace state after each non-terminal retry attempt.
		if trace.Status != "pending" {                                                                                     // Fail the test when a non-exhausted retry attempt terminalizes the trace too early.
			t.Fatalf("expected trace status pending before retry exhaustion, got %s", trace.Status) // Surface the premature terminalization because the dispatcher should only close the trace once retries are exhausted.
		}
		if len(events) != 0 { // Fail the test when a non-exhausted retry attempt appends a terminal event too early.
			t.Fatalf("expected no terminal events before retry exhaustion, got %d", len(events)) // Surface the premature event emission because task 4 requires terminal events only once the trace is actually closed.
		}
	}

	harness.service.dispatcher.handleDispatchError(context.Background(), "queue:shard:0", "1-0", traceID, dispatchErr) // Trigger the final retry attempt so the production dispatcher closes the trace before ACK.
	trace, events := harness.loadTraceAndEvents(t, traceID)                                                            // Read back the persisted trace row and replayable events after retry exhaustion.
	if trace.Status != "failed" {                                                                                      // Fail the test when retry exhaustion did not persist the failed terminal state.
		t.Fatalf("expected trace status failed after retry exhaustion, got %s", trace.Status) // Surface the unexpected trace state because task 4 requires a durable terminal failure state.
	}
	if len(events) != 1 { // Fail the test when retry exhaustion did not append exactly one final replayable event.
		t.Fatalf("expected 1 terminal event after retry exhaustion, got %d", len(events)) // Surface the unexpected event count because task 4 requires one durable terminal marker.
	}

	payload := decodeEventPayload(t, events[0].Payload) // Decode the persisted terminal payload so the test can inspect the retry-exhaustion metadata.
	if payload["retryCount"] != float64(maxRetries) {   // Fail the test when the final event payload does not preserve the exhausted retry count.
		t.Fatalf("expected retryCount %d, got %#v", maxRetries, payload["retryCount"]) // Surface the unexpected retry count because operators rely on it for diagnosis.
	}
	if payload["terminalStatus"] != "failed" { // Fail the test when the final event payload does not record the failed terminal status.
		t.Fatalf("expected terminal payload status failed, got %#v", payload["terminalStatus"]) // Surface the unexpected payload field because replay consumers may inspect the payload for terminal metadata.
	}
}
