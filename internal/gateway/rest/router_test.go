// router_test.go verifies that trace subscription token issuance enforces persisted trace ownership before minting WebSocket credentials.
package rest

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	gatewaywebsocket "mcp_for_appium/internal/gateway/websocket"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
	redisstore "mcp_for_appium/internal/storage/redis"

	"github.com/alicebob/miniredis/v2"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	routerTestDatabaseName = "mcp_mobile_worker" // routerTestDatabaseName keeps the embedded PostgreSQL database name aligned with repository defaults.
	routerTestDatabaseUser = "postgres"          // routerTestDatabaseUser defines the superuser name used by the embedded PostgreSQL test instance.
	routerTestDatabasePass = "postgres"          // routerTestDatabasePass defines the password paired with the embedded PostgreSQL test superuser.
	routerTestDatabaseHost = "127.0.0.1"         // routerTestDatabaseHost keeps the embedded PostgreSQL test instance bound to IPv4 loopback only.
)

// traceSubscribeHarness owns the isolated PostgreSQL, Redis, orchestrator service, and REST router used by one trace-subscription authorization test.
type traceSubscribeHarness struct {
	dao    *postgres.DAO
	cache  *redisstore.Cache
	router *Router
}

// newTraceSubscribeHarness starts isolated PostgreSQL and Redis dependencies, applies migrations, and constructs a REST router ready for trace-subscription authorization tests.
func newTraceSubscribeHarness(t *testing.T) *traceSubscribeHarness {
	ctx := context.Background()                                                        // Use one shared background context because the harness setup runs synchronously inside the current test.
	port := reserveTraceSubscribeLoopbackPort(t)                                       // Reserve one loopback TCP port so the embedded PostgreSQL instance can bind without colliding with developer-managed services.
	tempDir := t.TempDir()                                                             // Allocate one isolated filesystem root so the embedded PostgreSQL runtime never shares state across tests.
	postgresInstance := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig(). // Construct the embedded PostgreSQL instance with fully isolated paths and credentials for this one test.
												Port(uint32(port)).                               // Bind the embedded database to the reserved loopback port for this isolated test instance.
												Database(routerTestDatabaseName).                 // Create the same database name that the repository examples and DAO defaults already use.
												Username(routerTestDatabaseUser).                 // Create the same superuser name used by the repository examples and migration replay tooling.
												Password(routerTestDatabasePass).                 // Create the matching superuser password for the isolated test instance.
												RuntimePath(filepath.Join(tempDir, "runtime")).   // Store the PostgreSQL runtime files under the test temp directory so nothing leaks into the developer environment.
												DataPath(filepath.Join(tempDir, "data")).         // Store the PostgreSQL data directory under the test temp directory so every test starts from a clean cluster.
												BinariesPath(filepath.Join(tempDir, "binaries"))) // Store downloaded PostgreSQL binaries under the test temp directory so tests remain self-contained.
	if err := postgresInstance.Start(); err != nil { // Stop immediately when the embedded PostgreSQL server cannot start for this test.
		t.Fatalf("failed to start embedded postgres: %v", err) // Surface the startup failure because no authorization assertion can run without a live database.
	}
	t.Cleanup(func() { // Ensure the embedded PostgreSQL instance always stops even when the test fails partway through.
		if err := postgresInstance.Stop(); err != nil { // Attempt to stop the embedded PostgreSQL instance once the test completes.
			t.Fatalf("failed to stop embedded postgres: %v", err) // Surface shutdown failures because they usually indicate a leaked connection or stuck background process.
		}
	})

	dsn := buildTraceSubscribeTestDSN(port)                                                                                           // Build the DAO and migration DSN that points at the isolated embedded PostgreSQL instance.
	applyTraceSubscribeMigrations(t, ctx, dsn)                                                                                        // Apply the committed repository migrations before the DAO touches the fresh embedded database.
	dao, err := postgres.NewDAO(ctx, config.PostgresConfig{DSN: dsn, MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute}) // Open the production DAO against the live embedded database so the test exercises real persistence code.
	if err != nil {                                                                                                                   // Stop immediately when the DAO cannot connect to the isolated embedded database.
		t.Fatalf("failed to create dao: %v", err) // Surface the DAO construction failure because the authorization assertions cannot proceed without it.
	}
	t.Cleanup(func() { dao.Close() }) // Ensure the DAO pool closes before the embedded PostgreSQL instance shuts down.

	redisServer := miniredis.RunT(t)                                                // Start one in-memory Redis server so the subscription-token store uses real Redis commands.
	cache, err := redisstore.NewCache(config.RedisConfig{Addr: redisServer.Addr()}) // Open the production Redis cache wrapper against the isolated in-memory Redis server.
	if err != nil {                                                                 // Stop immediately when the Redis cache wrapper cannot connect to the isolated in-memory Redis server.
		t.Fatalf("failed to create redis cache: %v", err) // Surface the Redis setup failure because subscription token issuance depends on cache operations.
	}
	t.Cleanup(func() { // Ensure the Redis client and server always stop even when the test fails partway through.
		if err := cache.Close(); err != nil { // Close the production cache wrapper before the in-memory Redis server is torn down.
			t.Fatalf("failed to close redis cache: %v", err) // Surface cache-close failures because they usually indicate leaked resources in the test path.
		}
		redisServer.Close() // Stop the in-memory Redis server after the cache wrapper has released its connection.
	})

	orchSvc := orchestrator.NewService(config.OrchestratorConfig{PlanTimeout: time.Minute, StepTimeout: 5 * time.Second, AutoWaitMax: 500 * time.Millisecond, SnapshotTTL: time.Minute, ExecutionMode: orchestrator.ExecutionModeMonolith}, config.RPCSecurityConfig{}, config.WorkerConfig{AppiumURL: "http://127.0.0.1:4723"}, config.AWSConfig{}, config.DeviceFarmConfig{Mode: "disabled"}, dao, cache, nil) // Construct the production orchestrator service with the minimum config required for trace reads.
	router := NewRouter(orchSvc, cache, nil)                                                                                                                                                                                                                                                                                                                                                                     // Construct the production REST router around the real orchestrator service so subscription issuance runs through the live trace-read path.
	router.SetSubscriptionTokenStore(gatewaywebsocket.NewSubscriptionTokenStore(cache, time.Minute))                                                                                                                                                                                                                                                                                                             // Inject the production subscription-token store so authorized callers receive real Redis-backed tokens.

	return &traceSubscribeHarness{dao: dao, cache: cache, router: router} // Return the fully wired harness so individual tests can seed traces and hit the real subscription endpoint.
}

// reserveTraceSubscribeLoopbackPort reserves one IPv4 loopback TCP port and returns it so embedded PostgreSQL can bind deterministically during tests.
func reserveTraceSubscribeLoopbackPort(t *testing.T) int {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Ask the kernel for one unused IPv4 loopback port so the embedded database does not collide with other local services.
	if err != nil {                                    // Stop immediately when even loopback port reservation fails in the current environment.
		t.Fatalf("failed to reserve loopback port: %v", err) // Surface the port-reservation failure because the embedded database cannot start without a free port.
	}
	defer listener.Close() // Release the reserved port immediately so embedded PostgreSQL can bind it during the next setup step.

	return listener.Addr().(*net.TCPAddr).Port // Return the kernel-selected free TCP port for the embedded PostgreSQL instance.
}

// buildTraceSubscribeTestDSN returns the PostgreSQL DSN used by the embedded-database-backed router tests.
func buildTraceSubscribeTestDSN(port int) string {
	return "postgres://" + routerTestDatabaseUser + ":" + routerTestDatabasePass + "@" + routerTestDatabaseHost + ":" + strconv.Itoa(port) + "/" + routerTestDatabaseName + "?sslmode=disable" // Build one DSN string that points at the isolated embedded PostgreSQL test instance.
}

// applyTraceSubscribeMigrations replays every committed PostgreSQL migration against the embedded database used by the current test.
func applyTraceSubscribeMigrations(t *testing.T, ctx context.Context, dsn string) {
	conn, err := pgx.Connect(ctx, dsn) // Open one direct PostgreSQL connection used only for migration replay inside the current test setup.
	if err != nil {                    // Stop immediately when the embedded database cannot accept a direct pgx connection.
		t.Fatalf("failed to connect for migration replay: %v", err) // Surface the connection failure because the schema cannot be prepared without it.
	}
	defer conn.Close(ctx) // Release the direct migration connection before the DAO pool starts using the embedded database.

	migrationFiles, err := filepath.Glob(filepath.Join(traceSubscribeRepositoryRoot(t), "internal", "storage", "postgres", "migrations", "*.sql")) // Resolve every committed migration file shipped in the repository.
	if err != nil {                                                                                                                                // Stop immediately when the filesystem glob itself fails.
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

// traceSubscribeRepositoryRoot returns the repository root directory by walking upward from this test file's location.
func traceSubscribeRepositoryRoot(t *testing.T) string {
	_, file, _, ok := runtime.Caller(0) // Read the current test file path so the repository root can be derived without depending on the process working directory.
	if !ok {                            // Stop immediately when the runtime cannot determine the current file path.
		t.Fatal("failed to resolve current test file path") // Surface the missing runtime caller metadata because migration discovery depends on it.
	}

	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(file)))) // Walk from internal/gateway/rest/router_test.go back up to the repository root directory.
}

// seedOwnedTrace inserts one owned active session plus one owned pending trace that the subscription endpoint can authorize against.
func (h *traceSubscribeHarness) seedOwnedTrace(t *testing.T, tenantID string, subjectID string) string {
	ctx := context.Background()   // Use one shared background context because the seed writes run synchronously inside the current test.
	now := time.Now().UTC()       // Capture one stable timestamp so the related session and trace rows stay chronologically consistent.
	sessionID := uuid.NewString() // Generate one unique session identifier so concurrent or repeated test runs never collide on primary keys.
	traceID := uuid.NewString()   // Generate one unique trace identifier linked to the seeded session.

	if err := h.dao.CreateSession(ctx, &postgres.Session{ID: sessionID, ProjectID: "test-project", TenantID: tenantID, SubjectID: subjectID, Status: "created", Capabilities: []byte(`{"platformName":"Android"}`), CreatedAt: now, UpdatedAt: now}); err != nil { // Insert one owned session row because trace ownership should inherit from it.
		t.Fatalf("failed to seed owned session: %v", err) // Surface the seed failure because the subscription test cannot run without the owned session row.
	}
	if err := h.dao.CreateTrace(ctx, &postgres.Trace{ID: traceID, SessionID: sessionID, ProjectID: "test-project", TenantID: tenantID, SubjectID: subjectID, Status: "pending", CreatedAt: now, UpdatedAt: now}); err != nil { // Insert one owned trace row because the subscription endpoint authorizes against persisted trace ownership.
		t.Fatalf("failed to seed owned trace: %v", err) // Surface the seed failure because the subscription test cannot run without the owned trace row.
	}

	return traceID // Return the seeded trace identifier so the test can call the subscription endpoint under test.
}

// TestHandleTraceSubscribeRejectsUnauthorizedTraceOwner verifies that the REST subscription-token issuance endpoint denies authenticated callers that do not own the requested trace.
func TestHandleTraceSubscribeRejectsUnauthorizedTraceOwner(t *testing.T) {
	harness := newTraceSubscribeHarness(t)                                                                                                    // Start one isolated router harness so this test can exercise the real trace-subscription authorization path.
	traceID := harness.seedOwnedTrace(t, "tenant-a", "subject-owner")                                                                         // Seed one trace owned by a different subject so the endpoint must reject the mismatched caller.
	request := httptest.NewRequest(http.MethodPost, "/api/v1/traces/"+traceID+":subscribe", nil)                                              // Build one subscription-token issuance request that targets the owned trace.
	request = request.WithContext(auth.WithSubject(request.Context(), &auth.Subject{ID: "subject-other", TenantID: "tenant-a", Type: "pat"})) // Attach a different authenticated subject from the same tenant so the exact subject-ownership rule is exercised.
	recorder := httptest.NewRecorder()                                                                                                        // Capture the HTTP response generated by the production subscription endpoint.

	harness.router.handleTraceSubscribe(recorder, request, traceID) // Execute the production trace-subscription endpoint against the unauthorized caller.

	if recorder.Code != http.StatusForbidden { // Fail the test when the endpoint does not map the ownership failure to HTTP 403.
		t.Fatalf("expected forbidden status, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected status and body so routing regressions are easy to diagnose.
	}

	var payload map[string]map[string]interface{}                           // Allocate the destination map used to inspect the structured REST error payload.
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil { // Decode the JSON response so the test can assert the stable internal error code.
		t.Fatalf("expected JSON error payload, got error: %v", err) // Surface unexpected non-JSON responses because clients depend on the structured error envelope.
	}
	if payload["error"]["internalCode"] != string(errors.CodePermissionDenied) { // Fail when the endpoint does not preserve the stable permission-denied internal code.
		t.Fatalf("expected permission-denied internal code, got %#v", payload["error"]["internalCode"]) // Surface the unexpected internal code because clients rely on it for machine handling.
	}
}

// TestHandleTraceSubscribeIssuesTokenForAuthorizedOwner verifies that the REST subscription-token issuance endpoint mints one short-lived token for the authenticated trace owner.
func TestHandleTraceSubscribeIssuesTokenForAuthorizedOwner(t *testing.T) {
	harness := newTraceSubscribeHarness(t)                                                                                                    // Start one isolated router harness so this test can exercise the real trace-subscription issuance path.
	traceID := harness.seedOwnedTrace(t, "tenant-a", "subject-owner")                                                                         // Seed one trace owned by the same subject that will request the subscription token.
	request := httptest.NewRequest(http.MethodPost, "/api/v1/traces/"+traceID+":subscribe", nil)                                              // Build one subscription-token issuance request that targets the owned trace.
	request = request.WithContext(auth.WithSubject(request.Context(), &auth.Subject{ID: "subject-owner", TenantID: "tenant-a", Type: "pat"})) // Attach the matching authenticated subject so the endpoint should authorize the request.
	recorder := httptest.NewRecorder()                                                                                                        // Capture the HTTP response generated by the production subscription endpoint.

	harness.router.handleTraceSubscribe(recorder, request, traceID) // Execute the production trace-subscription endpoint against the authorized caller.

	if recorder.Code != http.StatusCreated { // Fail the test when the endpoint does not issue a subscription token for the authorized trace owner.
		t.Fatalf("expected created status, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected status and body so issuance regressions are easy to diagnose.
	}

	var payload map[string]interface{}                                      // Allocate the destination map used to inspect the structured REST success payload.
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil { // Decode the JSON response so the test can assert the returned token fields.
		t.Fatalf("expected JSON success payload, got error: %v", err) // Surface unexpected non-JSON responses because browser clients depend on the structured success envelope.
	}
	if payload["traceId"] != traceID { // Fail when the endpoint does not echo the authorized trace binding back to the caller.
		t.Fatalf("expected traceId %s, got %#v", traceID, payload["traceId"]) // Surface the unexpected trace binding because the issued token must remain trace-scoped.
	}
	if tokenValue, _ := payload["subscriptionToken"].(string); tokenValue == "" { // Fail when the endpoint does not return a usable opaque token for the authorized caller.
		t.Fatalf("expected non-empty subscription token, got %#v", payload["subscriptionToken"]) // Surface the unexpected token payload because the browser cannot establish the WebSocket handshake without it.
	}
}
