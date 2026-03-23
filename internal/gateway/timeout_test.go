// timeout_test.go verifies that gateway JSON-RPC session-start requests fail fast when Appium accepts the request but never responds.
package gateway

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
	"strings"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	internalerrors "mcp_for_appium/internal/errors"
	gatewayjsonrpc "mcp_for_appium/internal/gateway/jsonrpc"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
	redisstore "mcp_for_appium/internal/storage/redis"

	"github.com/alicebob/miniredis/v2"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
)

const (
	gatewayTestDatabaseName = "mcp_mobile_worker" // gatewayTestDatabaseName keeps the embedded PostgreSQL database name aligned with the repository defaults.
	gatewayTestDatabaseUser = "postgres"          // gatewayTestDatabaseUser defines the superuser name used by the embedded PostgreSQL gateway test instance.
	gatewayTestDatabasePass = "postgres"          // gatewayTestDatabasePass defines the password paired with the embedded PostgreSQL gateway test superuser.
	gatewayTestDatabaseHost = "127.0.0.1"         // gatewayTestDatabaseHost keeps the embedded PostgreSQL gateway test instance bound to IPv4 loopback only.
)

// gatewayTimeoutHarness owns the isolated PostgreSQL, Redis, Appium stub, and orchestrator service used by one gateway timeout test.
type gatewayTimeoutHarness struct {
	service        *orchestrator.Service
	cache          *redisstore.Cache
	stopPostgres   func()
	stopRedis      func()
	stopAppiumStub func()
}

// newGatewayTimeoutHarness starts isolated dependencies and constructs an orchestrator service that points at one intentionally unresponsive Appium stub.
func newGatewayTimeoutHarness(t *testing.T) *gatewayTimeoutHarness {
	ctx := context.Background()                                                                                                    // Use one shared background context because the harness setup runs synchronously inside the current test.
	appiumURL, stopAppiumStub := startGatewayLoopbackHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Start one explicit IPv4 loopback HTTP server that accepts Appium requests but never writes a response body.
		<-r.Context().Done() // Hold the Appium request open until the caller context is canceled so timeout propagation can be asserted deterministically.
	})) // Close the handler literal so the Appium stub server can start serving immediately.
	t.Cleanup(stopAppiumStub) // Ensure the temporary Appium stub server is always stopped after the test completes.

	postgresPort := reserveGatewayLoopbackPort(t)                                      // Reserve one loopback TCP port so the embedded PostgreSQL instance can bind without colliding with developer-managed services.
	tempDir := t.TempDir()                                                             // Allocate one isolated filesystem root so the embedded PostgreSQL runtime never shares state across tests.
	postgresInstance := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig(). // Construct the embedded PostgreSQL instance with fully isolated paths and credentials for this one test.
												Port(uint32(postgresPort)).                       // Bind the embedded database to the reserved loopback port for this isolated test instance.
												Database(gatewayTestDatabaseName).                // Create the same database name that the repository examples and DAO defaults already use.
												Username(gatewayTestDatabaseUser).                // Create the same superuser name used by the repository examples and migration replay tooling.
												Password(gatewayTestDatabasePass).                // Create the matching superuser password for the isolated test instance.
												RuntimePath(filepath.Join(tempDir, "runtime")).   // Store the PostgreSQL runtime files under the test temp directory so nothing leaks into the developer environment.
												DataPath(filepath.Join(tempDir, "data")).         // Store the PostgreSQL data directory under the test temp directory so every test starts from a clean cluster.
												BinariesPath(filepath.Join(tempDir, "binaries"))) // Store downloaded PostgreSQL binaries under the test temp directory so tests remain self-contained.
	if err := postgresInstance.Start(); err != nil { // Stop immediately when the embedded PostgreSQL server cannot start for this test.
		t.Fatalf("failed to start embedded postgres: %v", err) // Surface the startup failure because no gateway timeout assertion can run without a live database.
	}
	stopPostgres := func() { // Capture the embedded PostgreSQL shutdown in one closure so test cleanup remains concise and deterministic.
		if err := postgresInstance.Stop(); err != nil { // Attempt to stop the embedded PostgreSQL instance once the test completes.
			t.Fatalf("failed to stop embedded postgres: %v", err) // Surface shutdown failures because they usually indicate a leaked connection or stuck background process.
		}
	}
	t.Cleanup(stopPostgres) // Ensure the embedded PostgreSQL instance always stops even when the test fails partway through.

	dsn := buildGatewayTestDSN(postgresPort) // Build the DAO and migration DSN that points at the isolated embedded PostgreSQL instance.
	applyGatewayMigrations(t, ctx, dsn)      // Apply the committed repository migrations before the DAO touches the fresh embedded database.

	dao, err := postgres.NewDAO(ctx, config.PostgresConfig{DSN: dsn, MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute}) // Open the production DAO against the live embedded database so the gateway tests exercise real persistence code.
	if err != nil {                                                                                                                   // Stop immediately when the DAO cannot connect to the isolated embedded database.
		t.Fatalf("failed to create dao: %v", err) // Surface the DAO construction failure because none of the gateway timeout assertions can proceed without it.
	}
	t.Cleanup(func() { dao.Close() }) // Ensure the DAO pool closes before the embedded PostgreSQL instance shuts down.

	redisServer := miniredis.RunT(t)                                                // Start one in-memory Redis server so gateway and orchestrator cache calls use real Redis commands.
	cache, err := redisstore.NewCache(config.RedisConfig{Addr: redisServer.Addr()}) // Open the production Redis cache wrapper against the isolated in-memory Redis server.
	if err != nil {                                                                 // Stop immediately when the Redis cache wrapper cannot connect to the isolated in-memory Redis server.
		t.Fatalf("failed to create redis cache: %v", err) // Surface the Redis setup failure because the gateway tests exercise real orchestrator cache calls.
	}
	stopRedis := func() { // Capture the Redis shutdown in one closure so test cleanup remains concise and deterministic.
		if err := cache.Close(); err != nil { // Close the production cache wrapper before the in-memory Redis server is torn down.
			t.Fatalf("failed to close redis cache: %v", err) // Surface cache-close failures because they usually indicate leaked resources in the test path.
		}
		redisServer.Close() // Stop the in-memory Redis server after the cache wrapper has released its connection.
	}
	t.Cleanup(stopRedis) // Ensure the Redis client and server always stop even when the test fails partway through.

	service := orchestrator.NewService(config.OrchestratorConfig{ // Construct the production orchestrator service with the minimum config required for gateway timeout-path tests.
		PlanTimeout:   time.Minute,                        // Use a positive plan timeout because the orchestrator config requires one even though these tests focus on session start.
		StepTimeout:   5 * time.Second,                    // Use a positive per-step timeout because the orchestrator config requires one even though these tests focus on session start.
		AutoWaitMax:   500 * time.Millisecond,             // Use a small positive auto-wait value because the orchestrator config requires one even though these tests focus on session start.
		SnapshotTTL:   time.Minute,                        // Use a positive snapshot TTL because the orchestrator config expects one even though these tests do not touch snapshots.
		ExecutionMode: orchestrator.ExecutionModeMonolith, // Exercise the monolith service path that the gateway embeds directly today.
	}, config.RPCSecurityConfig{}, config.WorkerConfig{
		AppiumURL: appiumURL, // Point the service at the intentionally unresponsive Appium stub so request timeout propagation can be asserted.
	}, config.AWSConfig{}, config.DeviceFarmConfig{Mode: "disabled"}, dao, cache, nil) // Disable Device Farm and S3 because these gateway timeout tests do not touch those integrations.

	return &gatewayTimeoutHarness{service: service, cache: cache, stopPostgres: stopPostgres, stopRedis: stopRedis, stopAppiumStub: stopAppiumStub} // Return the fully wired harness so individual tests can exercise JSON-RPC timeout behavior.
}

// reserveGatewayLoopbackPort reserves one IPv4 loopback TCP port and returns it so embedded PostgreSQL can bind deterministically during gateway tests.
func reserveGatewayLoopbackPort(t *testing.T) int {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Ask the kernel for one unused IPv4 loopback port so the embedded database does not collide with other local services.
	if err != nil {                                    // Stop immediately when even loopback port reservation fails in the current environment.
		t.Fatalf("failed to reserve loopback port: %v", err) // Surface the port-reservation failure because the embedded database cannot start without a free port.
	}
	defer listener.Close() // Release the reserved port immediately so embedded PostgreSQL can bind it during the next setup step.

	return listener.Addr().(*net.TCPAddr).Port // Return the kernel-selected free TCP port for the embedded PostgreSQL instance.
}

// startGatewayLoopbackHTTPServer starts a minimal IPv4 loopback HTTP server for gateway timeout tests and returns its base URL plus a shutdown function.
func startGatewayLoopbackHTTPServer(t *testing.T, handler http.Handler) (string, func()) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind an explicit IPv4 loopback listener because the sandbox blocks the default IPv6 httptest server listener.
	if err != nil {                                    // Skip the current test when even explicit IPv4 loopback listeners are not permitted in the environment.
		t.Skipf("skipping gateway loopback timeout test because listen failed: %v", err) // Surface the environment limitation without misclassifying the production code as passing.
		return "", func() {}                                                             // Return zero values after Skipf so the compiler sees a complete return path.
	}
	server := &http.Server{Handler: handler} // Construct a minimal HTTP server that serves only the handler required by the current timeout test.
	go func() {
		_ = server.Serve(listener) // Serve requests in the background so the orchestrator can hit the loopback Appium stub during the test.
	}()
	return "http://" + listener.Addr().String(), func() { // Return the bound base URL together with a shutdown closure for the caller.
		_ = server.Close()   // Stop the HTTP server once the gateway timeout test completes.
		_ = listener.Close() // Close the listener explicitly so the loopback port is released immediately.
	}
}

// buildGatewayTestDSN returns the PostgreSQL DSN used by the embedded-database-backed gateway timeout tests.
func buildGatewayTestDSN(port int) string {
	return "postgres://" + gatewayTestDatabaseUser + ":" + gatewayTestDatabasePass + "@" + gatewayTestDatabaseHost + ":" + strconv.Itoa(port) + "/" + gatewayTestDatabaseName + "?sslmode=disable" // Build one DSN string that points at the isolated embedded PostgreSQL gateway test instance.
}

// applyGatewayMigrations replays every committed PostgreSQL migration against the embedded database used by the current gateway test.
func applyGatewayMigrations(t *testing.T, ctx context.Context, dsn string) {
	conn, err := pgx.Connect(ctx, dsn) // Open one direct PostgreSQL connection used only for migration replay inside the current gateway test setup.
	if err != nil {                    // Stop immediately when the embedded database cannot accept a direct pgx connection.
		t.Fatalf("failed to connect for migration replay: %v", err) // Surface the connection failure because the schema cannot be prepared without it.
	}
	defer conn.Close(ctx) // Release the direct migration connection before the DAO pool starts using the embedded database.

	migrationFiles, err := filepath.Glob(filepath.Join(gatewayRepositoryRoot(t), "internal", "storage", "postgres", "migrations", "*.sql")) // Resolve every committed migration file shipped in the repository.
	if err != nil {                                                                                                                         // Stop immediately when the filesystem glob itself fails.
		t.Fatalf("failed to glob migration files: %v", err) // Surface the glob failure because the test cannot prepare the schema without the migration list.
	}
	sort.Strings(migrationFiles)                   // Replay the migrations in lexical order so the schema matches normal startup expectations.
	for _, migrationFile := range migrationFiles { // Apply every committed migration file to the isolated embedded database before the test exercises gateway handlers.
		sqlBytes, readErr := os.ReadFile(migrationFile) // Read the exact migration SQL committed in the repository working tree.
		if readErr != nil {                             // Stop immediately when a committed migration file cannot be read.
			t.Fatalf("failed to read migration file %s: %v", migrationFile, readErr) // Surface the unreadable file path because the schema setup cannot continue without it.
		}
		if _, execErr := conn.Exec(ctx, string(sqlBytes)); execErr != nil { // Execute the migration SQL exactly as committed against the live embedded database.
			t.Fatalf("failed to apply migration %s: %v", filepath.Base(migrationFile), execErr) // Surface the failing migration name because the schema setup depends on every migration succeeding.
		}
	}
}

// gatewayRepositoryRoot returns the repository root directory by walking upward from this test file's location.
func gatewayRepositoryRoot(t *testing.T) string {
	_, file, _, ok := runtime.Caller(0) // Read the current test file path so the repository root can be derived without depending on the process working directory.
	if !ok {                            // Stop immediately when the runtime cannot determine the current file path.
		t.Fatal("failed to resolve current test file path") // Surface the missing runtime caller metadata because migration discovery depends on it.
	}

	return filepath.Dir(filepath.Dir(filepath.Dir(file))) // Walk from internal/gateway/timeout_test.go back up to the repository root directory.
}

// TestJSONRPCStartSessionRespectsRequestTimeout verifies that the JSON-RPC start-session endpoint returns promptly with the Appium timeout code in its error data when Appium never responds.
func TestJSONRPCStartSessionRespectsRequestTimeout(t *testing.T) {
	harness := newGatewayTimeoutHarness(t)                     // Start one isolated gateway timeout harness so this test can exercise the real JSON-RPC handler path.
	handler := gatewayjsonrpc.NewHandler(harness.service, nil) // Construct the production JSON-RPC handler against the isolated orchestrator service.

	requestBody := []byte(`{"jsonrpc":"2.0","method":"startSession","params":{"projectId":"gateway-timeout-project","w3cCapsJson":{"platformName":"Android"}},"id":1}`) // Build one valid JSON-RPC start-session request body so the only failure comes from the hanging Appium dependency.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)                                                                                      // Bound the JSON-RPC request context so the hanging Appium dependency must surface as a prompt timeout.
	defer cancel()                                                                                                                                                      // Release the timeout resources once the JSON-RPC handler returns.
	request := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader(string(requestBody))).WithContext(ctx)                                                // Build the production JSON-RPC HTTP request using the short-lived timeout context.
	recorder := httptest.NewRecorder()                                                                                                                                  // Capture the production JSON-RPC response so the test can assert the mapped error payload.

	startedAt := time.Now()              // Capture the handler start time so the test can verify the JSON-RPC layer fails promptly.
	handler.ServeHTTP(recorder, request) // Invoke the production JSON-RPC handler path against the intentionally unresponsive Appium dependency.
	elapsed := time.Since(startedAt)     // Measure the wall-clock duration so the test can verify fail-fast timeout handling.
	if recorder.Code != http.StatusOK {  // Fail the test when the JSON-RPC transport does not return its normal HTTP 200 envelope.
		t.Fatalf("expected HTTP 200 transport status for JSON-RPC error response, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected transport status because JSON-RPC errors should stay inside the response body.
	}
	if elapsed >= time.Second { // Fail the test when the JSON-RPC endpoint hangs far beyond its timeout budget instead of failing promptly.
		t.Fatalf("expected JSON-RPC startSession to fail fast, took %v", elapsed) // Surface the slow failure because unresponsive dependencies must not stall the gateway indefinitely.
	}

	var payload struct {
		Error struct {
			Code int `json:"code"`
			Data struct {
				InternalCode string `json:"internalCode"`
				RawError     string `json:"rawError"`
			} `json:"data"`
		} `json:"error"`
	} // Hold the decoded JSON-RPC error envelope so the test can assert the propagated internal error classification.
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil { // Decode the JSON-RPC error body into a lightweight assertion struct.
		t.Fatalf("failed to decode JSON-RPC error payload: %v", err) // Surface payload-decode failures because they would hide the actual timeout response semantics.
	}
	if payload.Error.Code != -32603 { // Fail the test when the JSON-RPC layer does not map the Appium timeout into its current internal-error bucket.
		t.Fatalf("expected JSON-RPC error code -32603 for timeout, got %d", payload.Error.Code) // Surface the unexpected JSON-RPC code because clients depend on its transport-level category.
	}
	if payload.Error.Data.InternalCode != string(internalerrors.CodeAppTimeout) { // Fail the test when the JSON-RPC error data does not preserve the Appium timeout code.
		t.Fatalf("expected JSON-RPC internal code %s, got %s", internalerrors.CodeAppTimeout, payload.Error.Data.InternalCode) // Surface the unexpected code because clients rely on it for retry and diagnostics.
	}
}
