// monolith_flow_test.go verifies one end-to-end monolith execution flow against embedded PostgreSQL, real Redis commands, and a fake Appium HTTP server.
package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
	redisstore "mcp_for_appium/internal/storage/redis"

	"github.com/alicebob/miniredis/v2"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
)

const (
	integrationDatabaseName = "mcp_mobile_worker" // integrationDatabaseName keeps the embedded PostgreSQL database name aligned with repository defaults.
	integrationDatabaseUser = "postgres"          // integrationDatabaseUser defines the superuser name used by the embedded PostgreSQL integration instance.
	integrationDatabasePass = "postgres"          // integrationDatabasePass defines the password paired with the embedded PostgreSQL integration superuser.
	integrationDatabaseHost = "127.0.0.1"         // integrationDatabaseHost keeps the embedded PostgreSQL integration instance bound to IPv4 loopback only.
)

// fakeAppiumServer records HTTP-level interactions made by the production Appium client during the monolith integration flow.
type fakeAppiumServer struct {
	mu              sync.Mutex
	listener        net.Listener
	server          *http.Server
	baseURL         string
	sessionID       string
	recordedCaps    map[string]interface{}
	foundSelectors  []string
	clickedElements []string
	sendKeysTexts   []string
	deletedSessions []string
	findElementID   string
}

// newFakeAppiumServer starts one IPv4 loopback HTTP server that implements the minimal Appium endpoints required by the monolith integration flow.
func newFakeAppiumServer(t *testing.T) *fakeAppiumServer {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind one explicit IPv4 loopback listener so the fake Appium server works in restricted environments too.
	if err != nil {                                    // Skip the current integration test when loopback listeners are unavailable in the environment.
		t.Skipf("skipping monolith integration test because fake appium listen failed: %v", err) // Surface the environment limitation without misclassifying the integration flow as passing.
	}

	fakeServer := &fakeAppiumServer{listener: listener, sessionID: "appium-session-1", findElementID: "element-123"} // Construct the fake Appium server state with deterministic identifiers for stable assertions.
	mux := http.NewServeMux()                                                                                        // Allocate one dedicated HTTP mux so the fake Appium server can register only the endpoints needed by the integration flow.
	mux.HandleFunc("/session", fakeServer.handleSessionRoot)                                                         // Register the root session endpoint used by session creation.
	mux.HandleFunc("/session/", fakeServer.handleSessionScoped)                                                      // Register the scoped session endpoint used by find, click, sendKeys, screenshot, and session deletion.
	fakeServer.server = &http.Server{Handler: mux}                                                                   // Construct the HTTP server over the fake Appium handler set so the production client can talk to it.
	go func() {
		_ = fakeServer.server.Serve(listener) // Serve fake Appium requests in the background so the integration flow can exercise real HTTP calls.
	}()
	fakeServer.baseURL = "http://" + listener.Addr().String() // Publish the loopback base URL used by the production Appium client under test.
	t.Cleanup(func() {
		fakeServer.Close() // Ensure the fake Appium server always shuts down after the integration test completes.
	})

	return fakeServer // Return the running fake Appium server so the integration harness can wire it into the orchestrator service and assert side effects.
}

// handleSessionRoot serves the Appium /session endpoint used by production session creation.
func (f *fakeAppiumServer) handleSessionRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { // Reject unsupported methods because the integration flow only expects POST /session here.
		w.WriteHeader(http.StatusMethodNotAllowed) // Return 405 so unexpected transport behavior becomes obvious during integration failures.
		return                                     // Stop handling once the unsupported method has been rejected.
	}

	var payload struct {
		Capabilities struct {
			AlwaysMatch map[string]interface{} `json:"alwaysMatch"`
		} `json:"capabilities"`
	} // Decode only the alwaysMatch payload used by the production Appium StartSession client path.
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil { // Decode the production session-creation request body so capabilities can be asserted later.
		http.Error(w, err.Error(), http.StatusBadRequest) // Return one explicit 400 response so malformed production requests fail clearly.
		return                                            // Stop handling once the malformed request body has been rejected.
	}

	f.mu.Lock()                                                 // Protect the fake Appium server state because the HTTP handler can be called concurrently.
	f.recordedCaps = payload.Capabilities.AlwaysMatch           // Record the session capabilities exactly as the production client sent them.
	f.mu.Unlock()                                               // Release the fake Appium server state lock before writing the HTTP response.
	writeJSONResponse(w, http.StatusOK, map[string]interface{}{ // Return one W3C-shaped session response so the production Appium client can extract the session identifier.
		"value": map[string]interface{}{"sessionId": f.sessionID}, // Embed the deterministic fake Appium session identifier under value.sessionId to match production parsing logic.
	})
}

// handleSessionScoped serves the fake Appium endpoints nested under /session/{id}/... that the monolith integration flow exercises.
func (f *fakeAppiumServer) handleSessionScoped(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/session/")       // Strip the shared prefix so the remaining path can be matched against minimal fake Appium routes.
	parts := strings.Split(path, "/")                         // Split the session-scoped path into segments so the fake server can route by endpoint shape.
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" { // Reject malformed session-scoped routes because every valid request must include one session identifier.
		http.Error(w, "missing session id", http.StatusBadRequest) // Return one explicit 400 response so malformed production requests fail clearly.
		return                                                     // Stop handling once the malformed session-scoped path has been rejected.
	}
	sessionID := parts[0]                                 // Read the scoped session identifier so delete requests can be recorded precisely.
	if len(parts) == 1 && r.Method == http.MethodDelete { // Handle DELETE /session/{id} used by the production session teardown path.
		f.mu.Lock()                                                                                    // Protect the fake Appium server state because teardown can race with later assertions.
		f.deletedSessions = append(f.deletedSessions, sessionID)                                       // Record the deleted session identifier so EndSession behavior can be asserted later.
		f.mu.Unlock()                                                                                  // Release the fake Appium server state lock before writing the HTTP response.
		writeJSONResponse(w, http.StatusOK, map[string]interface{}{"value": map[string]interface{}{}}) // Return one successful empty-value W3C response for session deletion.
		return                                                                                         // Stop handling once the delete-session response has been written.
	}
	if len(parts) == 2 && parts[1] == "element" && r.Method == http.MethodPost { // Handle POST /session/{id}/element used by the production FindElement client path.
		var payload struct {
			Using string `json:"using"`
			Value string `json:"value"`
		} // Decode only the fields used by the production Appium FindElement client path.
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil { // Decode the production find-element request body so selector details can be recorded precisely.
			http.Error(w, err.Error(), http.StatusBadRequest) // Return one explicit 400 response so malformed production requests fail clearly.
			return                                            // Stop handling once the malformed find-element payload has been rejected.
		}
		f.mu.Lock()                                                                                                                                     // Protect the fake Appium server state because find-element requests can be called repeatedly by auto-wait logic.
		f.foundSelectors = append(f.foundSelectors, payload.Using+":"+payload.Value)                                                                    // Record the lookup strategy and selector so the executed plan can be asserted later.
		f.mu.Unlock()                                                                                                                                   // Release the fake Appium server state lock before writing the HTTP response.
		writeJSONResponse(w, http.StatusOK, map[string]interface{}{"value": map[string]string{"element-6066-11e4-a52e-4f735466cecf": f.findElementID}}) // Return one W3C element response so the production Appium client can extract the element identifier.
		return                                                                                                                                          // Stop handling once the find-element response has been written.
	}
	if len(parts) == 4 && parts[1] == "element" && parts[3] == "click" && r.Method == http.MethodPost { // Handle POST /session/{id}/element/{elementId}/click used by the production click path.
		elementID := parts[2]                                                                          // Read the clicked element identifier from the session-scoped path.
		f.mu.Lock()                                                                                    // Protect the fake Appium server state because click requests can race with later assertions.
		f.clickedElements = append(f.clickedElements, elementID)                                       // Record the clicked element identifier so the executed plan can be asserted later.
		f.mu.Unlock()                                                                                  // Release the fake Appium server state lock before writing the HTTP response.
		writeJSONResponse(w, http.StatusOK, map[string]interface{}{"value": map[string]interface{}{}}) // Return one successful empty-value W3C response for the click action.
		return                                                                                         // Stop handling once the click response has been written.
	}
	if len(parts) == 4 && parts[1] == "element" && parts[3] == "value" && r.Method == http.MethodPost { // Handle POST /session/{id}/element/{elementId}/value used by the production sendKeys path.
		var payload struct {
			Text string `json:"text"`
		} // Decode only the text field used by the production Appium SendKeys client path.
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil { // Decode the production send-keys request body so the entered text can be recorded precisely.
			http.Error(w, err.Error(), http.StatusBadRequest) // Return one explicit 400 response so malformed production requests fail clearly.
			return                                            // Stop handling once the malformed send-keys payload has been rejected.
		}
		f.mu.Lock()                                                                                    // Protect the fake Appium server state because send-keys requests can race with later assertions.
		f.sendKeysTexts = append(f.sendKeysTexts, payload.Text)                                        // Record the entered text so the executed plan can be asserted later.
		f.mu.Unlock()                                                                                  // Release the fake Appium server state lock before writing the HTTP response.
		writeJSONResponse(w, http.StatusOK, map[string]interface{}{"value": map[string]interface{}{}}) // Return one successful empty-value W3C response for the send-keys action.
		return                                                                                         // Stop handling once the send-keys response has been written.
	}
	if len(parts) == 2 && parts[1] == "screenshot" && r.Method == http.MethodGet { // Handle GET /session/{id}/screenshot used only if failure paths attempt artifact capture.
		writeJSONResponse(w, http.StatusOK, map[string]interface{}{"value": base64.StdEncoding.EncodeToString([]byte("fake-screenshot"))}) // Return one valid base64 screenshot payload so unexpected failure-path captures still decode correctly.
		return                                                                                                                             // Stop handling once the screenshot response has been written.
	}
	if len(parts) == 2 && parts[1] == "source" && r.Method == http.MethodGet { // Handle GET /session/{id}/source so snapshot and debug paths can succeed if touched unexpectedly.
		writeJSONResponse(w, http.StatusOK, map[string]interface{}{"value": "<hierarchy/>"}) // Return one minimal page source payload so unexpected source reads still decode correctly.
		return                                                                               // Stop handling once the source response has been written.
	}

	http.NotFound(w, r) // Return one 404 response for any unexpected fake Appium endpoint so integration drift becomes obvious during failures.
}

// Close stops the fake Appium HTTP server and releases its loopback listener.
func (f *fakeAppiumServer) Close() {
	if f.server != nil { // Stop the HTTP server only when the fake Appium server was started successfully.
		_ = f.server.Close() // Close the fake Appium HTTP server so the loopback listener stops accepting new requests.
	}
	if f.listener != nil { // Close the listener only when the fake Appium server successfully bound one loopback socket.
		_ = f.listener.Close() // Release the loopback socket so repeated test runs do not leak listening ports.
	}
}

// monolithFlowHarness owns the isolated PostgreSQL, Redis, fake Appium server, and orchestrator service used by one monolith integration test.
type monolithFlowHarness struct {
	dao     *postgres.DAO
	cache   *redisstore.Cache
	service *orchestrator.Service
	appium  *fakeAppiumServer
	cancel  context.CancelFunc
}

// newMonolithFlowHarness starts isolated dependencies, applies migrations, and constructs a monolith orchestrator service ready for one end-to-end execution flow.
func newMonolithFlowHarness(t *testing.T) *monolithFlowHarness {
	ctx := context.Background()            // Use one shared background context because the harness setup runs synchronously inside the current test.
	appiumServer := newFakeAppiumServer(t) // Start one fake Appium server so the integration flow exercises real HTTP calls without a device farm or simulator.

	postgresPort := reserveIntegrationLoopbackPort(t)                                  // Reserve one loopback TCP port so the embedded PostgreSQL instance can bind without colliding with developer-managed services.
	tempDir := t.TempDir()                                                             // Allocate one isolated filesystem root so the embedded PostgreSQL runtime never shares state across tests.
	postgresInstance := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig(). // Construct the embedded PostgreSQL instance with fully isolated paths and credentials for this one integration test.
												Port(uint32(postgresPort)).                       // Bind the embedded database to the reserved loopback port for this isolated integration instance.
												Database(integrationDatabaseName).                // Create the same database name that the repository examples and DAO defaults already use.
												Username(integrationDatabaseUser).                // Create the same superuser name used by the repository examples and migration replay tooling.
												Password(integrationDatabasePass).                // Create the matching superuser password for the isolated integration instance.
												RuntimePath(filepath.Join(tempDir, "runtime")).   // Store the PostgreSQL runtime files under the test temp directory so nothing leaks into the developer environment.
												DataPath(filepath.Join(tempDir, "data")).         // Store the PostgreSQL data directory under the test temp directory so every integration run starts from a clean cluster.
												BinariesPath(filepath.Join(tempDir, "binaries"))) // Store downloaded PostgreSQL binaries under the test temp directory so the integration harness remains self-contained.
	if err := postgresInstance.Start(); err != nil { // Stop immediately when the embedded PostgreSQL server cannot start for this integration test.
		t.Fatalf("failed to start embedded postgres: %v", err) // Surface the startup failure because no persistence-backed integration assertion can run without a live database.
	}
	t.Cleanup(func() {
		if err := postgresInstance.Stop(); err != nil { // Stop the embedded PostgreSQL instance after the integration test completes.
			t.Fatalf("failed to stop embedded postgres: %v", err) // Surface shutdown failures because they usually indicate leaked connections or stuck background processes.
		}
	})

	dsn := buildIntegrationDSN(postgresPort)                                                                                          // Build the DAO and migration DSN that points at the isolated embedded PostgreSQL instance.
	applyIntegrationMigrations(t, ctx, dsn)                                                                                           // Apply the committed repository migrations before the DAO touches the fresh embedded database.
	dao, err := postgres.NewDAO(ctx, config.PostgresConfig{DSN: dsn, MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute}) // Open the production DAO against the live embedded database so the integration test exercises real persistence code.
	if err != nil {                                                                                                                   // Stop immediately when the DAO cannot connect to the isolated embedded database.
		t.Fatalf("failed to create dao: %v", err) // Surface the DAO construction failure because none of the integration assertions can proceed without it.
	}
	t.Cleanup(func() {
		dao.Close() // Close the DAO pool before the embedded PostgreSQL instance shuts down so connections are released cleanly.
	})

	redisServer := miniredis.RunT(t)                                                // Start one in-memory Redis server so queueing, session mapping, and trace events use real Redis commands.
	cache, err := redisstore.NewCache(config.RedisConfig{Addr: redisServer.Addr()}) // Open the production Redis cache wrapper against the isolated in-memory Redis server.
	if err != nil {                                                                 // Stop immediately when the Redis cache wrapper cannot connect to the isolated in-memory Redis server.
		t.Fatalf("failed to create redis cache: %v", err) // Surface the Redis setup failure because dispatcher execution depends on cache operations.
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil { // Close the production cache wrapper before the in-memory Redis server is torn down.
			t.Fatalf("failed to close redis cache: %v", err) // Surface cache-close failures because they usually indicate leaked resources in the integration path.
		}
		redisServer.Close() // Stop the in-memory Redis server after the cache wrapper has released its connection.
	})

	service := orchestrator.NewService(config.OrchestratorConfig{ // Construct the production monolith orchestrator service with the minimum config required for the integration flow.
		PlanTimeout:   10 * time.Second,                   // Use one bounded plan timeout so the integration flow fails promptly when execution stalls.
		StepTimeout:   3 * time.Second,                    // Use one bounded per-step timeout so fake Appium calls remain prompt and deterministic.
		AutoWaitMax:   500 * time.Millisecond,             // Use one short auto-wait ceiling so missing fake elements fail quickly during integration debugging.
		SnapshotTTL:   time.Minute,                        // Use one positive snapshot TTL because the orchestrator config expects it even though this test does not read snapshots.
		ExecutionMode: orchestrator.ExecutionModeMonolith, // Exercise the monolith path because task 9 requires the embedded gateway/orchestrator execution chain.
	}, config.RPCSecurityConfig{}, config.WorkerConfig{
		AppiumURL: appiumServer.baseURL, // Point the service at the fake Appium loopback server so StartSession and ExecutePlan exercise real HTTP behavior.
	}, config.AWSConfig{}, config.DeviceFarmConfig{Mode: "disabled"}, dao, cache, nil) // Disable Device Farm and S3 because the monolith integration flow does not touch those integrations.

	runCtx, cancel := context.WithCancel(context.Background()) // Create one service lifecycle context so dispatcher and registry loops can be started and later stopped deterministically.
	if err := service.Start(runCtx); err != nil {              // Start the production orchestrator service loops before enqueueing any integration flow plan.
		t.Fatalf("failed to start orchestrator service: %v", err) // Surface startup failures because the integration flow requires a running dispatcher.
	}
	t.Cleanup(func() {
		cancel()       // Cancel the service lifecycle context so background loops begin shutting down.
		service.Stop() // Stop the orchestrator service explicitly so dispatcher and registry goroutines release resources promptly.
	})

	return &monolithFlowHarness{dao: dao, cache: cache, service: service, appium: appiumServer, cancel: cancel} // Return the fully wired harness so the integration test can drive the monolith flow and assert side effects.
}

// reserveIntegrationLoopbackPort reserves one IPv4 loopback TCP port and returns it so embedded PostgreSQL can bind deterministically during integration tests.
func reserveIntegrationLoopbackPort(t *testing.T) int {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Ask the kernel for one unused IPv4 loopback port so the embedded database does not collide with other local services.
	if err != nil {                                    // Stop immediately when even loopback port reservation fails in the current environment.
		t.Fatalf("failed to reserve integration loopback port: %v", err) // Surface the port-reservation failure because the embedded database cannot start without a free port.
	}
	defer listener.Close() // Release the reserved port immediately so embedded PostgreSQL can bind it during the next setup step.

	return listener.Addr().(*net.TCPAddr).Port // Return the kernel-selected free TCP port for the embedded PostgreSQL integration instance.
}

// buildIntegrationDSN returns the PostgreSQL DSN used by the embedded-database-backed monolith integration test.
func buildIntegrationDSN(port int) string {
	return "postgres://" + integrationDatabaseUser + ":" + integrationDatabasePass + "@" + integrationDatabaseHost + ":" + strconv.Itoa(port) + "/" + integrationDatabaseName + "?sslmode=disable" // Build one DSN string that points at the isolated embedded PostgreSQL integration instance.
}

// applyIntegrationMigrations replays every committed PostgreSQL migration against the embedded database used by the current integration test.
func applyIntegrationMigrations(t *testing.T, ctx context.Context, dsn string) {
	conn, err := pgx.Connect(ctx, dsn) // Open one direct PostgreSQL connection used only for migration replay inside the current integration test setup.
	if err != nil {                    // Stop immediately when the embedded database cannot accept a direct pgx connection.
		t.Fatalf("failed to connect for integration migration replay: %v", err) // Surface the connection failure because the schema cannot be prepared without it.
	}
	defer conn.Close(ctx) // Release the direct migration connection before the DAO pool starts using the embedded database.

	migrationFiles, err := filepath.Glob(filepath.Join(integrationRepositoryRoot(t), "internal", "storage", "postgres", "migrations", "*.sql")) // Resolve every committed migration file shipped in the repository.
	if err != nil {                                                                                                                             // Stop immediately when the filesystem glob itself fails.
		t.Fatalf("failed to glob migration files: %v", err) // Surface the glob failure because the integration test cannot prepare the schema without the migration list.
	}
	sort.Strings(migrationFiles)                   // Replay the migrations in lexical order so the schema matches normal startup expectations.
	for _, migrationFile := range migrationFiles { // Apply every committed migration file to the isolated embedded database before the integration flow begins.
		sqlBytes, readErr := os.ReadFile(migrationFile) // Read the exact migration SQL committed in the repository working tree.
		if readErr != nil {                             // Stop immediately when a committed migration file cannot be read.
			t.Fatalf("failed to read migration file %s: %v", migrationFile, readErr) // Surface the unreadable file path because schema setup cannot continue without it.
		}
		if _, execErr := conn.Exec(ctx, string(sqlBytes)); execErr != nil { // Execute the migration SQL exactly as committed against the live embedded database.
			t.Fatalf("failed to apply migration %s: %v", filepath.Base(migrationFile), execErr) // Surface the failing migration name because the schema setup depends on every migration succeeding.
		}
	}
}

// integrationRepositoryRoot returns the repository root directory by walking upward from this integration test file's location.
func integrationRepositoryRoot(t *testing.T) string {
	_, file, _, ok := runtime.Caller(0) // Read the current test file path so the repository root can be derived without depending on the process working directory.
	if !ok {                            // Stop immediately when the runtime cannot determine the current file path.
		t.Fatal("failed to resolve current integration test file path") // Surface the missing runtime caller metadata because migration discovery depends on it.
	}

	return filepath.Dir(filepath.Dir(filepath.Dir(file))) // Walk from internal/integration/monolith_flow_test.go back up to the repository root directory.
}

// writeJSONResponse writes one JSON response with the supplied status code for the fake Appium server.
func writeJSONResponse(w http.ResponseWriter, statusCode int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json") // Mark the fake Appium response as JSON so the production client decodes it using its normal path.
	w.WriteHeader(statusCode)                          // Send the supplied HTTP status code before writing the response body.
	_ = json.NewEncoder(w).Encode(payload)             // Encode the supplied response payload directly because handler tests care about production client behavior rather than write errors.
}

// waitForTraceTerminalState polls the production service until the trace reaches one expected terminal state or the timeout expires.
func waitForTraceTerminalState(t *testing.T, ctx context.Context, service *orchestrator.Service, traceID string, expectedStatus string) (*postgres.Trace, []*postgres.PlanEvent) {
	deadline := time.Now().Add(10 * time.Second) // Bound the polling loop so the integration test fails deterministically when execution never reaches a terminal state.
	for time.Now().Before(deadline) {            // Keep polling until the trace reaches the expected terminal state or the timeout expires.
		trace, events, err := service.GetTrace(ctx, traceID)              // Read the current persisted trace row and event list through the production service API.
		if err == nil && trace != nil && trace.Status == expectedStatus { // Return as soon as the trace reaches the expected terminal state.
			return trace, events // Return the terminal trace and events so the caller can assert the completed flow in detail.
		}
		time.Sleep(100 * time.Millisecond) // Back off briefly between polls so the integration test does not spin hot while waiting for the dispatcher loop.
	}

	t.Fatalf("trace %s did not reach terminal status %s before timeout", traceID, expectedStatus) // Surface the missing terminal state because the integration flow under test failed to complete.
	return nil, nil                                                                               // Return zero values only to satisfy the compiler because the fatal assertion above does not return.
}

// TestMonolithStartExecuteTraceEndFlow verifies one minimal startSession -> executePlan -> getTrace -> endSession flow through the production monolith orchestrator stack.
func TestMonolithStartExecuteTraceEndFlow(t *testing.T) {
	harness := newMonolithFlowHarness(t) // Start one isolated monolith integration harness so the full execution flow can exercise real storage, cache, dispatcher, and Appium HTTP paths.
	ctx := context.Background()          // Use one shared background context because the integration flow runs synchronously inside the current test.

	sessionCaps := map[string]interface{}{ // Build one representative session capability set so StartSession exercises real Appium payload creation.
		"platformName":      "Android",  // Provide a platform name because Appium session creation expects one realistic capability payload.
		"appium:udid":       "emulator", // Provide one vendor-prefixed capability so the fake Appium server receives a realistic W3C payload.
		"appium:appPackage": "demo.app", // Provide one package capability so the session payload resembles a real Android automation request.
	}
	session, err := harness.service.StartSession(ctx, "integration-project", sessionCaps) // Execute the production StartSession flow so Appium HTTP, DAO persistence, and Redis session mapping all run for real.
	if err != nil {                                                                       // Fail immediately when session creation unexpectedly fails because the rest of the integration flow depends on one live session.
		t.Fatalf("expected start session to succeed, got error: %v", err) // Surface the session-creation failure so integration regressions are easy to diagnose.
	}
	if session == nil || strings.TrimSpace(session.ID) == "" { // Reject missing session identifiers because every downstream integration step depends on the persisted session ID.
		t.Fatalf("expected persisted session id, got %#v", session) // Surface the actual session payload so persistence regressions are easy to diagnose.
	}

	planJSON := json.RawMessage(`{"steps":[{"type":"sendKeys","selector":"id=username","params":{"text":"demo-user"}},{"type":"click","selector":"xpath=//button[@id='submit']"}]}`) // Build one minimal multi-step plan that exercises both sendKeys and click through the real monolith executor.
	traceID, err := harness.service.ExecutePlanWithTrace(ctx, session.ID, "", planJSON)                                                                                              // Execute the production plan-enqueue path so Redis queueing and dispatcher pickup run for real.
	if err != nil {                                                                                                                                                                  // Fail immediately when plan submission unexpectedly fails because the integration flow under test is the enqueue-to-terminal path.
		t.Fatalf("expected execute plan to succeed, got error: %v", err) // Surface the enqueue failure so dispatcher regressions are easy to diagnose.
	}
	if strings.TrimSpace(traceID) == "" { // Reject missing trace identifiers because terminal trace polling depends on one persisted trace ID.
		t.Fatal("expected non-empty trace id") // Surface the missing trace ID because downstream integration assertions depend on it.
	}

	trace, events := waitForTraceTerminalState(t, ctx, harness.service, traceID, "completed") // Poll the production service until the queued plan reaches its completed terminal state.
	if trace.SessionID != session.ID {                                                        // Assert that the persisted trace stayed linked to the created session throughout the flow.
		t.Fatalf("expected trace session %s, got %s", session.ID, trace.SessionID) // Surface the actual session linkage so trace-persistence regressions are easy to diagnose.
	}
	if len(events) < 5 { // Assert that the executor emitted step-level running/passed events plus one final completed event.
		t.Fatalf("expected at least 5 events for two successful steps plus terminal event, got %d", len(events)) // Surface the actual event count so trace-event regressions are easy to diagnose.
	}
	lastEvent := events[len(events)-1]                                // Read the terminal event so the completed terminal state can be asserted directly.
	if lastEvent.Status != "completed" || lastEvent.StepIndex != -1 { // Assert the durable terminal event contract required by the trace replay path.
		t.Fatalf("expected final completed trace-level event, got status=%s stepIndex=%d", lastEvent.Status, lastEvent.StepIndex) // Surface the actual terminal event so replay regressions are easy to diagnose.
	}

	if err := harness.service.EndSession(ctx, session.ID); err != nil { // Execute the production EndSession flow so the persisted session transition and Appium delete call run for real.
		t.Fatalf("expected end session to succeed, got error: %v", err) // Surface the session-teardown failure so lifecycle regressions are easy to diagnose.
	}
	persistedSession, err := harness.service.GetSession(ctx, session.ID) // Read the persisted session row back through the production service API after teardown.
	if err != nil {                                                      // Fail immediately when the service cannot read back the ended session.
		t.Fatalf("expected ended session to remain readable, got error: %v", err) // Surface the lookup failure so persistence regressions are easy to diagnose.
	}
	if persistedSession.Status != "ended" { // Assert the durable ended session state required by later lifecycle and replay behavior.
		t.Fatalf("expected session status ended, got %s", persistedSession.Status) // Surface the actual session state so teardown regressions are easy to diagnose.
	}

	harness.appium.mu.Lock()                                                     // Protect the recorded fake Appium state while the final assertions read the accumulated side effects.
	recordedCaps := harness.appium.recordedCaps                                  // Copy the recorded capability payload so assertions can proceed after the lock is released.
	recordedSelectors := append([]string(nil), harness.appium.foundSelectors...) // Copy the recorded selector list so assertions can proceed after the lock is released.
	recordedClicks := append([]string(nil), harness.appium.clickedElements...)   // Copy the recorded click list so assertions can proceed after the lock is released.
	recordedSendKeys := append([]string(nil), harness.appium.sendKeysTexts...)   // Copy the recorded send-keys list so assertions can proceed after the lock is released.
	recordedDeletes := append([]string(nil), harness.appium.deletedSessions...)  // Copy the recorded delete-session list so assertions can proceed after the lock is released.
	harness.appium.mu.Unlock()                                                   // Release the fake Appium server state lock before running assertions.
	if recordedCaps["platformName"] != "Android" {                               // Assert that StartSession forwarded the requested capabilities into the fake Appium server.
		t.Fatalf("expected fake appium to receive Android capabilities, got %#v", recordedCaps) // Surface the actual capabilities so session-creation regressions are easy to diagnose.
	}
	if len(recordedSelectors) < 2 || recordedSelectors[0] != "id:username" || recordedSelectors[1] != "xpath://button[@id='submit']" { // Assert that the production executor resolved the expected selectors through the fake Appium server.
		t.Fatalf("expected fake appium selectors [id:username xpath://button[@id='submit']], got %#v", recordedSelectors) // Surface the actual selector calls so executor regressions are easy to diagnose.
	}
	if len(recordedSendKeys) != 1 || recordedSendKeys[0] != "demo-user" { // Assert that the production executor forwarded the sendKeys text into the fake Appium server.
		t.Fatalf("expected fake appium sendKeys text demo-user, got %#v", recordedSendKeys) // Surface the actual send-keys calls so executor regressions are easy to diagnose.
	}
	if len(recordedClicks) != 1 || recordedClicks[0] != "element-123" { // Assert that the production executor clicked the fake element returned by the fake Appium server.
		t.Fatalf("expected fake appium click on element-123, got %#v", recordedClicks) // Surface the actual click calls so executor regressions are easy to diagnose.
	}
	if len(recordedDeletes) != 1 || recordedDeletes[0] != "appium-session-1" { // Assert that EndSession propagated one Appium DELETE request for the created fake session.
		t.Fatalf("expected fake appium delete for appium-session-1, got %#v", recordedDeletes) // Surface the actual delete calls so session-teardown regressions are easy to diagnose.
	}
}
