// distributed_flow_test.go verifies one end-to-end distributed execution flow against embedded PostgreSQL, real Redis commands, a fake Appium HTTP server, and live worker/orchestrator gRPC callbacks.
package integration

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/storage/postgres"
	redisstore "mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/worker"

	"github.com/alicebob/miniredis/v2"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"google.golang.org/grpc"
)

// distributedFlowHarness owns the isolated PostgreSQL, Redis, fake Appium server, orchestrator service, worker callback client, and gRPC servers used by one distributed integration test.
type distributedFlowHarness struct {
	dao                  *postgres.DAO
	cache                *redisstore.Cache
	service              *orchestrator.Service
	appium               *fakeAppiumServer
	cancel               context.CancelFunc
	orchestratorClient   *rpc.OrchestratorClient
	orchestratorServer   *grpc.Server
	orchestratorListener net.Listener
	workerServer         *grpc.Server
	workerListener       net.Listener
}

// newDistributedFlowHarness starts isolated dependencies, applies migrations, constructs a distributed orchestrator service, and wires one real worker gRPC server back to the orchestrator callback API.
func newDistributedFlowHarness(t *testing.T) *distributedFlowHarness {
	ctx := context.Background()            // Use one shared background context because harness setup runs synchronously inside the current test.
	appiumServer := newFakeAppiumServer(t) // Start one fake Appium server so both StartSession and distributed worker execution exercise real HTTP calls.

	postgresPort := reserveIntegrationLoopbackPort(t)                                  // Reserve one loopback TCP port so the embedded PostgreSQL instance can bind without colliding with developer-managed services.
	tempDir := t.TempDir()                                                             // Allocate one isolated filesystem root so the embedded PostgreSQL runtime never shares state across tests.
	postgresInstance := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig(). // Construct the embedded PostgreSQL instance with fully isolated paths and credentials for this distributed integration test.
												Port(uint32(postgresPort)).                       // Bind the embedded database to the reserved loopback port for this isolated integration instance.
												Database(integrationDatabaseName).                // Create the same database name that the repository examples and DAO defaults already use.
												Username(integrationDatabaseUser).                // Create the same superuser name used by the repository examples and migration replay tooling.
												Password(integrationDatabasePass).                // Create the matching superuser password for the isolated integration instance.
												RuntimePath(filepath.Join(tempDir, "runtime")).   // Store the PostgreSQL runtime files under the test temp directory so nothing leaks into the developer environment.
												DataPath(filepath.Join(tempDir, "data")).         // Store the PostgreSQL data directory under the test temp directory so every integration run starts from a clean cluster.
												BinariesPath(filepath.Join(tempDir, "binaries"))) // Store downloaded PostgreSQL binaries under the test temp directory so the integration harness remains self-contained.
	if err := postgresInstance.Start(); err != nil { // Stop immediately when the embedded PostgreSQL server cannot start for this distributed integration test.
		t.Fatalf("failed to start embedded postgres: %v", err) // Surface the startup failure because no persistence-backed distributed assertion can run without a live database.
	}
	t.Cleanup(func() {
		if err := postgresInstance.Stop(); err != nil { // Stop the embedded PostgreSQL instance after the distributed integration test completes.
			t.Fatalf("failed to stop embedded postgres: %v", err) // Surface shutdown failures because they usually indicate leaked connections or stuck background processes.
		}
	})

	dsn := buildIntegrationDSN(postgresPort)                                                                                          // Build the DAO and migration DSN that points at the isolated embedded PostgreSQL instance.
	applyIntegrationMigrations(t, ctx, dsn)                                                                                           // Apply the committed repository migrations before the DAO touches the fresh embedded database.
	dao, err := postgres.NewDAO(ctx, config.PostgresConfig{DSN: dsn, MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute}) // Open the production DAO against the live embedded database so the integration test exercises real persistence code.
	if err != nil {                                                                                                                   // Stop immediately when the DAO cannot connect to the isolated embedded database.
		t.Fatalf("failed to create dao: %v", err) // Surface the DAO construction failure because none of the distributed integration assertions can proceed without it.
	}
	t.Cleanup(func() {
		dao.Close() // Close the DAO pool before the embedded PostgreSQL instance shuts down so connections are released cleanly.
	})

	redisServer := miniredis.RunT(t)                                                // Start one in-memory Redis server so queueing, leases, and worker registration use real Redis commands.
	cache, err := redisstore.NewCache(config.RedisConfig{Addr: redisServer.Addr()}) // Open the production Redis cache wrapper against the isolated in-memory Redis server.
	if err != nil {                                                                 // Stop immediately when the Redis cache wrapper cannot connect to the isolated in-memory Redis server.
		t.Fatalf("failed to create redis cache: %v", err) // Surface the Redis setup failure because dispatcher execution and distributed leases depend on cache operations.
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil { // Close the production cache wrapper before the in-memory Redis server is torn down.
			t.Fatalf("failed to close redis cache: %v", err) // Surface cache-close failures because they usually indicate leaked resources in the distributed path.
		}
		redisServer.Close() // Stop the in-memory Redis server after the cache wrapper has released its connection.
	})

	service := orchestrator.NewService(config.OrchestratorConfig{ // Construct the production distributed orchestrator service with the minimum config required for the integration flow.
		PlanTimeout:   10 * time.Second,                      // Use one bounded plan timeout so the distributed flow fails promptly when worker execution stalls.
		StepTimeout:   3 * time.Second,                       // Use one bounded per-step timeout so fake Appium calls remain prompt and deterministic.
		AutoWaitMax:   500 * time.Millisecond,                // Use one short auto-wait ceiling so missing fake elements fail quickly during integration debugging.
		SnapshotTTL:   time.Minute,                           // Use one positive snapshot TTL because the orchestrator config expects it even though this test does not read snapshots.
		ExecutionMode: orchestrator.ExecutionModeDistributed, // Exercise the distributed path because this test exists to validate the full worker callback closure path.
	}, config.RPCSecurityConfig{}, config.WorkerConfig{
		AppiumURL:         appiumServer.baseURL, // Point StartSession at the fake Appium loopback server so the orchestrator creates one real Appium session before distributed execution starts.
		HeartbeatInterval: 10 * time.Second,     // Reuse the normal heartbeat cadence so the derived distributed execution lease matches production defaults closely.
	}, config.AWSConfig{}, config.DeviceFarmConfig{Mode: "disabled"}, dao, cache, nil) // Disable Device Farm and S3 because this distributed integration flow focuses on worker callbacks and trace closure.

	runCtx, cancel := context.WithCancel(context.Background()) // Create one service lifecycle context so dispatcher and registry loops can be started and later stopped deterministically.
	if err := service.Start(runCtx); err != nil {              // Start the production orchestrator service loops before starting the worker and enqueuing any plan.
		t.Fatalf("failed to start orchestrator service: %v", err) // Surface startup failures because the distributed integration flow requires a running dispatcher.
	}

	orchestratorListener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind one explicit IPv4 loopback listener so the worker callback client can dial the orchestrator reliably in restricted environments too.
	if err != nil {                                                // Skip the current distributed integration test when loopback listeners are unavailable in the environment.
		t.Skipf("skipping distributed integration test because orchestrator gRPC listen failed: %v", err) // Surface the environment limitation without misclassifying the distributed flow as passing.
	}
	orchestratorHandler, err := orchestrator.NewGRPCServer(service, config.RPCSecurityConfig{}) // Build the production orchestrator callback handler over the live distributed service under test.
	if err != nil {                                                                             // Stop immediately when the callback handler cannot be constructed because the worker cannot register or callback without it.
		t.Fatalf("failed to create orchestrator gRPC handler: %v", err) // Surface the construction failure because distributed callbacks depend on this service.
	}
	orchestratorServer := grpc.NewServer()                                         // Construct one loopback gRPC server dedicated to the distributed callback and worker registration API.
	rpc.RegisterOrchestratorServiceServer(orchestratorServer, orchestratorHandler) // Register the live orchestrator callback handler so the worker can register, renew leases, and report events.
	go func() {
		_ = orchestratorServer.Serve(orchestratorListener) // Serve orchestrator callback RPCs in the background so the worker can dial the loopback endpoint during the test.
	}()

	orchestratorClient, err := rpc.NewOrchestratorClient(orchestratorListener.Addr().String(), config.RPCSecurityConfig{}) // Dial the live orchestrator callback endpoint so the worker can use the same client path as production.
	if err != nil {                                                                                                        // Stop immediately when the worker-side orchestrator client cannot be constructed.
		t.Fatalf("failed to create orchestrator callback client: %v", err) // Surface the dial failure because the real worker server cannot report callbacks without it.
	}

	workerListener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind one explicit IPv4 loopback listener so the dispatcher can dial the real worker server reliably in restricted environments too.
	if err != nil {                                          // Skip the current distributed integration test when loopback listeners are unavailable in the environment.
		t.Skipf("skipping distributed integration test because worker gRPC listen failed: %v", err) // Surface the environment limitation without misclassifying the distributed flow as passing.
	}
	const workerID = "distributed-worker-1"                                                                                                   // Use one stable worker id so distributed lease ownership and callback assertions remain deterministic.
	workerService := worker.NewGRPCServer(appiumServer.baseURL, 3*time.Second, 500*time.Millisecond, workerID, orchestratorClient, time.Hour) // Construct the production worker gRPC service so execution, callbacks, and session attachment all use the real worker code path.
	workerServer := grpc.NewServer()                                                                                                          // Construct one loopback gRPC server dedicated to the live worker service under test.
	rpc.RegisterWorkerServiceServer(workerServer, workerService)                                                                              // Register the live worker service so the dispatcher can dial it through the production RPC contract.
	go func() {
		_ = workerServer.Serve(workerListener) // Serve worker RPCs in the background so the dispatcher can dispatch to the loopback worker during the test.
	}()

	registerResp, err := orchestratorClient.RegisterWorker(context.Background(), &rpc.RegisterWorkerRequest{ // Register the live worker through the production orchestrator gRPC API so dispatcher assignment uses the normal worker-registry path too.
		WorkerID: workerID,                       // Publish the stable worker identifier used throughout this distributed integration test.
		Address:  workerListener.Addr().String(), // Publish the real loopback worker address so dispatcher ExecutePlan RPCs reach the live worker service.
		Capacity: 1,                              // Keep worker capacity at one because this test exercises only one in-flight distributed trace.
		Tags:     map[string]string{},            // Use no special tags because the dispatcher should be able to assign the worker without extra filtering.
	})
	if err != nil { // Stop immediately when the worker cannot register through the live orchestrator callback endpoint.
		t.Fatalf("failed to register worker through orchestrator gRPC: %v", err) // Surface the registration failure because distributed dispatch cannot start without an assignable worker.
	}
	if registerResp == nil || !registerResp.Success { // Fail the test when the live worker registration call returns an application-level rejection.
		t.Fatalf("expected worker registration success, got %#v", registerResp) // Surface the registration response because distributed assignment depends on a healthy registered worker.
	}

	t.Cleanup(func() {
		workerServer.GracefulStop()       // Stop the live worker gRPC server after the distributed integration test completes.
		_ = workerListener.Close()        // Release the live worker loopback port immediately after the worker server stops.
		_ = orchestratorClient.Close()    // Close the worker-side orchestrator callback client before the orchestrator callback server is torn down.
		orchestratorServer.GracefulStop() // Stop the loopback orchestrator callback gRPC server after the distributed integration test completes.
		_ = orchestratorListener.Close()  // Release the orchestrator callback loopback port immediately after the gRPC server stops.
		cancel()                          // Cancel the service lifecycle context so background dispatcher and registry loops begin shutting down.
		service.Stop()                    // Stop the orchestrator service explicitly so dispatcher and registry goroutines release resources promptly.
	})

	return &distributedFlowHarness{ // Return the fully wired distributed harness so the integration test can drive the end-to-end distributed flow and assert side effects.
		dao:                  dao,                  // Expose the DAO in case later distributed integration assertions need direct persistence inspection.
		cache:                cache,                // Expose the Redis cache in case later distributed integration assertions need direct lease inspection.
		service:              service,              // Expose the live distributed orchestrator service used by the end-to-end flow.
		appium:               appiumServer,         // Expose the fake Appium server so the test can assert underlying HTTP side effects.
		cancel:               cancel,               // Expose the lifecycle cancel for completeness even though t.Cleanup owns shutdown.
		orchestratorClient:   orchestratorClient,   // Expose the live callback client so the harness retains it for cleanup.
		orchestratorServer:   orchestratorServer,   // Expose the loopback orchestrator callback server for cleanup and later diagnostics.
		orchestratorListener: orchestratorListener, // Expose the loopback orchestrator callback listener for cleanup and later diagnostics.
		workerServer:         workerServer,         // Expose the live worker gRPC server for cleanup and later diagnostics.
		workerListener:       workerListener,       // Expose the loopback worker listener for cleanup and later diagnostics.
	}
}

// TestDistributedStartExecuteTraceEndFlow verifies one minimal startSession -> executePlan -> worker callback closure -> getTrace -> endSession flow through the production distributed stack.
func TestDistributedStartExecuteTraceEndFlow(t *testing.T) {
	harness := newDistributedFlowHarness(t) // Start one isolated distributed integration harness so the full execution flow can exercise real storage, cache, worker RPC, and callback behavior.
	ctx := context.Background()             // Use one shared background context because the integration flow runs synchronously inside the current test.

	sessionCaps := map[string]interface{}{ // Build one representative session capability set so StartSession exercises real Appium payload creation before distributed execution begins.
		"platformName":      "Android",  // Provide a platform name because Appium session creation expects one realistic capability payload.
		"appium:udid":       "emulator", // Provide one vendor-prefixed capability so the fake Appium server receives a realistic W3C payload.
		"appium:appPackage": "demo.app", // Provide one package capability so the session payload resembles a real Android automation request.
	}
	session, err := harness.service.StartSession(ctx, "integration-project", sessionCaps) // Execute the production StartSession flow so the distributed worker later receives a real Appium session mapping.
	if err != nil {                                                                       // Fail immediately when session creation unexpectedly fails because the rest of the distributed integration flow depends on one live session.
		t.Fatalf("expected distributed start session to succeed, got error: %v", err) // Surface the session-creation failure so distributed integration regressions are easy to diagnose.
	}
	if session == nil || session.ID == "" { // Reject missing session identifiers because every downstream distributed integration step depends on the persisted session ID.
		t.Fatalf("expected persisted session id, got %#v", session) // Surface the actual session payload so persistence regressions are easy to diagnose.
	}

	planJSON := json.RawMessage(`{"steps":[{"type":"sendKeys","selector":"id=username","params":{"text":"demo-user"}},{"type":"click","selector":"xpath=//button[@id='submit']"}]}`) // Build one minimal multi-step plan that exercises sendKeys and click through the real distributed worker path.
	traceID, err := harness.service.ExecutePlanWithTrace(ctx, session.ID, "", planJSON)                                                                                              // Execute the production plan-enqueue path so Redis queueing, dispatcher pickup, worker RPC, and callback closure all run for real.
	if err != nil {                                                                                                                                                                  // Fail immediately when plan submission unexpectedly fails because the integration flow under test is the queue-to-worker-to-terminal path.
		t.Fatalf("expected distributed execute plan to succeed, got error: %v", err) // Surface the enqueue failure so distributed dispatcher regressions are easy to diagnose.
	}
	if traceID == "" { // Reject missing trace identifiers because terminal trace polling depends on one persisted trace ID.
		t.Fatal("expected non-empty distributed trace id") // Surface the missing trace ID because downstream distributed integration assertions depend on it.
	}

	trace, events := waitForTraceTerminalState(t, ctx, harness.service, traceID, "completed") // Poll the production distributed service until the queued plan reaches its completed terminal state.
	if trace.SessionID != session.ID {                                                        // Assert that the persisted trace stayed linked to the created platform session throughout the distributed flow.
		t.Fatalf("expected distributed trace session %s, got %s", session.ID, trace.SessionID) // Surface the actual session linkage so distributed persistence regressions are easy to diagnose.
	}
	if trace.CurrentAttempt != 1 { // Assert that the first accepted distributed worker run persisted attempt 1 on the trace row.
		t.Fatalf("expected distributed current attempt 1, got %d", trace.CurrentAttempt) // Surface the persisted attempt because stale-result protection depends on it.
	}
	if trace.TerminalReason == nil || *trace.TerminalReason != "completed" { // Assert that the terminal callback stored the structured completed reason on the trace row.
		t.Fatalf("expected distributed terminal reason completed, got %#v", trace.TerminalReason) // Surface the actual terminal reason so callback-finalization regressions are easy to diagnose.
	}
	if len(events) < 5 { // Assert that the worker callback path emitted step-level running/passed events plus one final completed event.
		t.Fatalf("expected at least 5 distributed events for two successful steps plus terminal event, got %d", len(events)) // Surface the actual event count so distributed callback regressions are easy to diagnose.
	}
	lastEvent := events[len(events)-1]                                // Read the terminal event so the completed terminal state can be asserted directly.
	if lastEvent.Status != "completed" || lastEvent.StepIndex != -1 { // Assert the durable terminal event contract required by the distributed callback replay path.
		t.Fatalf("expected final completed distributed trace-level event, got status=%s stepIndex=%d", lastEvent.Status, lastEvent.StepIndex) // Surface the actual terminal event so distributed replay regressions are easy to diagnose.
	}

	if err := harness.service.EndSession(ctx, session.ID); err != nil { // Execute the production EndSession flow so the persisted session transition and Appium delete call run for real after distributed execution completes.
		t.Fatalf("expected distributed end session to succeed, got error: %v", err) // Surface the session-teardown failure so lifecycle regressions are easy to diagnose.
	}

	harness.appium.mu.Lock()                                                           // Protect the recorded fake Appium state while the final assertions read the accumulated side effects.
	recordedCaps := harness.appium.recordedCaps                                        // Copy the recorded capability payload so assertions can proceed after the lock is released.
	recordedCommandSessions := append([]string(nil), harness.appium.usedSessionIDs...) // Copy the recorded command-scoped Appium session ids so assertions can verify the worker attached to the real Appium session.
	recordedSelectors := append([]string(nil), harness.appium.foundSelectors...)       // Copy the recorded selector list so assertions can proceed after the lock is released.
	recordedClicks := append([]string(nil), harness.appium.clickedElements...)         // Copy the recorded click list so assertions can proceed after the lock is released.
	recordedSendKeys := append([]string(nil), harness.appium.sendKeysTexts...)         // Copy the recorded send-keys list so assertions can proceed after the lock is released.
	recordedDeletes := append([]string(nil), harness.appium.deletedSessions...)        // Copy the recorded delete-session list so assertions can proceed after the lock is released.
	harness.appium.mu.Unlock()                                                         // Release the fake Appium server state lock before running assertions.

	if recordedCaps["platformName"] != "Android" { // Assert that StartSession forwarded the requested capabilities into the fake Appium server before distributed execution began.
		t.Fatalf("expected fake appium to receive Android capabilities, got %#v", recordedCaps) // Surface the actual capabilities so distributed session-creation regressions are easy to diagnose.
	}
	if len(recordedSelectors) < 2 || recordedSelectors[0] != "id:username" || recordedSelectors[1] != "xpath://button[@id='submit']" { // Assert that the real distributed worker resolved the expected selectors through the fake Appium server.
		t.Fatalf("expected distributed fake appium selectors [id:username xpath://button[@id='submit']], got %#v", recordedSelectors) // Surface the actual selector calls so distributed worker regressions are easy to diagnose.
	}
	if len(recordedSendKeys) != 1 || recordedSendKeys[0] != "demo-user" { // Assert that the real distributed worker forwarded the sendKeys text into the fake Appium server.
		t.Fatalf("expected distributed fake appium sendKeys text demo-user, got %#v", recordedSendKeys) // Surface the actual send-keys calls so distributed worker regressions are easy to diagnose.
	}
	if len(recordedClicks) != 1 || recordedClicks[0] != "element-123" { // Assert that the real distributed worker clicked the fake element returned by the fake Appium server.
		t.Fatalf("expected distributed fake appium click on element-123, got %#v", recordedClicks) // Surface the actual click calls so distributed worker regressions are easy to diagnose.
	}
	if len(recordedCommandSessions) < 3 { // Assert that the fake Appium server saw the distributed worker's command-scoped requests.
		t.Fatalf("expected at least 3 distributed recorded command session ids, got %#v", recordedCommandSessions) // Surface the actual command-scoped session list so distributed session-routing regressions are easy to diagnose.
	}
	for _, commandSessionID := range recordedCommandSessions { // Assert that every distributed worker command stayed bound to the Appium session created during StartSession.
		if commandSessionID != "appium-session-1" { // Fail when any distributed worker command unexpectedly targeted a different Appium session than the one created by StartSession.
			t.Fatalf("expected every distributed command to target appium-session-1, got %#v", recordedCommandSessions) // Surface the complete command-scoped session list so distributed session-routing regressions are easy to diagnose.
		}
	}
	if len(recordedDeletes) != 1 || recordedDeletes[0] != "appium-session-1" { // Assert that EndSession propagated one Appium DELETE request for the created fake session after distributed execution completed.
		t.Fatalf("expected distributed fake appium delete for appium-session-1, got %#v", recordedDeletes) // Surface the actual delete calls so distributed session-teardown regressions are easy to diagnose.
	}
}
