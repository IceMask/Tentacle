// artifact_flow_test.go verifies one artifact-inclusive monolith flow against embedded PostgreSQL, real Redis commands, a fake Appium HTTP server, and a fake S3-compatible HTTP endpoint.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"image/png"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
	redisstore "mcp_for_appium/internal/storage/redis"
	s3store "mcp_for_appium/internal/storage/s3"

	"github.com/alicebob/miniredis/v2"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

// fakeS3Server records object writes made through the production S3 client during the artifact integration flow.
type fakeS3Server struct {
	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
	baseURL  string
	objects  map[string][]byte
}

// newFakeS3Server starts one IPv4 loopback HTTP server that accepts path-style S3 PUT requests and records uploaded object bytes.
func newFakeS3Server(t *testing.T) *fakeS3Server {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind one explicit IPv4 loopback listener so the fake S3 server works in restricted environments too.
	if err != nil {                                    // Skip the current integration test when loopback listeners are unavailable in the environment.
		t.Skipf("skipping artifact integration test because fake s3 listen failed: %v", err) // Surface the environment limitation without misclassifying the artifact flow as passing.
	}

	fakeServer := &fakeS3Server{listener: listener, objects: make(map[string][]byte)} // Construct the fake S3 server state with one in-memory object map for deterministic assertions.
	mux := http.NewServeMux()                                                         // Allocate one dedicated HTTP mux so the fake S3 server can register only the endpoints needed by the artifact flow.
	mux.HandleFunc("/", fakeServer.handleRoot)                                        // Register the root endpoint used by the production S3 client for object PUTs.
	fakeServer.server = &http.Server{Handler: mux}                                    // Construct the HTTP server over the fake S3 handler set so the production client can talk to it.
	go func() {
		_ = fakeServer.server.Serve(listener) // Serve fake S3 requests in the background so the artifact flow can exercise real HTTP calls.
	}()
	fakeServer.baseURL = "http://" + listener.Addr().String() // Publish the loopback base URL used by the production S3 client under test.
	t.Cleanup(func() {
		fakeServer.Close() // Ensure the fake S3 server always shuts down after the integration test completes.
	})

	return fakeServer // Return the running fake S3 server so the integration harness can wire it into the orchestrator service and assert side effects.
}

// handleRoot records path-style S3 PUT requests and returns a minimal successful object-store response.
func (f *fakeS3Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut { // Reject unsupported methods because the current artifact flow only needs PutObject writes.
		http.NotFound(w, r) // Return a plain 404 so unexpected fake S3 traffic becomes obvious during integration failures.
		return              // Stop handling once the unsupported method has been rejected.
	}

	objectPath := strings.TrimPrefix(r.URL.Path, "/") // Normalize the request path so the recorded object key stays readable in later assertions.
	bodyBytes, err := io.ReadAll(r.Body)              // Read the full uploaded object body so the integration test can assert artifact writes precisely.
	if err != nil {                                   // Reject malformed uploads because the production artifact flow expects complete object writes.
		http.Error(w, err.Error(), http.StatusBadRequest) // Return one explicit 400 response so malformed object writes fail clearly.
		return                                            // Stop handling once the malformed upload has been rejected.
	}

	f.mu.Lock()                            // Protect the fake S3 server state because PUT requests can race with later assertions.
	f.objects[objectPath] = bodyBytes      // Record the uploaded object bytes under the full path-style key used by the production S3 client.
	f.mu.Unlock()                          // Release the fake S3 server state lock before writing the HTTP response.
	w.WriteHeader(http.StatusOK)           // Return a generic successful object-store response because the production S3 client only needs success here.
	_, _ = w.Write([]byte("<PutObject/>")) // Return one tiny XML-like payload so the AWS client sees a non-empty successful response body.
}

// Close stops the fake S3 HTTP server and releases its loopback listener.
func (f *fakeS3Server) Close() {
	if f.server != nil { // Stop the HTTP server only when the fake S3 server was started successfully.
		_ = f.server.Close() // Close the fake S3 HTTP server so the loopback listener stops accepting new requests.
	}
	if f.listener != nil { // Close the listener only when the fake S3 server successfully bound one loopback socket.
		_ = f.listener.Close() // Release the loopback socket so repeated test runs do not leak listening ports.
	}
}

// artifactFlowHarness owns the isolated PostgreSQL, Redis, fake Appium server, fake S3 server, and orchestrator service used by one artifact-inclusive integration test.
type artifactFlowHarness struct {
	dao     *postgres.DAO
	cache   *redisstore.Cache
	service *orchestrator.Service
	appium  *fakeAppiumServer
	s3      *fakeS3Server
	cancel  context.CancelFunc
}

// newArtifactFlowHarness starts isolated dependencies, applies migrations, and constructs a monolith orchestrator service with a live fake S3 client ready for artifact assertions.
func newArtifactFlowHarness(t *testing.T) *artifactFlowHarness {
	ctx := context.Background()            // Use one shared background context because the harness setup runs synchronously inside the current test.
	appiumServer := newFakeAppiumServer(t) // Start one fake Appium server so session creation, execution, and screenshot capture exercise real HTTP calls.
	s3Server := newFakeS3Server(t)         // Start one fake S3 server so artifact uploads exercise the production S3 client over real HTTP.

	postgresPort := reserveIntegrationLoopbackPort(t)                                  // Reserve one loopback TCP port so the embedded PostgreSQL instance can bind without colliding with developer-managed services.
	tempDir := t.TempDir()                                                             // Allocate one isolated filesystem root so the embedded PostgreSQL runtime never shares state across tests.
	postgresInstance := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig(). // Construct the embedded PostgreSQL instance with fully isolated paths and credentials for this artifact integration test.
												Port(uint32(postgresPort)).                       // Bind the embedded database to the reserved loopback port for this isolated integration instance.
												Database(integrationDatabaseName).                // Create the same database name that the repository examples and DAO defaults already use.
												Username(integrationDatabaseUser).                // Create the same superuser name used by the repository examples and migration replay tooling.
												Password(integrationDatabasePass).                // Create the matching superuser password for the isolated integration instance.
												RuntimePath(filepath.Join(tempDir, "runtime")).   // Store the PostgreSQL runtime files under the test temp directory so nothing leaks into the developer environment.
												DataPath(filepath.Join(tempDir, "data")).         // Store the PostgreSQL data directory under the test temp directory so every integration run starts from a clean cluster.
												BinariesPath(filepath.Join(tempDir, "binaries"))) // Store downloaded PostgreSQL binaries under the test temp directory so the integration harness remains self-contained.
	if err := postgresInstance.Start(); err != nil { // Stop immediately when the embedded PostgreSQL server cannot start for this artifact integration test.
		t.Fatalf("failed to start embedded postgres: %v", err) // Surface the startup failure because no persistence-backed artifact assertion can run without a live database.
	}
	t.Cleanup(func() {
		if err := postgresInstance.Stop(); err != nil { // Stop the embedded PostgreSQL instance after the artifact integration test completes.
			t.Fatalf("failed to stop embedded postgres: %v", err) // Surface shutdown failures because they usually indicate leaked connections or stuck background processes.
		}
	})

	dsn := buildIntegrationDSN(postgresPort)                                                                                          // Build the DAO and migration DSN that points at the isolated embedded PostgreSQL instance.
	applyIntegrationMigrations(t, ctx, dsn)                                                                                           // Apply the committed repository migrations before the DAO touches the fresh embedded database.
	dao, err := postgres.NewDAO(ctx, config.PostgresConfig{DSN: dsn, MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute}) // Open the production DAO against the live embedded database so the integration test exercises real persistence code.
	if err != nil {                                                                                                                   // Stop immediately when the DAO cannot connect to the isolated embedded database.
		t.Fatalf("failed to create dao: %v", err) // Surface the DAO construction failure because none of the artifact integration assertions can proceed without it.
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
			t.Fatalf("failed to close redis cache: %v", err) // Surface cache-close failures because they usually indicate leaked resources in the artifact path.
		}
		redisServer.Close() // Stop the in-memory Redis server after the cache wrapper has released its connection.
	})

	s3Client, err := s3store.NewClient(ctx, config.S3Config{ // Construct the production S3 client against the fake loopback endpoint so artifact upload and presign code paths are exercised for real.
		Region:          "us-east-1",       // Use one concrete region so the AWS SDK can sign requests deterministically.
		Bucket:          "artifact-bucket", // Use one path-style bucket name so the fake S3 server can record uploaded keys clearly.
		AccessKeyID:     "test-access-key", // Provide static credentials so the AWS SDK does not depend on ambient workstation configuration.
		SecretAccessKey: "test-secret-key", // Provide static credentials so the AWS SDK does not depend on ambient workstation configuration.
		Endpoint:        s3Server.baseURL,  // Point the production S3 client at the fake loopback server so PutObject runs over real HTTP.
		ForcePathStyle:  true,              // Force path-style URLs so the fake S3 server can accept bucket-prefixed paths without custom host routing.
	})
	if err != nil { // Stop immediately when the production S3 client cannot be constructed against the fake endpoint.
		t.Fatalf("failed to create fake-backed s3 client: %v", err) // Surface the client-construction failure because the artifact flow depends on real uploads.
	}

	service := orchestrator.NewService(config.OrchestratorConfig{ // Construct the production monolith orchestrator service with a live fake S3 client for the artifact flow.
		PlanTimeout:   10 * time.Second,                   // Use one bounded plan timeout so the artifact flow fails promptly when execution stalls.
		StepTimeout:   3 * time.Second,                    // Use one bounded per-step timeout so fake Appium calls remain prompt and deterministic.
		AutoWaitMax:   500 * time.Millisecond,             // Use one short auto-wait ceiling so missing fake elements fail quickly during integration debugging.
		SnapshotTTL:   time.Minute,                        // Use one positive snapshot TTL because the orchestrator config expects it even though this test does not read snapshots.
		ExecutionMode: orchestrator.ExecutionModeMonolith, // Exercise the monolith path because this artifact test focuses on screenshot persistence rather than distributed worker callbacks.
	}, config.RPCSecurityConfig{}, config.WorkerConfig{
		AppiumURL: appiumServer.baseURL, // Point the service at the fake Appium loopback server so StartSession and TakeScreenshot exercise real HTTP behavior.
	}, config.AWSConfig{}, config.DeviceFarmConfig{Mode: "disabled"}, dao, cache, s3Client) // Disable Device Farm because the artifact integration flow does not touch it.

	runCtx, cancel := context.WithCancel(context.Background()) // Create one service lifecycle context so dispatcher and registry loops can be started and later stopped deterministically.
	if err := service.Start(runCtx); err != nil {              // Start the production orchestrator service loops before enqueueing any integration flow plan.
		t.Fatalf("failed to start orchestrator service: %v", err) // Surface startup failures because the artifact flow requires a running dispatcher.
	}
	t.Cleanup(func() {
		cancel()       // Cancel the service lifecycle context so background loops begin shutting down.
		service.Stop() // Stop the orchestrator service explicitly so dispatcher and registry goroutines release resources promptly.
	})

	return &artifactFlowHarness{dao: dao, cache: cache, service: service, appium: appiumServer, s3: s3Server, cancel: cancel} // Return the fully wired harness so the integration test can drive the artifact flow and assert side effects.
}

// TestMonolithTakeScreenshotPersistsArtifacts verifies one screenshot flow that uploads artifact bytes, stores artifact metadata, and returns presigned references.
func TestMonolithTakeScreenshotPersistsArtifacts(t *testing.T) {
	harness := newArtifactFlowHarness(t) // Start one isolated artifact integration harness so the flow can exercise real storage, cache, Appium HTTP, and S3 HTTP behavior.
	ctx := context.Background()          // Use one shared background context because the artifact flow runs synchronously inside the current test.

	sessionCaps := map[string]interface{}{ // Build one representative session capability set so StartSession exercises real Appium payload creation.
		"platformName":      "Android",  // Provide a platform name because Appium session creation expects one realistic capability payload.
		"appium:udid":       "emulator", // Provide one vendor-prefixed capability so the fake Appium server receives a realistic W3C payload.
		"appium:appPackage": "demo.app", // Provide one package capability so the session payload resembles a real Android automation request.
	}
	session, err := harness.service.StartSession(ctx, "artifact-project", sessionCaps) // Execute the production StartSession flow so screenshot capture later reuses one live Appium session.
	if err != nil {                                                                    // Fail immediately when session creation unexpectedly fails because the rest of the artifact flow depends on one live session.
		t.Fatalf("expected start session to succeed, got error: %v", err) // Surface the session-creation failure so artifact integration regressions are easy to diagnose.
	}

	planJSON := json.RawMessage(`{"steps":[{"type":"wait","params":{"ms":1}}]}`)        // Build one tiny plan so the artifact trace id comes from the normal plan-execution path rather than manual seeding.
	traceID, err := harness.service.ExecutePlanWithTrace(ctx, session.ID, "", planJSON) // Execute the production plan-enqueue path so one real persisted trace exists before screenshot capture.
	if err != nil {                                                                     // Fail immediately when plan submission unexpectedly fails because the artifact flow under test depends on one valid trace id.
		t.Fatalf("expected execute plan to succeed, got error: %v", err) // Surface the enqueue failure so artifact regressions are easy to diagnose.
	}
	waitForTraceTerminalState(t, ctx, harness.service, traceID, "completed") // Wait until the trace reaches a stable terminal state so screenshot capture attaches to a known good execution record.

	result, err := harness.service.TakeScreenshot(ctx, session.ID, traceID, true) // Execute the production screenshot flow so Appium capture, S3 upload, artifact metadata persistence, and presign all run for real.
	if err != nil {                                                               // Fail immediately when screenshot capture unexpectedly fails because this test exists to validate the artifact path end to end.
		t.Fatalf("expected take screenshot to succeed, got error: %v", err) // Surface the screenshot failure so artifact regressions are easy to diagnose.
	}

	fullRef, ok := result["full"].(map[string]interface{}) // Read the full-image artifact reference returned by the production screenshot flow.
	if !ok {                                               // Fail the test when the screenshot result does not expose the expected full-image artifact map.
		t.Fatalf("expected full artifact reference map, got %#v", result["full"]) // Surface the actual payload so artifact response regressions are easy to diagnose.
	}
	thumbRef, ok := result["thumb"].(map[string]interface{}) // Read the thumbnail artifact reference returned by the production screenshot flow.
	if !ok {                                                 // Fail the test when the screenshot result does not expose the expected thumbnail artifact map.
		t.Fatalf("expected thumb artifact reference map, got %#v", result["thumb"]) // Surface the actual payload so artifact response regressions are easy to diagnose.
	}
	if !strings.Contains(fullRef["url"].(string), "artifact-bucket") { // Assert that the production presign path returned one bucket-qualified URL for the full artifact.
		t.Fatalf("expected full artifact presign URL to mention artifact-bucket, got %#v", fullRef["url"]) // Surface the actual URL so presign regressions are easy to diagnose.
	}
	if !strings.Contains(thumbRef["url"].(string), "artifact-bucket") { // Assert that the production presign path returned one bucket-qualified URL for the thumbnail artifact.
		t.Fatalf("expected thumb artifact presign URL to mention artifact-bucket, got %#v", thumbRef["url"]) // Surface the actual URL so presign regressions are easy to diagnose.
	}

	artifacts, err := harness.service.GetArtifacts(ctx, traceID) // Read back the persisted artifact metadata through the production service API after screenshot capture.
	if err != nil {                                              // Fail immediately when the service cannot list the persisted artifacts.
		t.Fatalf("expected artifacts to remain readable, got error: %v", err) // Surface the artifact-list failure so metadata regressions are easy to diagnose.
	}
	if len(artifacts) != 2 { // Assert that the screenshot flow persisted both the full image and the thumbnail metadata rows.
		t.Fatalf("expected 2 persisted artifacts after screenshot flow, got %d", len(artifacts)) // Surface the actual artifact count so metadata regressions are easy to diagnose.
	}

	harness.s3.mu.Lock()                         // Protect the recorded fake S3 state while the final assertions read the accumulated uploaded objects.
	recordedObjects := map[string][]byte{}       // Allocate one copy of the fake S3 object map so assertions can proceed after the lock is released.
	for key, value := range harness.s3.objects { // Copy every recorded fake S3 object so assertions remain stable after the lock is released.
		recordedObjects[key] = append([]byte(nil), value...) // Copy the uploaded object bytes so the later assertions do not alias shared mutable state.
	}
	harness.s3.mu.Unlock() // Release the fake S3 server state lock before running assertions.

	if len(recordedObjects) != 2 { // Assert that the production S3 client uploaded both the full image and the thumbnail object bytes.
		t.Fatalf("expected 2 uploaded objects after screenshot flow, got %#v", recordedObjects) // Surface the actual uploaded object set so storage regressions are easy to diagnose.
	}
	for objectPath, objectBytes := range recordedObjects { // Assert that every recorded fake S3 object path belongs to the expected trace and contains the fake Appium screenshot bytes.
		if !strings.HasPrefix(objectPath, "artifact-bucket/"+traceID+"/") { // Fail when the uploaded object path does not include the expected bucket and trace prefix.
			t.Fatalf("expected uploaded object path to include artifact-bucket/%s prefix, got %s", traceID, objectPath) // Surface the unexpected path so key-layout regressions are easy to diagnose.
		}
		cfg, err := png.DecodeConfig(bytes.NewReader(objectBytes)) // Decode the uploaded image dimensions so the test can distinguish the full screenshot from the resized thumbnail artifact.
		if err != nil {                                            // Fail immediately when the uploaded bytes are not valid PNG artifacts.
			t.Fatalf("expected uploaded object bytes to be valid png, got error: %v", err) // Surface the decode failure so screenshot-upload regressions are easy to diagnose.
		}
		switch {
		case strings.Contains(objectPath, "screenshot_full.png"):
			if cfg.Width != 800 || cfg.Height != 400 { // Assert that the full screenshot preserves the fake Appium screenshot dimensions.
				t.Fatalf("expected full screenshot dimensions 800x400, got %dx%d", cfg.Width, cfg.Height) // Surface the actual dimensions so full-screenshot regressions are easy to diagnose.
			}
		case strings.Contains(objectPath, "screenshot_thumb.png"):
			if cfg.Width != 640 || cfg.Height != 320 { // Assert that the thumbnail path now stores one genuinely resized 640px-long-edge image.
				t.Fatalf("expected thumbnail dimensions 640x320, got %dx%d", cfg.Width, cfg.Height) // Surface the actual dimensions so thumbnail regressions are easy to diagnose.
			}
		default:
			t.Fatalf("expected uploaded object path to be screenshot_full.png or screenshot_thumb.png, got %s", objectPath) // Surface unexpected artifact object names so artifact-layout regressions are easy to diagnose.
		}
	}
}
