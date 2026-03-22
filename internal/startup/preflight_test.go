// preflight_test.go covers startup configuration validation and Appium dependency probes with lightweight unit tests.
package startup

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
)

// newValidConfig returns a minimally valid config value that individual tests can mutate to exercise one startup failure at a time.
func newValidConfig() *config.Config {
	return &config.Config{ // Return one fully populated config so each test can focus on a single invalid field.
		Gateway: config.GatewayConfig{ // Seed the gateway section with a valid HTTP-mode baseline.
			Port:            8080,             // Use a valid HTTP listener port for baseline startup validation.
			EnableTLS:       false,            // Keep TLS disabled in the baseline config so tests can opt into TLS only when needed.
			ShutdownTimeout: 30 * time.Second, // Use a positive shutdown timeout for the baseline config.
		},
		Orchestrator: config.OrchestratorConfig{ // Seed the orchestrator section with a valid baseline execution config.
			GRPCPort:      9090,                    // Use a valid orchestrator gRPC port for baseline startup validation.
			PlanTimeout:   10 * time.Minute,        // Use a positive plan timeout for the baseline config.
			StepTimeout:   30 * time.Second,        // Use a positive step timeout for the baseline config.
			AutoWaitMax:   5 * time.Second,         // Use a positive auto-wait ceiling for the baseline config.
			SnapshotTTL:   1500 * time.Millisecond, // Use a positive snapshot TTL for the baseline config.
			ExecutionMode: "monolith",              // Use the gateway-supported execution mode for the baseline config.
		},
		Worker: config.WorkerConfig{ // Seed the worker section with valid connectivity defaults.
			AppiumURL:         "http://127.0.0.1:4723", // Use a syntactically valid Appium base URL for the baseline config.
			Concurrency:       4,                       // Use a positive worker concurrency for the baseline config.
			HeartbeatInterval: 10 * time.Second,        // Use a positive heartbeat interval for the baseline config.
			OrchestratorAddr:  "127.0.0.1:9090",        // Use a valid orchestrator target address for the baseline config.
			GRPCPort:          9092,                    // Use a valid worker gRPC port for the baseline config.
		},
		Storage: config.StorageConfig{ // Seed the storage section with syntactically valid connection settings.
			Postgres: config.PostgresConfig{ // Use a valid Postgres DSN and pool sizing for baseline validation.
				DSN:             "postgres://postgres:postgres@127.0.0.1:5432/mcp_mobile_worker?sslmode=disable", // Use a valid DSN shape for baseline startup validation.
				MaxOpenConns:    25,                                                                              // Use a positive max-open pool size for the baseline config.
				MaxIdleConns:    5,                                                                               // Use a positive max-idle pool size for the baseline config.
				ConnMaxLifetime: 5 * time.Minute,                                                                 // Use a positive connection lifetime for the baseline config.
			},
			Redis: config.RedisConfig{ // Use a valid Redis client address for baseline validation.
				Addr: "127.0.0.1:6379", // Use a valid host:port Redis address for the baseline config.
			},
			S3: config.S3Config{ // Use a valid artifact storage baseline for startup validation.
				Region: "us-east-1",     // Use a non-empty S3 region for the baseline config.
				Bucket: "mcp-artifacts", // Use a non-empty artifact bucket for the baseline config.
			},
		},
		AWS: config.AWSConfig{ // Seed the AWS section with a valid region for optional Device Farm validation.
			Region: "us-west-2", // Use a non-empty AWS region for the baseline config.
		},
		DeviceFarm: config.DeviceFarmConfig{ // Keep Device Farm disabled in the baseline config unless a test opts into another mode.
			Mode: "disabled", // Disable Device Farm by default so tests can opt into run_api or test_grid explicitly.
		},
		WebSocket: config.WebSocketConfig{ // Seed the WebSocket section with a valid short-lived subscription-token TTL for baseline startup validation.
			SubscriptionTokenTTL: 60 * time.Second, // Use a positive browser subscription-token TTL for the baseline startup config.
		},
	} // Return the fully populated baseline config for mutation by individual tests.
}

// startLoopbackHTTPServer starts a minimal IPv4 loopback HTTP server for dependency-probe tests and returns its base URL plus a shutdown function.
func startLoopbackHTTPServer(t *testing.T, handler http.Handler) (string, func()) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind an IPv4 loopback listener explicitly because the sandbox blocks the default IPv6 httptest listener.
	if err != nil {                                    // Skip the test when even explicit loopback listeners are not permitted in the current environment.
		t.Skipf("skipping loopback HTTP probe test because listen failed: %v", err) // Surface the environment limitation without flagging the production code as broken.
	}
	server := &http.Server{Handler: handler} // Construct a minimal HTTP server that serves only the handler required by the current test.
	go func() {
		_ = server.Serve(listener) // Serve requests in the background so the startup probe can hit the loopback endpoint during the test.
	}()
	return "http://" + listener.Addr().String(), func() { // Return the bound base URL together with a shutdown closure for the caller.
		_ = server.Close()   // Stop the HTTP server once the dependency-probe test completes.
		_ = listener.Close() // Close the listener explicitly so the loopback port is released immediately.
	}
}

// TestValidateGatewayStartupRejectsMissingBucket verifies that gateway startup fails fast when artifact storage is not configured.
func TestValidateGatewayStartupRejectsMissingBucket(t *testing.T) {
	cfg := newValidConfig()                   // Start from a fully valid baseline config so this test mutates only the bucket field.
	cfg.Storage.S3.Bucket = ""                // Clear the artifact bucket so gateway startup validation should reject the config.
	err := ValidateGatewayStartup(cfg, false) // Run gateway startup validation in HTTP mode against the mutated config.
	if err == nil {                           // Fail the test when validation unexpectedly accepts a missing artifact bucket.
		t.Fatal("expected gateway startup validation to reject an empty s3 bucket") // Surface the missing expected validation error clearly.
	}
}

// TestValidateGatewayStartupRejectsMissingTLSFiles verifies that HTTP-mode gateway startup fails when TLS is enabled without readable key material.
func TestValidateGatewayStartupRejectsMissingTLSFiles(t *testing.T) {
	cfg := newValidConfig()                      // Start from a fully valid baseline config so this test mutates only the TLS fields.
	cfg.Gateway.EnableTLS = true                 // Enable TLS so the gateway startup validator must check the configured key-pair paths.
	cfg.Gateway.TLSCertFile = "/tmp/missing.crt" // Point the certificate path at a definitely missing file.
	cfg.Gateway.TLSKeyFile = "/tmp/missing.key"  // Point the key path at a definitely missing file.
	err := ValidateGatewayStartup(cfg, false)    // Run gateway startup validation in HTTP mode against the mutated TLS config.
	if err == nil {                              // Fail the test when validation unexpectedly accepts missing TLS files.
		t.Fatal("expected gateway startup validation to reject missing tls files") // Surface the missing expected validation error clearly.
	}
}

// TestValidateWorkerStartupRejectsInvalidAppiumURL verifies that worker startup fails fast on malformed Appium URLs.
func TestValidateWorkerStartupRejectsInvalidAppiumURL(t *testing.T) {
	cfg := newValidConfig()            // Start from a fully valid baseline config so this test mutates only the Appium URL field.
	cfg.Worker.AppiumURL = "://broken" // Replace the valid Appium URL with a malformed URL string.
	err := ValidateWorkerStartup(cfg)  // Run worker startup validation against the mutated config.
	if err == nil {                    // Fail the test when validation unexpectedly accepts a malformed Appium URL.
		t.Fatal("expected worker startup validation to reject a malformed appium url") // Surface the missing expected validation error clearly.
	}
}

// TestValidateWorkerStartupRejectsTLSRPCWithoutFiles verifies that worker startup fails fast when internal RPC TLS mode is enabled without readable server certificate material.
func TestValidateWorkerStartupRejectsTLSRPCWithoutFiles(t *testing.T) {
	cfg := newValidConfig()                                  // Start from a fully valid baseline config so this test mutates only the internal RPC security fields.
	cfg.RPC.Security.Mode = "tls"                            // Enable internal RPC TLS mode so startup validation must require server certificate material and a shared auth token.
	cfg.RPC.Security.AuthToken = "shared-rpc-token"          // Provide the shared RPC auth token so the test isolates the missing-TLS-file failure path.
	cfg.RPC.Security.ServerCertFile = "/tmp/missing-rpc.crt" // Point the server certificate path at a definitely missing file.
	cfg.RPC.Security.ServerKeyFile = "/tmp/missing-rpc.key"  // Point the server key path at a definitely missing file.
	err := ValidateWorkerStartup(cfg)                        // Run worker startup validation against the mutated internal RPC TLS config.
	if err == nil {                                          // Fail the test when validation unexpectedly accepts missing internal RPC TLS files.
		t.Fatal("expected worker startup validation to reject missing internal rpc tls files") // Surface the missing expected validation error clearly.
	}
}

// TestCheckAppiumReachabilityAcceptsHealthyStatus verifies that the Appium startup probe accepts a healthy status endpoint.
func TestCheckAppiumReachabilityAcceptsHealthyStatus(t *testing.T) {
	baseURL, stopServer := startLoopbackHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Start a lightweight HTTP server that mimics a healthy Appium status endpoint on IPv4 loopback.
		if r.URL.Path != "/status" { // Fail the request when the startup probe hits an unexpected path.
			http.NotFound(w, r) // Return 404 so the test still exercises the probe's status-code handling.
			return              // Stop the handler once the unexpected path response has been written.
		}
		w.WriteHeader(http.StatusOK) // Return HTTP 200 so the startup probe treats the dependency as healthy.
	})) // Close the handler literal so the loopback server helper can start it.
	defer stopServer() // Ensure the temporary IPv4 loopback HTTP server is stopped after the test completes.

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Bound the startup probe so the unit test fails fast on hangs.
	defer cancel()                                                          // Release the timeout resources after the probe finishes.
	if err := CheckAppiumReachability(ctx, baseURL); err != nil {           // Run the startup probe against the healthy fake Appium endpoint.
		t.Fatalf("expected healthy appium probe to succeed, got error: %v", err) // Surface the unexpected probe failure with the returned error.
	}
}

// TestCheckAppiumReachabilityRejectsServerError verifies that the Appium startup probe rejects unhealthy status responses.
func TestCheckAppiumReachabilityRejectsServerError(t *testing.T) {
	baseURL, stopServer := startLoopbackHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Start a lightweight HTTP server that mimics an unhealthy Appium status endpoint on IPv4 loopback.
		w.WriteHeader(http.StatusInternalServerError) // Return HTTP 500 so the startup probe must reject the dependency as unhealthy.
	})) // Close the handler literal so the loopback server helper can start it.
	defer stopServer() // Ensure the temporary IPv4 loopback HTTP server is stopped after the test completes.

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Bound the startup probe so the unit test fails fast on hangs.
	defer cancel()                                                          // Release the timeout resources after the probe finishes.
	err := CheckAppiumReachability(ctx, baseURL)                            // Run the startup probe against the unhealthy fake Appium endpoint.
	if err == nil {                                                         // Fail the test when the probe unexpectedly accepts an unhealthy dependency.
		t.Fatal("expected unhealthy appium probe to fail") // Surface the missing expected probe failure clearly.
	}
}

// TestCheckAppiumReachabilityRejectsTimeout verifies that the Appium startup probe fails fast when the status endpoint accepts the connection but never sends a response.
func TestCheckAppiumReachabilityRejectsTimeout(t *testing.T) {
	baseURL, stopServer := startLoopbackHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Start a lightweight HTTP server that intentionally hangs until the client cancels the request context.
		<-r.Context().Done() // Hold the handler open without writing any response so the client-side startup probe must fail on its timeout.
	})) // Close the handler literal so the loopback server helper can start it.
	defer stopServer() // Ensure the temporary IPv4 loopback HTTP server is stopped after the test completes.

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond) // Bound the startup probe with a short timeout so the unresponsive endpoint test completes quickly.
	defer cancel()                                                                // Release the timeout resources after the probe finishes.
	startedAt := time.Now()                                                       // Capture the probe start time so the test can verify the failure happens promptly.
	err := CheckAppiumReachability(ctx, baseURL)                                  // Run the startup probe against the intentionally unresponsive fake Appium endpoint.
	elapsed := time.Since(startedAt)                                              // Measure the wall-clock duration so the test can verify fail-fast timeout behavior.
	if err == nil {                                                               // Fail the test when the probe unexpectedly accepts an unresponsive dependency.
		t.Fatal("expected unresponsive appium probe to fail") // Surface the missing expected timeout failure clearly.
	}
	if !errors.IsCode(err, errors.CodeHealthDown) { // Fail the test when the returned error code does not classify the timeout as a dependency-health failure.
		t.Fatalf("expected E.HEALTH.DOWN for unresponsive appium probe, got: %v", err) // Surface the unexpected error classification because startup callers rely on that code.
	}
	if elapsed >= time.Second { // Fail the test when the probe hangs far beyond its timeout budget instead of failing fast.
		t.Fatalf("expected unresponsive appium probe to fail fast, took %v", elapsed) // Surface the slow failure because startup checks must not stall process boot indefinitely.
	}
}
