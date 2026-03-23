// appium_local_test.go verifies local loopback Appium auto-start behavior without depending on one real Appium installation.
package startup

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"mcp_for_appium/internal/errors"
)

// TestLocalAppiumSupervisorEnsureReadyStartsLoopbackHelper verifies that the supervisor auto-starts one local loopback Appium helper process when no server is already listening.
func TestLocalAppiumSupervisorEnsureReadyStartsLoopbackHelper(t *testing.T) {
	baseURL := unusedLoopbackAppiumURL(t)                     // Reserve one currently unused loopback Appium URL so the supervisor must exercise the local auto-start path.
	supervisor := NewLocalAppiumSupervisor(baseURL)           // Construct one supervisor bound to the currently unused loopback Appium URL under test.
	supervisor.lookPath = func(name string) (string, error) { // Replace executable lookup with a deterministic helper pseudo-path so the test never depends on a real Appium binary.
		return "test-helper-appium", nil // Return one synthetic executable path because the helper-process command factory ignores the real binary path.
	}
	supervisor.commandFactory = helperCommandFactory(t, "serve") // Replace child-process construction with the current test binary so the test owns the fake Appium lifecycle fully.
	defer func() {                                               // Stop any helper process that the supervisor started so the test leaves no orphaned background child process behind.
		if err := supervisor.Stop(); err != nil { // Fail loudly when helper shutdown does not complete because later tests could inherit one stale listening process otherwise.
			t.Fatalf("expected helper stop to succeed, got error: %v", err) // Surface the unexpected shutdown error so local process-lifecycle regressions are obvious.
		}
	}() // Close the helper-process shutdown defer block.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Bound the supervisor readiness call so the test fails fast on startup hangs.
	defer cancel()                                                          // Release the readiness timeout resources after the supervisor call completes.
	if err := supervisor.EnsureReady(ctx); err != nil {                     // Ask the supervisor to auto-start the missing loopback Appium endpoint and wait for readiness.
		t.Fatalf("expected helper-backed local appium auto-start to succeed, got error: %v", err) // Surface the unexpected auto-start failure together with the returned error chain.
	}
	if err := CheckAppiumReachability(ctx, baseURL); err != nil { // Re-probe the same loopback Appium URL to prove the helper process is now serving a healthy status endpoint.
		t.Fatalf("expected helper-backed appium endpoint to be reachable after EnsureReady, got error: %v", err) // Surface the unexpected post-start reachability failure clearly.
	}
}

// TestLocalAppiumSupervisorEnsureReadyReturnsMissingExecutable verifies that the supervisor returns one direct startup error when the Appium executable cannot be found locally.
func TestLocalAppiumSupervisorEnsureReadyReturnsMissingExecutable(t *testing.T) {
	baseURL := unusedLoopbackAppiumURL(t)                     // Reserve one currently unused loopback Appium URL so the supervisor must attempt local auto-start.
	supervisor := NewLocalAppiumSupervisor(baseURL)           // Construct one supervisor bound to the currently unused loopback Appium URL under test.
	supervisor.lookPath = func(name string) (string, error) { // Replace executable lookup with one deterministic failure so the test stays independent of the machine PATH.
		return "", fmt.Errorf("appium not installed") // Return one synthetic lookup failure because the missing-executable path is the behavior under test.
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Bound the supervisor readiness call so the test fails fast on unexpected hangs.
	defer cancel()                                                          // Release the readiness timeout resources after the supervisor call completes.
	err := supervisor.EnsureReady(ctx)                                      // Ask the supervisor to auto-start the missing loopback Appium endpoint, which must fail on executable lookup.
	if err == nil {                                                         // Fail the test when the supervisor unexpectedly reports success without any available Appium executable.
		t.Fatal("expected missing local appium executable to fail") // Surface the missing expected startup failure clearly.
	}
	if !errors.IsCode(err, errors.CodeHealthDown) { // Fail the test when the startup error is not classified as one unavailable local Appium dependency.
		t.Fatalf("expected E.HEALTH.DOWN for missing local appium executable, got: %v", err) // Surface the unexpected error classification because callers map this code to user-facing availability failures.
	}
	if !strings.Contains(err.Error(), "failed to find local appium executable") { // Fail the test when the returned error message does not explain the real root cause succinctly.
		t.Fatalf("expected missing-executable message, got: %v", err) // Surface the unexpected error text so diagnostics regressions are obvious.
	}
}

// TestLocalAppiumSupervisorHelperProcess runs inside a spawned helper subprocess and serves one minimal Appium-compatible /status endpoint for supervisor tests.
func TestLocalAppiumSupervisorHelperProcess(t *testing.T) {
	if os.Getenv("MCP_FAKE_APPIUM_HELPER") != "1" { // Skip normal parent test runs because only dedicated helper subprocesses should execute the embedded fake Appium server path.
		return // Return immediately so the parent test binary does not accidentally start one fake Appium server during normal execution.
	}
	if os.Getenv("MCP_FAKE_APPIUM_MODE") != "serve" { // Reject unsupported helper modes explicitly so failing test setup produces one obvious subprocess error.
		fmt.Fprintln(os.Stderr, "unsupported fake appium helper mode") // Emit one clear helper-setup error to stderr so parent-process diagnostics stay readable.
		os.Exit(2)                                                     // Exit with one non-zero status so the parent supervisor surfaces the subprocess bootstrap failure clearly.
	}
	address, port, basePath, err := parseFakeAppiumHelperArgs(os.Args) // Parse the forwarded Appium CLI flags so the helper binds the exact host, port, and base path requested by the supervisor.
	if err != nil {                                                    // Stop immediately when the parent supervisor forwarded one malformed helper argument set.
		fmt.Fprintln(os.Stderr, err.Error()) // Emit the exact helper-argument parse failure to stderr so the parent supervisor includes it in diagnostics.
		os.Exit(2)                           // Exit with one non-zero status so the parent supervisor surfaces the malformed helper command line clearly.
	}
	statusPath := strings.TrimRight(basePath, "/") + "/status" // Derive the exact Appium status path from the optional base path so helper reachability mirrors production Appium URL behavior.
	if statusPath == "/status" || statusPath == "status" {     // Normalize the empty-base-path case explicitly so the helper still exposes the canonical root /status endpoint.
		statusPath = "/status" // Keep the root status path canonical when no non-root Appium base path is configured.
	}
	mux := http.NewServeMux()                                                 // Construct one tiny HTTP mux that serves only the Appium-compatible status route required by the supervisor tests.
	mux.HandleFunc(statusPath, func(w http.ResponseWriter, r *http.Request) { // Serve one healthy Appium-style status response on the derived status path.
		w.Header().Set("Content-Type", "application/json") // Return one JSON content type so the helper response shape looks realistic to any future status-body parsing.
		w.WriteHeader(http.StatusOK)                       // Return HTTP 200 so the supervisor reachability probe treats the helper endpoint as healthy.
		_, _ = w.Write([]byte(`{"value":{"ready":true}}`)) // Return one tiny Appium-style JSON body so the helper mimics a realistic status payload.
	}) // Close the fake Appium status handler registration.
	server := &http.Server{Addr: net.JoinHostPort(address, port), Handler: mux}    // Bind one tiny HTTP server to the exact loopback address and port requested by the supervisor.
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed { // Serve until the parent supervisor interrupts the helper process or one unexpected server error occurs.
		fmt.Fprintln(os.Stderr, err.Error()) // Emit the exact helper server error to stderr so the parent supervisor can surface it in readiness diagnostics.
		os.Exit(2)                           // Exit with one non-zero status so the parent supervisor classifies the helper server failure as a startup error.
	}
	os.Exit(0) // Exit cleanly when the helper server shuts down normally after the parent supervisor interrupts it.
}

// helperCommandFactory returns one child-process factory that reuses the current test binary as a deterministic fake Appium server helper.
func helperCommandFactory(t *testing.T, mode string) func(string, ...string) *exec.Cmd {
	return func(name string, args ...string) *exec.Cmd { // Construct one helper subprocess for every supervisor start attempt so each test owns one isolated fake Appium lifecycle.
		commandArgs := append([]string{"-test.run=TestLocalAppiumSupervisorHelperProcess", "--"}, args...) // Forward the synthesized Appium CLI flags after "--" so the helper test can parse them deterministically.
		cmd := exec.Command(os.Args[0], commandArgs...)                                                    // Reuse the current Go test binary as the fake Appium subprocess so the test never depends on any external executable.
		cmd.Env = append(os.Environ(), "MCP_FAKE_APPIUM_HELPER=1", "MCP_FAKE_APPIUM_MODE="+mode)           // Mark the child process as the dedicated fake Appium helper and select its deterministic behavior mode.
		return cmd                                                                                         // Return the fully configured helper subprocess command to the supervisor under test.
	}
}

// unusedLoopbackAppiumURL reserves one currently unused IPv4 loopback port and returns the Appium-style base URL that should be free immediately after the listener is closed.
func unusedLoopbackAppiumURL(t *testing.T) string {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind one temporary IPv4 loopback listener so the test can discover one currently unused local TCP port deterministically.
	if err != nil {                                    // Fail the test immediately when the current environment does not permit even temporary loopback listeners.
		t.Fatalf("expected temporary loopback listen to succeed, got error: %v", err) // Surface the environment limitation clearly because the supervisor tests require local sockets.
	}
	address := listener.Addr().String()      // Snapshot the discovered host:port string before the temporary listener is closed and the port becomes reusable.
	if err := listener.Close(); err != nil { // Fail the test when the temporary loopback listener cannot be released because the later helper process must bind the same port.
		t.Fatalf("expected temporary loopback listener close to succeed, got error: %v", err) // Surface the unexpected close failure because it would invalidate the discovered free port.
	}

	return "http://" + address // Return one Appium-style base URL that points at the now-free temporary loopback host and port.
}

// parseFakeAppiumHelperArgs extracts one address, port, and optional base path from the forwarded Appium CLI arguments used by the fake helper process.
func parseFakeAppiumHelperArgs(argv []string) (string, string, string, error) {
	address := "127.0.0.1"                       // Default the helper bind address to one local loopback interface so missing helper flags still stay local-only.
	port := ""                                   // Start with one empty port so the parser can require the parent supervisor to pass one explicit bind port.
	basePath := ""                               // Default the base path to empty so the helper serves the canonical root /status endpoint unless told otherwise.
	for index := 0; index < len(argv); index++ { // Walk the forwarded helper argv slice sequentially so flag-value pairs can be parsed deterministically.
		switch argv[index] { // Parse only the small subset of Appium CLI flags that the supervisor forwards for local auto-start.
		case "--address": // Parse the forwarded bind address value.
			index++                 // Advance to the bind-address value that follows the current flag token.
			if index >= len(argv) { // Fail when the parent supervisor omitted the required address value after the flag token.
				return "", "", "", fmt.Errorf("missing value for --address") // Surface the malformed helper command line clearly.
			}
			address = argv[index] // Record the forwarded bind address so the helper listens on the expected loopback interface.
		case "--port": // Parse the forwarded bind port value.
			index++                 // Advance to the bind-port value that follows the current flag token.
			if index >= len(argv) { // Fail when the parent supervisor omitted the required port value after the flag token.
				return "", "", "", fmt.Errorf("missing value for --port") // Surface the malformed helper command line clearly.
			}
			port = argv[index] // Record the forwarded bind port so the helper listens on the expected local TCP port.
		case "--base-path": // Parse the forwarded optional Appium base path value.
			index++                 // Advance to the base-path value that follows the current flag token.
			if index >= len(argv) { // Fail when the parent supervisor omitted the required base-path value after the flag token.
				return "", "", "", fmt.Errorf("missing value for --base-path") // Surface the malformed helper command line clearly.
			}
			basePath = argv[index] // Record the forwarded base path so the helper exposes the expected /status route beneath it.
		}
	}
	if strings.TrimSpace(port) == "" { // Fail when the parent supervisor never forwarded any explicit Appium port to the helper subprocess.
		return "", "", "", fmt.Errorf("missing required --port") // Surface the malformed helper command line clearly because the helper cannot listen without one port.
	}

	return address, port, basePath, nil // Return the parsed bind address, bind port, and optional base path for the fake helper server.
}
