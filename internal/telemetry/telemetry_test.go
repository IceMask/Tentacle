// telemetry_test.go verifies selected direct helpers in the telemetry package.
package telemetry

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// TestTraceMiddlewareInvokesNext verifies that the HTTP trace middleware preserves the request flow while wrapping the handler in a span.
func TestTraceMiddlewareInvokesNext(t *testing.T) {
	called := false                                                                                            // Track whether the wrapped handler ran so the middleware can be asserted without inspecting span internals directly.
	handler := TraceMiddleware("test-service", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Wrap one trivial handler in the production trace middleware so the request path can be exercised directly.
		called = true                       // Record that the wrapped handler executed so the middleware can be asserted after the request completes.
		w.WriteHeader(http.StatusNoContent) // Return one simple success status so the request completes normally through the middleware.
	}))

	recorder := httptest.NewRecorder()                              // Capture the HTTP response so status propagation can be asserted directly.
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil) // Build one simple request that will travel through the production middleware path.
	handler.ServeHTTP(recorder, request)                            // Execute the request through the production middleware and wrapped handler.
	if !called {                                                    // Fail the test when the middleware does not forward the request to the wrapped handler.
		t.Fatal("expected wrapped handler to be called") // Surface the missing invocation because request-flow regressions would break all traced HTTP paths.
	}
	if recorder.Code != http.StatusNoContent { // Fail the test when the middleware does not preserve the wrapped handler's response status.
		t.Fatalf("expected status 204, got %d", recorder.Code) // Surface the unexpected status so middleware regressions are obvious.
	}
}

// TestBuildLogFileWriterCreatesConfiguredFile verifies that file logging setup creates the target file path and returns a writer.
func TestBuildLogFileWriterCreatesConfiguredFile(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "logs", "gateway.log") // Build one nested log-file path so the helper must create parent directories before opening the file.
	writer, err := buildLogFileWriter(logPath)                   // Construct the production file writer so directory creation and file-open logic are exercised directly.
	if err != nil {                                              // Fail the test when the helper cannot create the configured file path unexpectedly.
		t.Fatalf("expected log writer creation to succeed, got error: %v", err) // Surface the unexpected error so file-logging regressions are obvious.
	}
	if writer == nil { // Fail the test when the helper succeeds but does not return a writer.
		t.Fatal("expected non-nil log writer") // Surface the unexpected nil writer so file-logging regressions are obvious.
	}
}
