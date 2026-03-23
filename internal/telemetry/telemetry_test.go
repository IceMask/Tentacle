// telemetry_test.go verifies selected direct helpers in the telemetry package.
package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

// TestInitLoggerMasksSecrets verifies that structured log output redacts common sensitive values before they reach the configured sink.
func TestInitLoggerMasksSecrets(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "masked.log")                 // Build one isolated log-file path so the production logger can write a deterministic test record.
	InitLogger("info", logPath)                                         // Reconfigure the production logger to write JSON records into the isolated log file.
	Logger().Info("masked-test", "payload", `{"token":"secret-value"}`) // Emit one log record containing a sensitive token so the logger's masking hook can be asserted directly.
	logBytes, err := os.ReadFile(logPath)                               // Read back the configured log file so the emitted structured record can be inspected directly.
	if err != nil {                                                     // Fail the test when the configured log file cannot be read after the logger writes to it.
		t.Fatalf("expected log file read to succeed, got error: %v", err) // Surface the unexpected read failure so logger regressions are obvious.
	}
	if bytes.Contains(logBytes, []byte("secret-value")) { // Fail the test when the raw sensitive token value reaches the configured log sink.
		t.Fatalf("expected secret value to be masked, got %s", string(logBytes)) // Surface the emitted record so masking regressions are obvious.
	}
	if !bytes.Contains(logBytes, []byte(`***`)) { // Fail the test when the logger does not emit any masked token marker in the serialized log payload.
		t.Fatalf("expected masked token marker, got %s", string(logBytes)) // Surface the emitted record so masking regressions are obvious.
	}
}

// TestWithContextAttachesTraceIdentifiers verifies that loggers derived from a traced context emit trace and span identifiers.
func TestWithContextAttachesTraceIdentifiers(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "trace.log")                                            // Build one isolated log-file path so the production logger can write a deterministic traced record.
	InitLogger("info", logPath)                                                                   // Reconfigure the production logger to write JSON records into the isolated log file.
	tracerProvider := sdktrace.NewTracerProvider()                                                // Construct one SDK tracer provider so new spans carry valid trace and span identifiers.
	defer func() { _ = tracerProvider.Shutdown(context.Background()) }()                          // Ensure the tracer provider always shuts down after the test completes.
	otel.SetTracerProvider(tracerProvider)                                                        // Install the SDK tracer provider globally so the traced context contains valid span identifiers.
	ctx, span := tracerProvider.Tracer("telemetry-test").Start(context.Background(), "operation") // Start one real span so WithContext can extract valid trace metadata.
	WithContext(ctx).Info("traced-log")                                                           // Emit one log record through the context-enriched logger so trace fields are written to the sink.
	span.End()                                                                                    // End the test span after the traced log record has been emitted.

	logBytes, err := os.ReadFile(logPath) // Read back the configured log file so the emitted structured record can be inspected directly.
	if err != nil {                       // Fail the test when the configured log file cannot be read after the logger writes to it.
		t.Fatalf("expected traced log file read to succeed, got error: %v", err) // Surface the unexpected read failure so logger regressions are obvious.
	}
	lines := bytes.Split(bytes.TrimSpace(logBytes), []byte("\n")) // Split the file into JSON log lines so the final traced record can be decoded directly.
	if len(lines) == 0 {                                          // Fail the test when the configured logger does not write any record at all.
		t.Fatal("expected at least one traced log line") // Surface the missing output because trace-enriched logging regressions would otherwise go unnoticed.
	}
	var payload map[string]interface{}                                    // Allocate the destination map used to decode the final traced JSON log record.
	if err := json.Unmarshal(lines[len(lines)-1], &payload); err != nil { // Decode the final traced log line so trace and span identifiers can be asserted directly.
		t.Fatalf("expected traced log line to be valid JSON, got error: %v", err) // Surface the malformed line so logger regressions are obvious.
	}
	if payload["trace_id"] == "" || payload["span_id"] == "" { // Fail the test when the context-enriched logger does not include trace identifiers in the emitted record.
		t.Fatalf("expected trace_id and span_id, got %#v", payload) // Surface the emitted record so trace-correlation regressions are obvious.
	}
}
