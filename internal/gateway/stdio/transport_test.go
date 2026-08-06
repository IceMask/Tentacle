// transport_test.go verifies direct stdio transport request parsing and JSON-RPC response writing behavior.
package stdio

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"mcp_for_appium/internal/gateway/jsonrpc"
)

// TestHandleRequestWritesParseError verifies that malformed JSON input produces the standard JSON-RPC parse-error response.
func TestHandleRequestWritesParseError(t *testing.T) {
	var stdout bytes.Buffer                                                                                  // Capture the transport's response stream in memory so the emitted JSON-RPC error can be asserted directly.
	transport := &Transport{handler: jsonrpc.NewHandler(nil, nil), stdout: &stdout, stderr: &bytes.Buffer{}} // Construct one transport with an in-memory stdout sink because this test exercises only request parsing and response writing.

	if err := transport.handleRequest(context.Background(), []byte("{not-json")); err != nil { // Process one malformed JSON payload through the production request handler.
		t.Fatalf("expected parse error to be written successfully, got error: %v", err) // Surface unexpected transport write failures because the request parser should still emit a response.
	}
	if !strings.Contains(stdout.String(), `"code":-32700`) { // Fail the test when the transport does not emit the standard JSON-RPC parse-error code.
		t.Fatalf("expected parse error response, got %s", stdout.String()) // Surface the emitted output so parsing-regression diagnosis is straightforward.
	}
}

// TestProcessRequestRejectsInvalidVersion verifies that the stdio transport blocks non-2.0 requests before delegating to business dispatch.
func TestProcessRequestRejectsInvalidVersion(t *testing.T) {
	transport := &Transport{handler: jsonrpc.NewHandler(nil, nil)}                                           // Construct one transport with a production JSON-RPC handler because this test exercises only the protocol-version gate.
	request := &jsonrpc.Request{JSONRPC: "1.0", Method: "describeCapabilities", Params: []byte(`{}`), ID: 1} // Build one request with an invalid JSON-RPC version so the protocol gate can be exercised directly.

	if _, err := transport.processRequest(context.Background(), request); err == nil { // Process the invalid-version request through the production stdio transport gate.
		t.Fatal("expected invalid jsonrpc version to fail") // Surface the missing failure because the transport must reject non-2.0 requests deterministically.
	}
}

// TestHandleRequestSuppressesInitializedNotificationResponse verifies that stdio consumes initialized notifications without writing any JSON-RPC response line.
func TestHandleRequestSuppressesInitializedNotificationResponse(t *testing.T) {
	var stdout bytes.Buffer                                                                                  // Capture the transport stdout stream in memory so notification suppression can be asserted directly.
	transport := &Transport{handler: jsonrpc.NewHandler(nil, nil), stdout: &stdout, stderr: &bytes.Buffer{}} // Construct one transport with in-memory sinks because this test exercises only stdio notification handling.

	if err := transport.handleRequest(context.Background(), []byte(`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`)); err != nil { // Process one initialized notification through the production stdio handler.
		t.Fatalf("expected initialized notification to be handled successfully, got error: %v", err) // Surface unexpected transport failures because notifications should be consumed quietly.
	}
	if stdout.Len() != 0 { // Fail when the stdio transport still writes one JSON-RPC response for notifications.
		t.Fatalf("expected no stdout output for notification, got %q", stdout.String()) // Surface the unexpected stdout payload so notification regressions are obvious.
	}
}

// TestHandleRequestCancelledNotificationSuppressesResponseAndCancelsInflightRequest verifies stdio cancellation notification behavior.
func TestHandleRequestCancelledNotificationSuppressesResponseAndCancelsInflightRequest(t *testing.T) {
	var stdout bytes.Buffer                                                             // Capture the transport stdout stream in memory so cancellation notifications can be asserted as silent.
	handler := jsonrpc.NewHandler(nil, nil)                                             // Construct one shared handler so the test can register an in-flight request and then cancel it through stdio.
	transport := &Transport{handler: handler, stdout: &stdout, stderr: &bytes.Buffer{}} // Construct one transport with in-memory sinks because this test exercises only stdio cancellation handling.
	requestCtx, cancel := context.WithCancel(context.Background())                      // Create one cancellable context that stands in for an active request owned by the transport.
	cleanup := handler.RegisterInFlightRequest("stdio-cancel", cancel)                  // Register the active request id exactly as stdio request dispatch does before handler execution.
	defer cleanup()                                                                     // Ensure the registry entry is removed if the test fails before cancellation.

	if err := transport.handleRequest(context.Background(), []byte(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"stdio-cancel","reason":"client cancelled"}}`)); err != nil { // Process one cancellation notification through the production stdio handler.
		t.Fatalf("expected cancellation notification to be handled successfully, got error: %v", err) // Surface unexpected transport failures because cancellation notifications should be consumed quietly.
	}
	select { // Check the registered request context synchronously because cancellation is invoked before handleRequest returns.
	case <-requestCtx.Done(): // Observe context cancellation to prove the stdio notification reached the shared in-flight registry.
	default:
		t.Fatal("expected stdio cancellation notification to cancel registered request") // Surface the missing cancellation because MCP clients rely on notifications/cancelled for long calls.
	}
	if stdout.Len() != 0 { // Fail when the stdio transport writes any JSON-RPC response for a cancellation notification.
		t.Fatalf("expected no stdout output for cancellation notification, got %q", stdout.String()) // Surface the unexpected stdout payload so notification regressions are obvious.
	}
}
