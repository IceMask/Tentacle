// handler_test.go verifies selected JSON-RPC handler and schema-validation behaviors directly at the package boundary.
package jsonrpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/capabilities"
	mcperrors "mcp_for_appium/internal/gateway/mcp"
)

// TestProcessRequestReturnsCapabilities verifies that the direct JSON-RPC describeCapabilities method delegates to the capability service.
func TestProcessRequestReturnsCapabilities(t *testing.T) {
	handler := NewHandler(nil, capabilities.NewService(config.GatewayConfig{}))                               // Construct one handler with no orchestrator dependency because describeCapabilities only depends on the capability service.
	request := &Request{JSONRPC: "2.0", Method: "describeCapabilities", Params: json.RawMessage(`{}`), ID: 1} // Build one direct describeCapabilities request so the package boundary can be exercised without HTTP transport.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute the production JSON-RPC dispatch path for the direct capability method.
	if err != nil {                                                      // Fail the test when the direct capability method unexpectedly returns an error.
		t.Fatalf("expected describeCapabilities to succeed, got error: %v", err) // Surface the unexpected error so dispatch regressions are obvious.
	}
	payload, ok := result.(map[string]interface{}) // Decode the returned capability snapshot so the direct JSON-RPC result shape can be asserted.
	if !ok {                                       // Fail the test when the direct method result does not expose the expected payload shape.
		t.Fatalf("expected capability result map, got %#v", result) // Surface the unexpected result so dispatch regressions are obvious.
	}
	if payload["apiVersion"] != "4.3.0" { // Fail the test when the direct method no longer returns the capability snapshot expected by clients.
		t.Fatalf("expected apiVersion 4.3.0, got %#v", payload["apiVersion"]) // Surface the unexpected version so dispatch regressions are obvious.
	}
}

// TestProcessRequestRejectsUnimplementedReplay verifies that the reserved replay method still returns the stable unsupported-method error code.
func TestProcessRequestRejectsUnimplementedReplay(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                    // Construct one handler with no orchestrator dependency because the reserved replay method exits before any service call.
	request := &Request{JSONRPC: "2.0", Method: "replay", Params: json.RawMessage(`{"traceId":"trace-1"}`), ID: "req"} // Build one schema-valid replay request so the reserved-method path itself is exercised directly instead of failing earlier in validation.

	if _, err := handler.ProcessRequest(context.Background(), request); err == nil { // Execute the production JSON-RPC dispatch path for the reserved replay method.
		t.Fatal("expected replay to fail as unimplemented") // Surface the missing rejection because clients depend on the reserved method staying non-GA until it is fully implemented.
	} else if !errors.IsCode(err, errors.CodeStepUnsupported) { // Fail the test when the returned error does not preserve the stable unsupported-method code.
		t.Fatalf("expected unsupported-method error, got %v", err) // Surface the unexpected error so dispatch regressions are obvious.
	}
}

// TestValidatorRejectsInvalidStartSessionPayload verifies that schema validation fails before dispatch when required params are missing.
func TestValidatorRejectsInvalidStartSessionPayload(t *testing.T) {
	validator := NewValidator()                                      // Construct one production JSON-RPC schema validator so the embedded schema set is exercised directly.
	err := validator.Validate("startSession", json.RawMessage(`{}`)) // Validate one invalid startSession payload that omits the required projectId and capability fields.
	if err == nil {                                                  // Fail the test when schema validation does not reject the malformed payload.
		t.Fatal("expected schema validation to fail") // Surface the missing validation failure because gateway transports rely on it before orchestration begins.
	}
	if !errors.IsCode(err, errors.CodeSchemaInvalid) { // Fail the test when the returned error does not preserve the stable schema-invalid code.
		t.Fatalf("expected schema invalid error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}

// TestProcessRequestReturnsMethodNotFoundMCPError verifies that unknown methods still map to the MCP-compatible method-not-found error shape.
func TestProcessRequestReturnsMethodNotFoundMCPError(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                         // Construct one handler with no orchestrator dependency because unknown-method dispatch exits before any service call.
	request := &Request{JSONRPC: "2.0", Method: "methodThatDoesNotExist", Params: json.RawMessage(`{}`), ID: "req-unknown"} // Build one direct unknown-method request with an id so the fallback request path is exercised instead of notification semantics.

	if _, err := handler.ProcessRequest(context.Background(), request); err == nil { // Execute the production JSON-RPC fallback path for one unknown method.
		t.Fatal("expected unknown method to fail") // Surface the missing failure because clients depend on stable method-not-found behavior.
	} else if mcpErr, ok := err.(*mcperrors.MCPError); !ok || mcpErr.Code != -32601 { // Fail the test when the fallback path no longer returns the MCP-compatible method-not-found shape.
		t.Fatalf("expected MCP method-not-found error, got %#v", err) // Surface the unexpected error so dispatch regressions are obvious.
	}
}

// TestProcessRequestSilentlyIgnoresUnknownNotification verifies that unsupported notifications are consumed without surfacing a protocol error.
func TestProcessRequestSilentlyIgnoresUnknownNotification(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                      // Construct one handler with no orchestrator dependency because the test exercises only notification classification and fallback behavior.
	request := &Request{JSONRPC: "2.0", Method: "methodThatDoesNotExist", Params: json.RawMessage(`{}`)} // Build one unknown notification without an id so the notification-ignore path is exercised directly.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute the production fallback dispatch path for one unsupported notification.
	if err != nil {                                                      // Fail when unsupported notifications unexpectedly surface protocol errors to the shared handler boundary.
		t.Fatalf("expected unknown notification to be ignored, got error: %v", err) // Surface the unexpected error so notification regressions are obvious.
	}
	if result != nil { // Fail when unsupported notifications unexpectedly produce one result payload.
		t.Fatalf("expected unknown notification to return nil result, got %#v", result) // Surface the unexpected payload so notification regressions are obvious.
	}
}

// TestProcessRequestAcceptsInitializedNotification verifies that the MCP initialized lifecycle notification is accepted silently.
func TestProcessRequestAcceptsInitializedNotification(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                         // Construct one handler without orchestrator dependencies because the initialized notification is pure protocol bookkeeping.
	request := &Request{JSONRPC: "2.0", Method: "notifications/initialized", Params: json.RawMessage(`{}`)} // Build one initialized notification payload with no id so notification semantics are exercised directly.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute the production notification dispatch path directly at the JSON-RPC handler boundary.
	if err != nil {                                                      // Fail when the initialized notification unexpectedly returns a protocol error.
		t.Fatalf("expected initialized notification to succeed, got error: %v", err) // Surface the unexpected error so lifecycle-handshake regressions are obvious.
	}
	if result != nil { // Fail when the notification unexpectedly produces a result payload because notifications must not carry responses.
		t.Fatalf("expected initialized notification to return nil result, got %#v", result) // Surface the unexpected payload so notification regressions are obvious.
	}
	if !IsNotification(request) { // Fail when the shared message classifier no longer recognizes initialized as a notification.
		t.Fatal("expected initialized message to be classified as notification") // Surface the classifier regression because transports depend on it to suppress responses.
	}
}

// TestProcessRequestCancelledNotificationCancelsInflightRequest verifies that notifications/cancelled cancels one matching registered request.
func TestProcessRequestCancelledNotificationCancelsInflightRequest(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                         // Construct one handler without orchestrator dependencies because this test exercises only protocol-level cancellation bookkeeping.
	requestCtx, cancel := context.WithCancel(context.Background())                                                                                          // Create one cancellable context that represents an active request registered by a transport.
	cleanup := handler.RegisterInFlightRequest("req-cancel", cancel)                                                                                        // Register the request id exactly as a transport would before dispatching work.
	defer cleanup()                                                                                                                                         // Ensure the registry is cleaned even if the cancellation assertion fails.
	request := &Request{JSONRPC: "2.0", Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"req-cancel","reason":"client timeout"}`)} // Build one cancellation notification that targets the registered request id.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute the production cancellation notification dispatch path.
	if err != nil {                                                      // Fail when a valid cancellation notification unexpectedly returns a protocol error.
		t.Fatalf("expected cancellation notification to succeed, got error: %v", err) // Surface the unexpected error so cancellation regressions are obvious.
	}
	if result != nil { // Fail when the notification unexpectedly produces a result payload.
		t.Fatalf("expected cancellation notification to return nil result, got %#v", result) // Surface the unexpected payload so notification regressions are obvious.
	}
	select { // Check the request context synchronously because CancelInFlightRequest invokes the cancel function before returning.
	case <-requestCtx.Done(): // Observe the cancelled request context to prove the registry invoked the registered cancel function.
	default:
		t.Fatal("expected cancellation notification to cancel registered request") // Surface the missing cancellation because clients depend on request-id cancellation semantics.
	}
}

// TestProcessRequestCancelledNotificationIgnoresLateRequest verifies that cancellation for an already-completed request is a benign no-op.
func TestProcessRequestCancelledNotificationIgnoresLateRequest(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                    // Construct one handler without orchestrator dependencies because this test exercises only late-cancellation handling.
	request := &Request{JSONRPC: "2.0", Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":"missing-request","reason":"late"}`)} // Build one cancellation notification for a request id that is not registered.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute the production cancellation notification dispatch path for the missing request id.
	if err != nil {                                                      // Fail when late cancellation unexpectedly surfaces a protocol error.
		t.Fatalf("expected late cancellation notification to be ignored, got error: %v", err) // Surface the unexpected error so benign cancellation races remain supported.
	}
	if result != nil { // Fail when late cancellation unexpectedly produces a result payload.
		t.Fatalf("expected late cancellation notification to return nil result, got %#v", result) // Surface the unexpected payload so notification regressions are obvious.
	}
}

// TestProcessRequestReturnsPingResult verifies that the MCP ping method returns the required empty object payload.
func TestProcessRequestReturnsPingResult(t *testing.T) {
	handler := NewHandler(nil, nil)                                                           // Construct one handler without orchestrator dependencies because ping is a pure protocol liveness method.
	request := &Request{JSONRPC: "2.0", Method: "ping", Params: json.RawMessage(`{}`), ID: 9} // Build one ping request with an id so request semantics are exercised directly.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute the production ping dispatch path directly at the JSON-RPC handler boundary.
	if err != nil {                                                      // Fail when ping unexpectedly returns a protocol error.
		t.Fatalf("expected ping to succeed, got error: %v", err) // Surface the unexpected error so liveness regressions are obvious.
	}
	payload, ok := result.(map[string]interface{}) // Decode the generic ping result so the empty-object response shape can be asserted directly.
	if !ok {                                       // Fail when ping no longer returns a structured object payload.
		t.Fatalf("expected ping response map, got %#v", result) // Surface the unexpected payload type so liveness regressions are obvious.
	}
	if len(payload) != 0 { // Fail when ping no longer returns the required empty object payload.
		t.Fatalf("expected empty ping response, got %#v", payload) // Surface the unexpected payload content so liveness regressions are obvious.
	}
	if !IsRequest(request) { // Fail when the shared message classifier no longer recognizes ping with an id as a request.
		t.Fatal("expected ping message to be classified as request") // Surface the classifier regression because transports depend on it to emit responses.
	}
}

// TestProcessRequestRejectsADBShellWhenDisabled verifies that the JSON-RPC MCP bridge hides and rejects adbShell when the gateway kill switch is active.
func TestProcessRequestRejectsADBShellWhenDisabled(t *testing.T) {
	handler := NewHandler(nil, capabilities.NewService(config.GatewayConfig{DisableADBShellTool: true}))                                                                                                       // Construct one handler with adbShell disabled so the JSON-RPC-to-MCP bridge can be asserted directly.
	request := &Request{JSONRPC: "2.0", Method: "tools/call", Params: json.RawMessage(`{"name":"adbShell","arguments":{"deviceSerial":"emulator-5554","command":["getprop","ro.build.version.sdk"]}}`), ID: 7} // Build one schema-valid adbShell tools/call request so disablement behavior can be asserted directly.

	if _, err := handler.ProcessRequest(context.Background(), request); err == nil { // Execute the production JSON-RPC dispatch path for adbShell while the gateway kill switch is active.
		t.Fatal("expected disabled adbShell tool to fail") // Surface the missing rejection because operators rely on the kill switch to fully close the public tool surface.
	} else if mcpErr, ok := err.(*mcperrors.MCPError); !ok || mcpErr.Code != -32601 || mcpErr.Message != "Tool not found: adbShell" { // Fail when the disabled tool no longer maps to the standard tool-not-found MCP error.
		t.Fatalf("expected adbShell tool-not-found MCP error, got %#v", err) // Surface the unexpected error so gateway disablement regressions are easy to diagnose.
	}
}

// TestServeHTTPRejectsParseError verifies that the HTTP transport returns the standard JSON-RPC parse-error envelope for malformed request bodies.
func TestServeHTTPRejectsParseError(t *testing.T) {
	handler := NewHandler(nil, nil)                                                     // Construct one handler with no orchestrator dependency because malformed JSON is rejected before dispatch.
	request := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader("{")) // Build one malformed JSON-RPC HTTP request body so the parse-error path is exercised directly.
	recorder := httptest.NewRecorder()                                                  // Capture the HTTP response so the emitted JSON-RPC error envelope can be asserted directly.

	handler.ServeHTTP(recorder, request) // Execute the malformed HTTP request through the production JSON-RPC HTTP transport.

	if recorder.Code != http.StatusOK { // Fail the test when the transport does not return the normal JSON-RPC HTTP status for parse errors.
		t.Fatalf("expected HTTP 200 for parse error envelope, got %d", recorder.Code) // Surface the unexpected transport status so HTTP mapping regressions are obvious.
	}
	if !strings.Contains(recorder.Body.String(), `"code":-32700`) { // Fail the test when the transport does not emit the standard JSON-RPC parse-error code.
		t.Fatalf("expected parse error body, got %s", recorder.Body.String()) // Surface the unexpected body so HTTP mapping regressions are obvious.
	}
}

// TestServeHTTPRejectsInvalidRequestVersion verifies that the HTTP transport returns the standard JSON-RPC invalid-request envelope for non-2.0 requests.
func TestServeHTTPRejectsInvalidRequestVersion(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                            // Construct one handler with no orchestrator dependency because protocol-version validation runs before business dispatch.
	request := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader(`{"jsonrpc":"1.0","method":"describeCapabilities","id":1}`)) // Build one syntactically valid but protocol-invalid JSON-RPC request body.
	recorder := httptest.NewRecorder()                                                                                                         // Capture the HTTP response so the emitted JSON-RPC error envelope can be asserted directly.

	handler.ServeHTTP(recorder, request) // Execute the invalid-version HTTP request through the production JSON-RPC HTTP transport.

	if recorder.Code != http.StatusOK { // Fail the test when the transport does not return the normal JSON-RPC HTTP status for invalid requests.
		t.Fatalf("expected HTTP 200 for invalid-request envelope, got %d", recorder.Code) // Surface the unexpected transport status so HTTP mapping regressions are obvious.
	}
	if !strings.Contains(recorder.Body.String(), `"code":-32600`) { // Fail the test when the transport does not emit the standard JSON-RPC invalid-request code.
		t.Fatalf("expected invalid request body, got %s", recorder.Body.String()) // Surface the unexpected body so HTTP mapping regressions are obvious.
	}
}

// TestServeHTTPSuppressesInitializedNotificationResponse verifies that the compatibility HTTP transport consumes notifications without emitting JSON-RPC bodies.
func TestServeHTTPSuppressesInitializedNotificationResponse(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                      // Construct one handler without orchestrator dependencies because initialized is pure protocol lifecycle traffic.
	request := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`)) // Build one HTTP notification payload without an id so the transport suppression path is exercised directly.
	recorder := httptest.NewRecorder()                                                                                                                   // Capture the HTTP exchange so the transport status and body can be asserted directly.

	handler.ServeHTTP(recorder, request) // Execute the initialized notification through the production HTTP compatibility transport.

	if recorder.Code != http.StatusNoContent { // Fail when the compatibility transport no longer suppresses notification response bodies.
		t.Fatalf("expected HTTP 204 for notification, got %d", recorder.Code) // Surface the unexpected transport status so notification regressions are obvious.
	}
	if recorder.Body.Len() != 0 { // Fail when the compatibility transport still emits a JSON-RPC body for notifications.
		t.Fatalf("expected empty notification body, got %q", recorder.Body.String()) // Surface the unexpected body so notification regressions are obvious.
	}
}
