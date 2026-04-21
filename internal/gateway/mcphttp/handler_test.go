// handler_test.go verifies the MCP Streamable HTTP transport semantics implemented at the HTTP package boundary.
package mcphttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mcp_for_appium/internal/gateway/jsonrpc"
	"mcp_for_appium/internal/gateway/mcp"
)

// newTestHandler constructs one Streamable HTTP handler with no orchestrator dependency for protocol-only tests.
func newTestHandler() *Handler {
	return NewHandler(jsonrpc.NewHandler(nil, nil), "") // Reuse the production JSON-RPC handler because these tests exercise transport behavior over pure protocol methods.
}

// newStreamablePOST builds one POST request with the Accept header required by MCP Streamable HTTP.
func newStreamablePOST(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body)) // Build an in-memory POST request targeting the standard MCP endpoint path.
	request.Header.Set("Accept", "application/json, text/event-stream")              // Advertise both response content types required by Streamable HTTP POST.
	return request                                                                   // Return the prepared request so tests can add protocol-version headers when needed.
}

// TestPostInitializeReturnsJSONAndProtocolVersion verifies that initialize can negotiate before the version header exists.
func TestPostInitializeReturnsJSONAndProtocolVersion(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                               // Construct one protocol-only Streamable HTTP handler.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}},"id":1}`) // Build an initialize request without MCP-Protocol-Version because negotiation has not completed yet.
	recorder := httptest.NewRecorder()                                                                                                                                                        // Capture the response so status, headers, and body can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the initialize request through the production Streamable HTTP handler.

	if recorder.Code != http.StatusOK { // Fail when request-style initialize no longer returns a JSON response.
		t.Fatalf("expected HTTP 200, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the response body so transport regressions are easy to diagnose.
	}
	if recorder.Header().Get(protocolVersionHeader) != mcp.SupportedProtocolVersion { // Fail when the response no longer echoes the accepted protocol version.
		t.Fatalf("expected protocol header %s, got %q", mcp.SupportedProtocolVersion, recorder.Header().Get(protocolVersionHeader)) // Surface the actual header for negotiation debugging.
	}
	if !strings.Contains(recorder.Body.String(), `"protocolVersion":"2025-06-18"`) { // Fail when initialize does not advertise the target MCP protocol version.
		t.Fatalf("expected initialize body to contain target protocol version, got %s", recorder.Body.String()) // Surface the actual body so protocol-version regressions are obvious.
	}
}

// TestPostPingRequiresProtocolVersionHeader verifies that post-initialize requests must include MCP-Protocol-Version.
func TestPostPingRequiresProtocolVersionHeader(t *testing.T) {
	handler := newTestHandler()                                              // Construct one protocol-only Streamable HTTP handler.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"ping","id":2}`) // Build one post-initialize request without the required protocol-version header.
	recorder := httptest.NewRecorder()                                       // Capture the response so status and body can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the invalid request through the production Streamable HTTP handler.

	if recorder.Code != http.StatusBadRequest { // Fail when the transport accepts a post-initialize request without protocol version.
		t.Fatalf("expected HTTP 400, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the response body so header-validation regressions are easy to diagnose.
	}
	if !strings.Contains(recorder.Body.String(), "unsupported MCP protocol version") { // Fail when the diagnostic no longer explains the protocol-version rejection.
		t.Fatalf("expected protocol-version diagnostic, got %s", recorder.Body.String()) // Surface the body so validation regressions are easy to diagnose.
	}
}

// TestPostPingReturnsJSONWhenProtocolVersionMatches verifies that request-style POSTs return one JSON-RPC response body.
func TestPostPingReturnsJSONWhenProtocolVersionMatches(t *testing.T) {
	handler := newTestHandler()                                                // Construct one protocol-only Streamable HTTP handler.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"ping","id":"p"}`) // Build one valid ping request so request response semantics can be asserted.
	request.Header.Set(protocolVersionHeader, mcp.SupportedProtocolVersion)    // Attach the negotiated MCP protocol version required after initialize.
	recorder := httptest.NewRecorder()                                         // Capture the response so status, headers, and body can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the valid ping request through the production Streamable HTTP handler.

	if recorder.Code != http.StatusOK { // Fail when request-style POST no longer returns HTTP 200 with a JSON-RPC envelope.
		t.Fatalf("expected HTTP 200, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the response body so transport regressions are easy to diagnose.
	}
	if recorder.Header().Get("Content-Type") != contentTypeJSON { // Fail when the request response is no longer served as application/json.
		t.Fatalf("expected content type %s, got %q", contentTypeJSON, recorder.Header().Get("Content-Type")) // Surface the actual header so content negotiation regressions are obvious.
	}
	if !strings.Contains(recorder.Body.String(), `"id":"p"`) || !strings.Contains(recorder.Body.String(), `"result":{}`) { // Fail when ping no longer returns the expected JSON-RPC result envelope.
		t.Fatalf("expected ping JSON-RPC response, got %s", recorder.Body.String()) // Surface the actual body so request-response regressions are obvious.
	}
}

// TestPostNotificationReturnsAcceptedNoBody verifies that accepted notifications return HTTP 202 with no JSON-RPC body.
func TestPostNotificationReturnsAcceptedNoBody(t *testing.T) {
	handler := newTestHandler()                                                                        // Construct one protocol-only Streamable HTTP handler.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`) // Build one initialized notification with no id.
	request.Header.Set(protocolVersionHeader, mcp.SupportedProtocolVersion)                            // Attach the negotiated MCP protocol version required after initialize.
	recorder := httptest.NewRecorder()                                                                 // Capture the response so status and body can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the notification through the production Streamable HTTP handler.

	if recorder.Code != http.StatusAccepted { // Fail when accepted notifications no longer return the Streamable HTTP 202 status.
		t.Fatalf("expected HTTP 202, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the response body so notification regressions are easy to diagnose.
	}
	if recorder.Body.Len() != 0 { // Fail when notifications emit a JSON-RPC response body.
		t.Fatalf("expected empty notification body, got %q", recorder.Body.String()) // Surface the unexpected body so notification semantics are obvious.
	}
}

// TestPostResponseReturnsAcceptedNoBody verifies that client JSON-RPC responses are accepted without dispatch.
func TestPostResponseReturnsAcceptedNoBody(t *testing.T) {
	handler := newTestHandler()                                                   // Construct one protocol-only Streamable HTTP handler.
	request := newStreamablePOST(`{"jsonrpc":"2.0","result":{"ok":true},"id":3}`) // Build one JSON-RPC response envelope from a client.
	request.Header.Set(protocolVersionHeader, mcp.SupportedProtocolVersion)       // Attach the negotiated MCP protocol version required after initialize.
	recorder := httptest.NewRecorder()                                            // Capture the response so status and body can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the response envelope through the production Streamable HTTP handler.

	if recorder.Code != http.StatusAccepted { // Fail when accepted JSON-RPC responses no longer return Streamable HTTP 202.
		t.Fatalf("expected HTTP 202, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the response body so response-acceptance regressions are easy to diagnose.
	}
	if recorder.Body.Len() != 0 { // Fail when client responses emit any server response body.
		t.Fatalf("expected empty response body, got %q", recorder.Body.String()) // Surface the unexpected body so response semantics are obvious.
	}
}

// TestGetReturnsMethodNotAllowedWhenSSEUnavailable verifies the allowed no-SSE GET behavior for the MCP endpoint.
func TestGetReturnsMethodNotAllowedWhenSSEUnavailable(t *testing.T) {
	handler := newTestHandler()                                             // Construct one protocol-only Streamable HTTP handler.
	request := httptest.NewRequest(http.MethodGet, "/mcp", nil)             // Build one GET request targeting the MCP endpoint.
	request.Header.Set("Accept", contentTypeSSE)                            // Advertise SSE support so the handler reaches the no-stream decision.
	request.Header.Set(protocolVersionHeader, mcp.SupportedProtocolVersion) // Attach the negotiated MCP protocol version required for GET.
	recorder := httptest.NewRecorder()                                      // Capture the response so status can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the GET request through the production Streamable HTTP handler.

	if recorder.Code != http.StatusMethodNotAllowed { // Fail when the no-SSE GET path no longer returns the spec-allowed 405 status.
		t.Fatalf("expected HTTP 405, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the response body so GET behavior regressions are easy to diagnose.
	}
}

// TestPostRejectsMismatchedOrigin verifies that browser-originated requests must be same-origin when no explicit allowlist exists.
func TestPostRejectsMismatchedOrigin(t *testing.T) {
	handler := newTestHandler()                                              // Construct one protocol-only Streamable HTTP handler with same-origin fallback.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"ping","id":1}`) // Build one otherwise-valid request so origin rejection is isolated.
	request.Header.Set(protocolVersionHeader, mcp.SupportedProtocolVersion)  // Attach the negotiated MCP protocol version required after initialize.
	request.Header.Set("Origin", "https://evil.example")                     // Simulate a browser request from a different host than the MCP endpoint.
	recorder := httptest.NewRecorder()                                       // Capture the response so status can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the request through the production Streamable HTTP handler.

	if recorder.Code != http.StatusForbidden { // Fail when mismatched browser origins are no longer rejected.
		t.Fatalf("expected HTTP 403, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the body so origin-policy regressions are easy to diagnose.
	}
}
