// handler_test.go verifies the MCP Streamable HTTP transport semantics implemented at the HTTP package boundary.
package mcphttp

import (
	"encoding/base64"
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
	request.Header.Set("Content-Type", contentTypeJSON)                              // Declare the JSON request representation required by Streamable HTTP POST.
	return request                                                                   // Return the prepared request so tests can add protocol-version headers when needed.
}

// newModernPOST builds one current-protocol request with mirrored standard headers and an optional method target name.
func newModernPOST(body string, method string, targetName string, protocolVersion string) *http.Request {
	request := newStreamablePOST(body)                         // Start from the common JSON and response-content negotiation headers.
	request.Header.Set(protocolVersionHeader, protocolVersion) // Mirror the request-local modern protocol revision at the HTTP layer.
	request.Header.Set(methodHeader, method)                   // Mirror the exact case-sensitive JSON-RPC method for server validation.
	if targetName != "" {                                      // Add Mcp-Name only for methods whose params provide a target identity.
		request.Header.Set(nameHeader, targetName) // Mirror the tool, resource, or prompt identity for header/body comparison.
	}
	return request // Return the fully prepared modern request for transport-level assertions.
}

// TestPostRejectsOversizedBody verifies that direct Streamable HTTP mounts enforce the shared request-size ceiling with HTTP 413.
func TestPostRejectsOversizedBody(t *testing.T) {
	handler := newTestHandler()                                  // Construct the production transport without application dependencies.
	request := newStreamablePOST(strings.Repeat("x", (1<<20)+1)) // Build a body one byte larger than the shared one-mebibyte ceiling.
	recorder := httptest.NewRecorder()                           // Capture the transport status and JSON-RPC error body.
	handler.ServeHTTP(recorder, request)                         // Execute the oversized request through the direct handler mount.
	if recorder.Code != http.StatusRequestEntityTooLarge {       // Require the standard transport status instead of a generic parse error.
		t.Fatalf("expected status %d, got %d", http.StatusRequestEntityTooLarge, recorder.Code) // Surface the incorrect status and preserve the size-contract regression signal.
	}
}

// TestPostRejectsTrailingJSONDocument verifies that one Streamable HTTP POST cannot contain multiple concatenated JSON-RPC messages.
func TestPostRejectsTrailingJSONDocument(t *testing.T) {
	handler := newTestHandler()                                                                                      // Construct the production transport without application dependencies.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"ping","id":1}{"jsonrpc":"2.0","method":"ping","id":2}`) // Build two valid envelopes concatenated into one HTTP body.
	recorder := httptest.NewRecorder()                                                                               // Capture the parse-error transport response.
	handler.ServeHTTP(recorder, request)                                                                             // Execute the concatenated body through strict single-document decoding.
	if recorder.Code != http.StatusBadRequest {                                                                      // Require transport rejection before either request can dispatch.
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code) // Surface accidental acceptance or misclassification.
	}
}

// TestAcceptsContentTypeHonorsQuality verifies that explicitly unacceptable media types do not satisfy Streamable HTTP response negotiation.
func TestAcceptsContentTypeHonorsQuality(t *testing.T) {
	if acceptsContentType("application/json;q=0, text/event-stream", contentTypeJSON) { // Check that an explicit zero JSON quality does not count as supported.
		t.Fatal("expected application/json with q=0 to be rejected") // Surface accidental response negotiation that could return a forbidden representation.
	}
	if !acceptsContentType("application/json;q=0.5, text/event-stream", contentTypeJSON) { // Check that a positive valid quality still advertises JSON support.
		t.Fatal("expected application/json with positive quality to be accepted") // Surface overly strict parsing that would reject a compliant client.
	}
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
	if recorder.Header().Get(protocolVersionHeader) != mcp.LegacyProtocolVersion { // Fail when the response no longer echoes the fallback legacy protocol version.
		t.Fatalf("expected protocol header %s, got %q", mcp.LegacyProtocolVersion, recorder.Header().Get(protocolVersionHeader)) // Surface the actual header for negotiation debugging.
	}
	if !strings.Contains(recorder.Body.String(), `"protocolVersion":"2025-06-18"`) { // Fail when an unknown legacy initialize request does not receive the historical fallback version.
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
	request.Header.Set(protocolVersionHeader, mcp.LegacyProtocolVersion)       // Attach the negotiated legacy MCP protocol version required after initialize.
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

// TestPostPingEchoesLargeNumericIDExactly verifies that Streamable HTTP preserves arbitrary-size JSON-RPC integer identifiers through its wire envelope.
func TestPostPingEchoesLargeNumericIDExactly(t *testing.T) {
	handler := newTestHandler()                                                             // Construct one protocol-only Streamable HTTP handler for the legacy ping path.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"ping","id":9007199254740993}`) // Use an integer above IEEE-754's exact range to detect lossy generic decoding.
	request.Header.Set(protocolVersionHeader, mcp.LegacyProtocolVersion)                    // Attach the negotiated legacy revision required by the ping transport contract.
	recorder := httptest.NewRecorder()                                                      // Capture the exact JSON response body.
	handler.ServeHTTP(recorder, request)                                                    // Execute through the Streamable HTTP wire-message custom unmarshaler and shared JSON-RPC handler.
	if recorder.Code != http.StatusOK {                                                     // Require normal request-response success for the valid large identifier.
		t.Fatalf("expected HTTP 200, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface transport output on precision regression.
	}
	if !strings.Contains(recorder.Body.String(), `"id":9007199254740993`) { // Require the original integer text rather than a float64-rounded value.
		t.Fatalf("expected exact large numeric id in response, got %s", recorder.Body.String()) // Surface the full response for diagnosis.
	}
}

// TestPostNotificationReturnsAcceptedNoBody verifies that accepted notifications return HTTP 202 with no JSON-RPC body.
func TestPostNotificationReturnsAcceptedNoBody(t *testing.T) {
	handler := newTestHandler()                                                                        // Construct one protocol-only Streamable HTTP handler.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`) // Build one initialized notification with no id.
	request.Header.Set(protocolVersionHeader, mcp.LegacyProtocolVersion)                               // Attach the negotiated legacy MCP protocol version required after initialize.
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
	request.Header.Set(protocolVersionHeader, mcp.LegacyProtocolVersion)          // Attach the negotiated legacy MCP protocol version required after initialize.
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
	handler := newTestHandler()                                          // Construct one protocol-only Streamable HTTP handler.
	request := httptest.NewRequest(http.MethodGet, "/mcp", nil)          // Build one GET request targeting the MCP endpoint.
	request.Header.Set("Accept", contentTypeSSE)                         // Advertise SSE support so the handler reaches the no-stream decision.
	request.Header.Set(protocolVersionHeader, mcp.LegacyProtocolVersion) // Attach one legacy version to prove GET rejection no longer depends on negotiation state.
	recorder := httptest.NewRecorder()                                   // Capture the response so status can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the GET request through the production Streamable HTTP handler.

	if recorder.Code != http.StatusMethodNotAllowed { // Fail when the no-SSE GET path no longer returns the spec-allowed 405 status.
		t.Fatalf("expected HTTP 405, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the response body so GET behavior regressions are easy to diagnose.
	}
}

// TestPostRejectsMismatchedOrigin verifies that browser-originated requests must be same-origin when no explicit allowlist exists.
func TestPostRejectsMismatchedOrigin(t *testing.T) {
	handler := newTestHandler()                                              // Construct one protocol-only Streamable HTTP handler with same-origin fallback.
	request := newStreamablePOST(`{"jsonrpc":"2.0","method":"ping","id":1}`) // Build one otherwise-valid request so origin rejection is isolated.
	request.Header.Set(protocolVersionHeader, mcp.LegacyProtocolVersion)     // Attach the negotiated legacy MCP protocol version required after initialize.
	request.Header.Set("Origin", "https://evil.example")                     // Simulate a browser request from a different host than the MCP endpoint.
	recorder := httptest.NewRecorder()                                       // Capture the response so status can be asserted.

	handler.ServeHTTP(recorder, request) // Execute the request through the production Streamable HTTP handler.

	if recorder.Code != http.StatusForbidden { // Fail when mismatched browser origins are no longer rejected.
		t.Fatalf("expected HTTP 403, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the body so origin-policy regressions are easy to diagnose.
	}
}

// TestModernDiscoverReturnsCurrentProtocolMetadata verifies mandatory stateless discovery and modern result decoration over Streamable HTTP.
func TestModernDiscoverReturnsCurrentProtocolMetadata(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                                                                                                               // Construct one protocol-only handler because server/discover does not require an orchestrator.
	body := `{"jsonrpc":"2.0","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{},"io.modelcontextprotocol/clientInfo":{"name":"test","version":"1.0.0"}}},"id":"discover"}` // Build a complete stateless discovery request using the current protocol metadata fields.
	request := newModernPOST(body, "server/discover", "", mcp.CurrentProtocolVersion)                                                                                                                                                                                         // Mirror the modern version and discovery method in required HTTP headers.
	recorder := httptest.NewRecorder()                                                                                                                                                                                                                                        // Capture the discovery response status, version header, and JSON result.

	handler.ServeHTTP(recorder, request) // Execute discovery through the production modern HTTP path.

	if recorder.Code != http.StatusOK { // Fail when mandatory discovery does not produce one successful JSON-RPC response.
		t.Fatalf("expected HTTP 200, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the response body so discovery failures are actionable.
	}
	if recorder.Header().Get(protocolVersionHeader) != mcp.CurrentProtocolVersion { // Require the response to identify the implemented modern protocol revision.
		t.Fatalf("expected current protocol header, got %q", recorder.Header().Get(protocolVersionHeader)) // Surface the actual version header for negotiation debugging.
	}
	for _, expected := range []string{`"resultType":"complete"`, `"supportedVersions":["2026-07-28"`, `"io.modelcontextprotocol/serverInfo"`, `"ttlMs":3600000`, `"cacheScope":"public"`} { // Assert each mandatory or advertised discovery field independently of JSON map ordering.
		if !strings.Contains(recorder.Body.String(), expected) { // Fail when any modern discovery or base-result field is absent.
			t.Fatalf("expected discovery body to contain %s, got %s", expected, recorder.Body.String()) // Name the missing fragment and preserve the full response for diagnosis.
		}
	}
}

// TestModernRequestRequiresPerRequestMetadata verifies that a current-version HTTP header cannot substitute for stateless body metadata.
func TestModernRequestRequiresPerRequestMetadata(t *testing.T) {
	handler := newTestHandler()                                                                                                          // Construct one protocol-only handler for metadata validation.
	request := newModernPOST(`{"jsonrpc":"2.0","method":"tools/list","params":{},"id":1}`, "tools/list", "", mcp.CurrentProtocolVersion) // Build a header-complete request that deliberately omits params._meta.
	recorder := httptest.NewRecorder()                                                                                                   // Capture the required modern malformed-request response.

	handler.ServeHTTP(recorder, request) // Execute the request through modern header and metadata validation.

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":-32602`) { // Require HTTP 400 and the protocol-mandated invalid-params code.
		t.Fatalf("expected missing metadata HTTP 400/-32602, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface both transport and JSON-RPC outputs on regression.
	}
}

// TestModernRequestRejectsMethodHeaderMismatch verifies the protocol-reserved HeaderMismatch response before dispatch.
func TestModernRequestRejectsMethodHeaderMismatch(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                          // Construct one protocol-only handler because rejection occurs before method execution.
	body := `{"jsonrpc":"2.0","method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":2}` // Build one otherwise-valid tools/list request.
	request := newModernPOST(body, "tools/call", "", mcp.CurrentProtocolVersion)                                                                                                         // Deliberately mirror a different method in the required HTTP header.
	recorder := httptest.NewRecorder()                                                                                                                                                   // Capture the header-validation error envelope.

	handler.ServeHTTP(recorder, request) // Execute the mismatch through the production validation path.

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":-32020`) { // Require the exact modern HeaderMismatch status and code.
		t.Fatalf("expected method mismatch HTTP 400/-32020, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected response for transport debugging.
	}
}

// TestModernRequestReturnsSupportedVersionsForUnknownRevision verifies retryable protocol negotiation without an initialize handshake.
func TestModernRequestReturnsSupportedVersionsForUnknownRevision(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                               // Construct one protocol-only handler because version rejection occurs before discovery execution.
	body := `{"jsonrpc":"2.0","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"1900-01-01","io.modelcontextprotocol/clientCapabilities":{}}},"id":3}` // Build a structurally valid discovery request for an unavailable revision.
	request := newModernPOST(body, "server/discover", "", "1900-01-01")                                                                                                                       // Keep HTTP and body versions consistent so the version error is not masked by HeaderMismatch.
	recorder := httptest.NewRecorder()                                                                                                                                                        // Capture the retry metadata returned with the modern error.

	handler.ServeHTTP(recorder, request) // Execute unsupported-version selection through the production transport.

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":-32022`) { // Require the protocol-reserved unsupported-version error.
		t.Fatalf("expected unsupported version HTTP 400/-32022, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface transport and JSON-RPC outputs on regression.
	}
	if !strings.Contains(recorder.Body.String(), `"supported":["2026-07-28","2025-11-25","2025-06-18"]`) || !strings.Contains(recorder.Body.String(), `"requested":"1900-01-01"`) { // Require enough data for deterministic client retry or legacy fallback.
		t.Fatalf("expected supported/requested version data, got %s", recorder.Body.String()) // Surface the incomplete negotiation payload.
	}
}

// TestModernRemovedPingReturnsHTTPNotFound verifies that a legacy lifecycle method is not exposed under current stateless semantics.
func TestModernRemovedPingReturnsHTTPNotFound(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                    // Construct one protocol-only handler because ping never reaches an orchestrator.
	body := `{"jsonrpc":"2.0","method":"ping","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":4}` // Build a current-protocol request for the removed ping method.
	request := newModernPOST(body, "ping", "", mcp.CurrentProtocolVersion)                                                                                                         // Supply every modern header so only method availability is under test.
	recorder := httptest.NewRecorder()                                                                                                                                             // Capture the modern unknown-method status and envelope.

	handler.ServeHTTP(recorder, request) // Execute the removed method through modern dispatch gating.

	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), `"code":-32601`) { // Require the current transport's explicit 404 plus JSON-RPC method-not-found code.
		t.Fatalf("expected modern ping HTTP 404/-32601, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected response for era-selection debugging.
	}
}

// TestModernRequestRejectsNameHeaderMismatch verifies target identity consistency for tools/call before any tool execution.
func TestModernRequestRejectsNameHeaderMismatch(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                                                              // Construct one protocol-only handler because the mismatch prevents healthCheck execution.
	body := `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"healthCheck","arguments":{},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":5}` // Build one valid target-bearing modern request.
	request := newModernPOST(body, "tools/call", "startSession", mcp.CurrentProtocolVersion)                                                                                                                                 // Deliberately mirror a different tool identity in Mcp-Name.
	recorder := httptest.NewRecorder()                                                                                                                                                                                       // Capture the pre-dispatch HeaderMismatch response.

	handler.ServeHTTP(recorder, request) // Execute the request through standard target-header validation.

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":-32020`) { // Require an exact HeaderMismatch result for target disagreement.
		t.Fatalf("expected name mismatch HTTP 400/-32020, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected result for routing-safety debugging.
	}
}

// TestModernNameHeaderAcceptsBase64Sentinel verifies that an explicitly encoded request target is decoded safely before normal method dispatch.
func TestModernNameHeaderAcceptsBase64Sentinel(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                                                 // Construct one protocol-only handler because prompts/get is intentionally unimplemented after header validation.
	body := `{"jsonrpc":"2.0","method":"prompts/get","params":{"name":"weather-test","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":6}` // Build a modern target-bearing request that will reach method gating.
	encodedName := base64ValuePrefix + base64.StdEncoding.EncodeToString([]byte("weather-test")) + base64ValueSuffix                                                                                            // Encode even a safe ASCII sentinel payload to verify exact Base64 decoding support.
	request := newModernPOST(body, "prompts/get", encodedName, mcp.CurrentProtocolVersion)                                                                                                                      // Supply the sentinel-encoded target under Mcp-Name.
	recorder := httptest.NewRecorder()                                                                                                                                                                          // Capture the post-validation method-not-found response.

	handler.ServeHTTP(recorder, request) // Execute the encoded header through decoding, comparison, and modern dispatch.

	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), `"code":-32601`) { // Prove header validation succeeded by observing the later unimplemented-method response.
		t.Fatalf("expected decoded name to reach HTTP 404/-32601, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface a HeaderMismatch if decoding regresses.
	}
}

// TestDecodeMCPHeaderValueAcceptsPartialSentinelText verifies that safe ASCII matching only one sentinel marker remains an ordinary plain header value.
func TestDecodeMCPHeaderValueAcceptsPartialSentinelText(t *testing.T) {
	plainValue := "=?base64?literal-name"                 // Build one safe ASCII name with the sentinel prefix but no closing marker.
	decodedValue, err := decodeMCPHeaderValue(plainValue) // Decode the value through the production header safety helper.
	if err != nil {                                       // Fail when one unambiguous partial marker is incorrectly treated as malformed Base64.
		t.Fatalf("expected partial sentinel text to remain plain, got %v", err) // Surface the unexpected header-validation failure.
	}
	if decodedValue != plainValue { // Require exact byte preservation for plain ASCII request target comparison.
		t.Fatalf("expected decoded value %q, got %q", plainValue, decodedValue) // Surface unwanted mutation of the valid target name.
	}
}

// TestModernToolsListReturnsCacheAndSchemaMetadata verifies the current tool catalog's result, cache, and JSON Schema dialect fields end to end.
func TestModernToolsListReturnsCacheAndSchemaMetadata(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                          // Construct one protocol-only handler because tools/list reads only the embedded registry.
	body := `{"jsonrpc":"2.0","method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":7}` // Build one complete modern tool discovery request.
	request := newModernPOST(body, "tools/list", "", mcp.CurrentProtocolVersion)                                                                                                         // Supply the two standard headers required for a non-targeted modern method.
	recorder := httptest.NewRecorder()                                                                                                                                                   // Capture the fully decorated tool catalog response.

	handler.ServeHTTP(recorder, request) // Execute tool discovery through modern HTTP and shared JSON-RPC layers.

	if recorder.Code != http.StatusOK { // Require successful current-protocol tool discovery.
		t.Fatalf("expected modern tools/list HTTP 200, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the full response on failure.
	}
	for _, expected := range []string{`"resultType":"complete"`, `"ttlMs":300000`, `"cacheScope":"public"`, `"$schema":"https://json-schema.org/draft/2020-12/schema"`} { // Assert all alignment fields independently of catalog length and map ordering.
		if !strings.Contains(recorder.Body.String(), expected) { // Fail when result decoration, caching, or dialect advertisement regresses.
			t.Fatalf("expected tools/list body to contain %s, got %s", expected, recorder.Body.String()) // Name the missing current-protocol field.
		}
	}
}

// TestModernClientResponseIsRejected verifies that the current Streamable HTTP binding no longer accepts client JSON-RPC response envelopes.
func TestModernClientResponseIsRejected(t *testing.T) {
	handler := newTestHandler()                                                                                         // Construct one protocol-only handler because the transport rejects the envelope before dispatch.
	request := newModernPOST(`{"jsonrpc":"2.0","result":{"ok":true},"id":8}`, "unused", "", mcp.CurrentProtocolVersion) // Build a methodless modern response envelope with a current-version header.
	recorder := httptest.NewRecorder()                                                                                  // Capture the modern invalid-request response.

	handler.ServeHTTP(recorder, request) // Execute the forbidden envelope through transport classification.

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":-32600`) { // Require explicit rejection instead of the legacy empty 202 behavior.
		t.Fatalf("expected modern client response HTTP 400/-32600, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected response for era behavior debugging.
	}
}

// TestModernClientNotificationIsRejected verifies that current Streamable HTTP uses request-stream closure rather than client cancellation notifications.
func TestModernClientNotificationIsRejected(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                                                             // Construct one protocol-only handler because the transport rejects the notification before cancellation dispatch.
	body := `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"active-request","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}` // Build a complete current-protocol cancellation notification without a JSON-RPC id.
	request := newStreamablePOST(body)                                                                                                                                                                                      // Omit modern request headers because current Streamable HTTP defines no header contract for unsupported client notifications.
	recorder := httptest.NewRecorder()                                                                                                                                                                                      // Capture the transport-level invalid-request response.

	handler.ServeHTTP(recorder, request) // Execute the unsupported notification through modern Streamable HTTP classification.

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":-32600`) { // Require explicit rejection instead of dispatching stdio-only cancellation behavior.
		t.Fatalf("expected modern notification HTTP 400/-32600, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected status or JSON-RPC code.
	}
}

// TestModernRequestRejectsExplicitNullID verifies that an id member set to JSON null is not treated as a fire-and-forget notification.
func TestModernRequestRejectsExplicitNullID(t *testing.T) {
	handler := newTestHandler()                                                                                                                                                             // Construct one protocol-only handler because request classification fails before tools/list dispatch.
	body := `{"jsonrpc":"2.0","method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}},"id":null}` // Build a header-complete modern request whose explicit null id violates MCP request rules.
	request := newModernPOST(body, "tools/list", "", mcp.CurrentProtocolVersion)                                                                                                            // Supply valid mirrored headers so only id classification is under test.
	recorder := httptest.NewRecorder()                                                                                                                                                      // Capture the invalid-request status and JSON-RPC error body.

	handler.ServeHTTP(recorder, request) // Execute the malformed request through production wire decoding and classification.

	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":-32600`) { // Require a JSON-RPC Invalid Request response rather than notification-style HTTP 202.
		t.Fatalf("expected explicit null id HTTP 400/-32600, got %d with body %s", recorder.Code, recorder.Body.String()) // Surface the unexpected transport behavior.
	}
}
