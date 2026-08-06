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

// TestServeHTTPRejectsOversizedBody verifies that the compatibility transport applies the shared request-body ceiling when mounted directly.
func TestServeHTTPRejectsOversizedBody(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                // Construct the protocol handler without application dependencies.
	request := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader(strings.Repeat("x", (1<<20)+1))) // Build a body one byte larger than the shared ceiling.
	recorder := httptest.NewRecorder()                                                                             // Capture the explicit HTTP size rejection.
	handler.ServeHTTP(recorder, request)                                                                           // Execute the oversized request through the direct compatibility handler.
	if recorder.Code != http.StatusRequestEntityTooLarge {                                                         // Require HTTP 413 instead of an ordinary JSON-RPC HTTP 200 parse envelope.
		t.Fatalf("expected status %d, got %d", http.StatusRequestEntityTooLarge, recorder.Code) // Surface incorrect transport mapping for the size limit.
	}
}

// TestServeHTTPRejectsTrailingJSONDocument verifies that the compatibility transport accepts exactly one request document per body.
func TestServeHTTPRejectsTrailingJSONDocument(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                                    // Construct the protocol handler without application dependencies.
	request := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader(`{"jsonrpc":"2.0","method":"ping","id":1}{"jsonrpc":"2.0","method":"ping","id":2}`)) // Build two concatenated valid JSON-RPC requests.
	recorder := httptest.NewRecorder()                                                                                                                                 // Capture the compatibility parse-error response.
	handler.ServeHTTP(recorder, request)                                                                                                                               // Execute the concatenated documents through strict decoding.
	if recorder.Code != http.StatusOK {                                                                                                                                // Preserve compatibility HTTP 200 while requiring a JSON-RPC parse error body.
		t.Fatalf("expected compatibility status %d, got %d", http.StatusOK, recorder.Code) // Surface accidental transport behavior drift.
	}
	if !strings.Contains(recorder.Body.String(), `"code":-32700`) { // Require the JSON-RPC parse-error classification in the response envelope.
		t.Fatalf("expected parse error response, got %s", recorder.Body.String()) // Surface accidental dispatch or malformed error serialization.
	}
}

// TestServeHTTPEchoesLargeNumericIDExactly verifies that compatibility JSON-RPC decoding never rounds request identifiers through float64.
func TestServeHTTPEchoesLargeNumericIDExactly(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                           // Construct the protocol-only handler because ping does not require application dependencies.
	request := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader(`{"jsonrpc":"2.0","method":"ping","id":9007199254740993}`)) // Send the first integer above IEEE-754's exact range so any float64 conversion becomes visible.
	recorder := httptest.NewRecorder()                                                                                                        // Capture the serialized response identifier for exact textual comparison.
	handler.ServeHTTP(recorder, request)                                                                                                      // Execute the request through strict body decoding and the custom request unmarshaler.
	if recorder.Code != http.StatusOK {                                                                                                       // Preserve the compatibility transport's successful HTTP status.
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, recorder.Code, recorder.Body.String()) // Surface transport and body details on failure.
	}
	if !strings.Contains(recorder.Body.String(), `"id":9007199254740993`) { // Require exact wire-value echoing instead of the rounded adjacent integer.
		t.Fatalf("expected exact large numeric id in response, got %s", recorder.Body.String()) // Surface the serialized response for precision diagnosis.
	}
}

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

// TestProcessRequestCancelledNotificationPreservesLargeNumericID verifies that cancellation lookup uses the same exact json.Number representation as request decoding.
func TestProcessRequestCancelledNotificationPreservesLargeNumericID(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                                  // Construct the protocol handler whose in-flight registry is under test.
	requestCtx, cancel := context.WithCancel(context.Background())                                                                                                   // Create one observable cancellation target for the large numeric id.
	cleanup := handler.RegisterInFlightRequest(json.Number("9007199254740993"), cancel)                                                                              // Register the exact integer text preserved by request decoding.
	defer cleanup()                                                                                                                                                  // Remove any unmatched entry if the test exits before cancellation succeeds.
	notification := &Request{JSONRPC: "2.0", Method: "notifications/cancelled", Params: json.RawMessage(`{"requestId":9007199254740993,"reason":"client timeout"}`)} // Send the same large integer through cancellation-param decoding.
	if _, err := handler.ProcessRequest(context.Background(), notification); err != nil {                                                                            // Process the notification through the production cancellation path.
		t.Fatalf("expected large-id cancellation notification to succeed, got error: %v", err) // Surface decode or key-normalization regressions.
	}
	select {
	case <-requestCtx.Done():
		// The exact large numeric key matched and invoked the registered cancellation function.
	default:
		t.Fatal("expected large numeric request id to match the inflight cancellation key") // Surface silent numeric rounding or representation mismatch.
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

// TestRequestClassificationRejectsInvalidIDs verifies that explicit null and fractional request IDs are not mistaken for notifications or valid MCP requests.
func TestRequestClassificationRejectsInvalidIDs(t *testing.T) {
	var nullIDRequest Request                                                                                                       // Allocate one request that will be populated through the production custom JSON decoder.
	if err := json.Unmarshal([]byte(`{"jsonrpc":"2.0","method":"tools/list","params":{},"id":null}`), &nullIDRequest); err != nil { // Decode an explicitly null id so wire presence can be asserted.
		t.Fatalf("expected null-id envelope to decode, got %v", err) // Surface unexpected JSON decoding failures before classification assertions.
	}
	if !nullIDRequest.IDPresent || IsRequest(&nullIDRequest) || IsNotification(&nullIDRequest) { // Require explicit null to remain distinguishable from an omitted notification id.
		t.Fatalf("expected explicit null id to be invalid, got %#v", nullIDRequest) // Surface all decoded request fields and presence metadata.
	}
	handler := NewHandler(nil, nil)                                        // Construct one protocol-only handler because invalid request classification stops before method dispatch.
	_, err := handler.ProcessRequest(context.Background(), &nullIDRequest) // Process the malformed request through the shared JSON-RPC boundary.
	mcpErr, ok := err.(*mcperrors.MCPError)                                // Inspect the typed invalid-request error returned by classification.
	if !ok || mcpErr.Code != -32600 {                                      // Require the standard JSON-RPC Invalid Request code.
		t.Fatalf("expected explicit null id error -32600, got %#v", err) // Surface the unexpected error type or code.
	}
	fractionalRequest := &Request{JSONRPC: "2.0", Method: "tools/list", Params: json.RawMessage(`{}`), ID: 1.5, IDPresent: true} // Build a programmatic request with a non-integral numeric id.
	if IsRequest(fractionalRequest) || IsNotification(fractionalRequest) {                                                       // Reject fractional numbers from both valid message categories.
		t.Fatalf("expected fractional id to be invalid, got %#v", fractionalRequest) // Surface the malformed request on classifier regression.
	}
}

// TestProcessRequestPreservesLegacyProgressMetadata verifies that a generic _meta.progressToken does not accidentally select modern protocol semantics.
func TestProcessRequestPreservesLegacyProgressMetadata(t *testing.T) {
	handler := NewHandler(nil, nil)                                                   // Construct one handler without an orchestrator because tools/list reads only embedded metadata.
	params := json.RawMessage(`{"_meta":{"progressToken":"legacy-progress"}}`)        // Build a legacy MCP request that uses the cross-era progress metadata member only.
	request := &Request{JSONRPC: "2.0", Method: "tools/list", Params: params, ID: 91} // Build one initialization-era tool discovery request with generic metadata.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute era classification and normal legacy tool discovery.
	if err != nil {                                                      // Fail when progress metadata is mistaken for incomplete modern metadata.
		t.Fatalf("expected legacy progress metadata to remain valid, got %v", err) // Surface the misclassification diagnostic.
	}
	resultObject, ok := result.(map[string]interface{}) // Read the undecorated legacy tool list result.
	if !ok {                                            // Require the ordinary object response shape.
		t.Fatalf("expected legacy tools/list result object, got %#v", result) // Surface malformed legacy output.
	}
	if _, modernResult := resultObject["resultType"]; modernResult { // Ensure generic legacy metadata did not trigger modern result decoration.
		t.Fatalf("expected legacy result without resultType, got %#v", resultObject) // Surface accidental modern-era selection.
	}
	if _, modernCache := resultObject["ttlMs"]; modernCache { // Ensure cache metadata introduced by current MCP is not emitted to initialization-era clients.
		t.Fatalf("expected legacy result without ttlMs, got %#v", resultObject) // Surface accidental current-protocol cache leakage.
	}
}

// TestProcessRequestReturnsModernDiscovery verifies mandatory server discovery and result decoration directly on the shared stdio-compatible dispatch path.
func TestProcessRequestReturnsModernDiscovery(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                                                                                              // Construct one handler without business dependencies because discovery is pure protocol metadata.
	params := json.RawMessage(`{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{},"io.modelcontextprotocol/clientInfo":{"name":"stdio-test","version":"1.0.0"}}}`) // Build complete request-local modern metadata as a stdio client would send it.
	request := &Request{JSONRPC: "2.0", Method: "server/discover", Params: params, ID: "discover"}                                                                                                                               // Build the mandatory discovery probe with an id so it receives one result.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute discovery through the transport-independent JSON-RPC dispatcher.
	if err != nil {                                                      // Fail when a complete modern discovery probe is rejected.
		t.Fatalf("expected modern discovery to succeed, got %v", err) // Surface the protocol failure for metadata or dispatch debugging.
	}
	resultObject, ok := result.(map[string]interface{}) // Read the decorated discovery object returned to both stdio and HTTP transports.
	if !ok {                                            // Reject any non-object result because modern MCP success values require object results.
		t.Fatalf("expected discovery result object, got %#v", result) // Surface the malformed result shape.
	}
	if resultObject["resultType"] != mcperrors.ResultTypeComplete { // Require the current protocol's explicit final-result discriminator.
		t.Fatalf("expected complete resultType, got %#v", resultObject["resultType"]) // Surface the missing or incorrect discriminator.
	}
	supportedVersions, ok := resultObject["supportedVersions"].([]interface{})                          // Read the JSON-normalized preference-ordered version list.
	if !ok || len(supportedVersions) != 3 || supportedVersions[0] != mcperrors.CurrentProtocolVersion { // Require current plus two supported legacy revisions with modern first.
		t.Fatalf("expected dual-era supported versions, got %#v", resultObject["supportedVersions"]) // Surface the negotiation metadata on regression.
	}
	resultMetadata, ok := resultObject["_meta"].(map[string]interface{}) // Read response-local server identity metadata added by the shared decorator.
	if !ok || resultMetadata[mcperrors.ServerInfoMetaKey] == nil {       // Require identity without relying on a previous initialize exchange.
		t.Fatalf("expected modern serverInfo metadata, got %#v", resultObject["_meta"]) // Surface the malformed metadata object.
	}
}

// TestProcessRequestDecoratesModernToolsList verifies that shared dispatch adds current result fields while retaining deterministic catalog cache metadata.
func TestProcessRequestDecoratesModernToolsList(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                 // Construct one handler without an orchestrator because tools/list reads only embedded metadata.
	params := json.RawMessage(`{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}`) // Build the minimum complete modern metadata object.
	request := &Request{JSONRPC: "2.0", Method: "tools/list", Params: params, ID: 10}                                                               // Build one stateless tool discovery request.

	result, err := handler.ProcessRequest(context.Background(), request) // Execute tools/list through the same path used by stdio.
	if err != nil {                                                      // Fail when valid modern tool discovery is rejected.
		t.Fatalf("expected modern tools/list to succeed, got %v", err) // Surface the unexpected protocol or catalog error.
	}
	resultObject, ok := result.(map[string]interface{}) // Read the JSON-normalized and decorated result object.
	if !ok {                                            // Reject any non-object tool list response.
		t.Fatalf("expected tools/list result object, got %#v", result) // Surface the malformed payload.
	}
	if resultObject["resultType"] != mcperrors.ResultTypeComplete || resultObject["cacheScope"] != mcperrors.CacheScopePublic { // Require completion and public cache semantics.
		t.Fatalf("expected decorated public tools/list result, got %#v", resultObject) // Surface the entire compact result for field diagnosis.
	}
	if resultObject["ttlMs"] != mcperrors.CatalogCacheTTLMS { // Require the integer cache duration injected after modern result normalization.
		t.Fatalf("expected tools/list ttlMs %d, got %#v", mcperrors.CatalogCacheTTLMS, resultObject["ttlMs"]) // Surface the actual cache duration.
	}
	tools, ok := resultObject["tools"].([]interface{}) // Read the normalized tool array returned to clients.
	if !ok || len(tools) == 0 {                        // Require at least one discovered tool after registry construction.
		t.Fatalf("expected non-empty modern tool catalog, got %#v", resultObject["tools"]) // Surface malformed or empty discovery data.
	}
}

// TestProcessRequestSeparatesModernResourcesAndTemplates verifies that current clients receive concrete resources and parameterized URI templates from their standard distinct methods.
func TestProcessRequestSeparatesModernResourcesAndTemplates(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                 // Construct one protocol-only handler because both resource catalog methods use static metadata.
	params := json.RawMessage(`{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}`) // Build the minimum complete modern request metadata reused by both catalog requests.
	resourcesRequest := &Request{JSONRPC: "2.0", Method: "resources/list", Params: params, ID: 14}                                                  // Request the concrete resource set through the current protocol method.

	resourcesResult, err := handler.ProcessRequest(context.Background(), resourcesRequest) // Execute modern resources/list through era-aware shared dispatch.
	if err != nil {                                                                        // Fail when the required resource-list method is rejected.
		t.Fatalf("expected modern resources/list to succeed, got %v", err) // Surface the unexpected protocol or dispatch error.
	}
	resourcesObject, ok := resourcesResult.(map[string]interface{}) // Read the decorated concrete resource result.
	if !ok {                                                        // Reject malformed non-object modern results.
		t.Fatalf("expected resources/list result object, got %#v", resourcesResult) // Surface the actual result type and value.
	}
	resources, ok := resourcesObject["resources"].([]interface{}) // Read the JSON-normalized concrete resource array.
	if !ok || len(resources) != 0 {                               // Require an explicit empty set because this server cannot enumerate caller-specific IDs.
		t.Fatalf("expected empty concrete resource list, got %#v", resourcesObject["resources"]) // Surface placeholder leakage or a malformed array.
	}

	templatesRequest := &Request{JSONRPC: "2.0", Method: "resources/templates/list", Params: params, ID: 15} // Request the parameterized URI catalog through its standard modern method.
	templatesResult, err := handler.ProcessRequest(context.Background(), templatesRequest)                   // Execute template discovery through modern dispatch and result decoration.
	if err != nil {                                                                                          // Fail when the implemented template-list method is rejected.
		t.Fatalf("expected modern resources/templates/list to succeed, got %v", err) // Surface the unexpected protocol or dispatch error.
	}
	templatesObject, ok := templatesResult.(map[string]interface{}) // Read the decorated resource-template result.
	if !ok {                                                        // Reject malformed non-object modern results.
		t.Fatalf("expected resource templates result object, got %#v", templatesResult) // Surface the actual result type and value.
	}
	templates, ok := templatesObject["resourceTemplates"].([]interface{}) // Read the JSON-normalized standard template array.
	if !ok || len(templates) != 3 {                                       // Require all three Appium URI patterns under the standard field.
		t.Fatalf("expected 3 resource templates, got %#v", templatesObject["resourceTemplates"]) // Surface missing or malformed template discovery data.
	}
	firstTemplate, ok := templates[0].(map[string]interface{}) // Inspect one normalized entry for the protocol-defined URI field.
	if !ok || firstTemplate["uriTemplate"] == nil {            // Require a standard resource template rather than a placeholder concrete URI.
		t.Fatalf("expected uriTemplate in modern template entry, got %#v", templates[0]) // Surface the malformed template object.
	}
	if _, hasLegacyURI := firstTemplate["uri"]; hasLegacyURI { // Prevent the historical non-standard field from leaking into modern template discovery.
		t.Fatalf("expected modern template without legacy uri field, got %#v", firstTemplate) // Surface the incompatible field on regression.
	}
	if templatesObject["ttlMs"] != mcperrors.CatalogCacheTTLMS || templatesObject["cacheScope"] != mcperrors.CacheScopePublic { // Require the cache controls mandated for resource-template lists.
		t.Fatalf("expected public template cache metadata, got %#v", templatesObject) // Surface the full decorated object for cache-policy diagnosis.
	}
}

// TestProcessRequestRejectsMissingModernCapabilities verifies that every modern request declares client capabilities even when the object is empty.
func TestProcessRequestRejectsMissingModernCapabilities(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                 // Construct one protocol-only handler because validation stops before discovery.
	params := json.RawMessage(`{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}`) // Omit the required clientCapabilities metadata key deliberately.
	request := &Request{JSONRPC: "2.0", Method: "server/discover", Params: params, ID: 11}          // Build a modern discovery probe whose metadata is incomplete.

	_, err := handler.ProcessRequest(context.Background(), request) // Execute validation through the shared stdio-compatible path.
	mcpErr, ok := err.(*mcperrors.MCPError)                         // Inspect the typed protocol failure returned before dispatch.
	if !ok || mcpErr.Code != -32602 {                               // Require the current protocol's malformed-request error code.
		t.Fatalf("expected missing capabilities error -32602, got %#v", err) // Surface the unexpected error type or code.
	}
}

// TestProcessRequestRejectsUnsupportedModernVersion verifies that stateless clients receive retryable supported and requested version data.
func TestProcessRequestRejectsUnsupportedModernVersion(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                 // Construct one protocol-only handler because version selection occurs before discovery.
	params := json.RawMessage(`{"_meta":{"io.modelcontextprotocol/protocolVersion":"1900-01-01","io.modelcontextprotocol/clientCapabilities":{}}}`) // Request one unavailable protocol revision with otherwise-complete metadata.
	request := &Request{JSONRPC: "2.0", Method: "server/discover", Params: params, ID: 12}                                                          // Build the standard discovery probe used for stdio era and version detection.

	_, err := handler.ProcessRequest(context.Background(), request)  // Execute request-local version validation.
	mcpErr, ok := err.(*mcperrors.MCPError)                          // Inspect the typed retryable protocol error.
	if !ok || mcpErr.Code != mcperrors.ErrorCodeUnsupportedVersion { // Require the exact specification-reserved version error.
		t.Fatalf("expected unsupported version error %d, got %#v", mcperrors.ErrorCodeUnsupportedVersion, err) // Surface the unexpected failure.
	}
	errorData, ok := mcpErr.Data.(map[string]interface{})                               // Read the retry metadata supplied with the version error.
	if !ok || errorData["requested"] != "1900-01-01" || errorData["supported"] == nil { // Require both the rejected value and available alternatives.
		t.Fatalf("expected retryable version data, got %#v", mcpErr.Data) // Surface incomplete negotiation metadata.
	}
}

// TestProcessRequestRejectsLegacyPingUnderModernMetadata verifies that legacy liveness remains available only to initialization-based clients.
func TestProcessRequestRejectsLegacyPingUnderModernMetadata(t *testing.T) {
	handler := NewHandler(nil, nil)                                                                                                                 // Construct one protocol-only handler because modern method gating precedes business dispatch.
	params := json.RawMessage(`{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}`) // Build complete modern stateless metadata.
	request := &Request{JSONRPC: "2.0", Method: "ping", Params: params, ID: 13}                                                                     // Invoke the lifecycle method removed by the current MCP revision.

	_, err := handler.ProcessRequest(context.Background(), request) // Execute modern method gating directly.
	mcpErr, ok := err.(*mcperrors.MCPError)                         // Inspect the standard unknown-method response.
	if !ok || mcpErr.Code != -32601 {                               // Require ping to be absent from the current method surface.
		t.Fatalf("expected modern ping method-not-found, got %#v", err) // Surface accidental legacy leakage into modern semantics.
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
