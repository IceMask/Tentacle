// mcp_test.go verifies the non-integration MCP protocol behavior that should remain testable without a live orchestrator or device stack.
package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"mcp_for_appium/internal/config"
	internalerrors "mcp_for_appium/internal/errors"
)

// expectedToolCatalogSize stores the current embedded MCP tool count so catalog drift is detected immediately in unit tests.
const expectedToolCatalogSize = 26

// mustMCPError asserts that one returned error is an MCPError with the expected code and returns the typed error for further assertions.
func mustMCPError(t *testing.T, err error, expectedCode int) *MCPError {
	t.Helper()      // Mark this helper so failures point at the calling test rather than the shared assertion helper.
	if err == nil { // Reject missing errors because the caller is explicitly testing one failure path.
		t.Fatal("expected MCP error, got nil") // Stop immediately because no MCP error fields can be asserted on a nil error.
	}
	mcpErr, ok := err.(*MCPError) // Type-assert the returned error so the test can inspect MCP protocol fields directly.
	if !ok {                      // Reject non-MCP errors because the protocol layer should always expose structured MCP failures.
		t.Fatalf("expected MCPError, got %T: %v", err, err) // Surface the actual error type so protocol-wrapping regressions are easy to diagnose.
	}
	if mcpErr.Code != expectedCode { // Assert the exact MCP error code so clients can depend on stable protocol semantics.
		t.Fatalf("expected MCP error code %d, got %d", expectedCode, mcpErr.Code) // Surface the actual code so routing regressions are easy to diagnose.
	}

	return mcpErr // Return the typed MCP error so callers can assert message and data fields without repeating the type assertion.
}

// TestMCPHandlerInitializeReturnsServerCapabilities verifies that an unknown legacy initialize version receives the historical fallback, capabilities, and server metadata.
func TestMCPHandlerInitializeReturnsServerCapabilities(t *testing.T) {
	handler := NewMCPHandler(nil)                                                                                                         // Construct one handler without an orchestrator because initialize is pure protocol metadata.
	params := json.RawMessage(`{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test-client","version":"1.0.0"}}`) // Build one representative initialize payload that matches the MCP handshake shape.

	result, err := handler.Initialize(context.Background(), params) // Execute the real initialize handler so protocol metadata is tested end to end.
	if err != nil {                                                 // Fail immediately when initialize unexpectedly rejects a valid handshake request.
		t.Fatalf("expected initialize to succeed, got error: %v", err) // Surface the unexpected error so handshake regressions are easy to diagnose.
	}
	resultMap, ok := result.(map[string]interface{}) // Type-assert the generic response into the expected object shape.
	if !ok {                                         // Reject non-map results because initialize must return a structured MCP response object.
		t.Fatalf("expected initialize response map, got %T", result) // Surface the actual response type so protocol regressions are easy to diagnose.
	}
	if resultMap["protocolVersion"] != "2025-06-18" { // Assert the historical fallback used only for unknown initialization-era protocol versions.
		t.Fatalf("expected protocolVersion 2025-06-18, got %#v", resultMap["protocolVersion"]) // Surface the actual version so handshake regressions are easy to diagnose.
	}
	capabilities, ok := resultMap["capabilities"].(map[string]interface{}) // Extract the capabilities map so tool and resource support can be asserted precisely.
	if !ok {                                                               // Reject malformed capabilities because MCP clients rely on this shape during negotiation.
		t.Fatalf("expected capabilities map, got %#v", resultMap["capabilities"]) // Surface the actual value so protocol regressions are easy to diagnose.
	}
	toolCapabilities, ok := capabilities["tools"].(map[string]interface{}) // Extract the tools capability section so listChanged can be asserted directly.
	if !ok || toolCapabilities["listChanged"] != false {                   // Assert the static-tool-catalog contract exposed by this server.
		t.Fatalf("expected static tools capability, got %#v", capabilities["tools"]) // Surface the actual capability payload so negotiation regressions are easy to diagnose.
	}
	resourceCapabilities, ok := capabilities["resources"].(map[string]interface{})                         // Extract the resources capability section so subscription support can be asserted directly.
	if !ok || resourceCapabilities["subscribe"] != false || resourceCapabilities["listChanged"] != false { // Assert that the server no longer advertises unsupported MCP resource subscriptions.
		t.Fatalf("expected non-subscribed resources capability, got %#v", capabilities["resources"]) // Surface the actual capability payload so negotiation regressions are easy to diagnose.
	}
	serverInfo, ok := resultMap["serverInfo"].(map[string]interface{})                        // Extract the server metadata so name and version can be asserted directly.
	if !ok || serverInfo["name"] != "MCP Mobile Worker" || serverInfo["version"] != "1.0.0" { // Assert the exposed server identity so client diagnostics remain stable.
		t.Fatalf("expected MCP Mobile Worker server info, got %#v", resultMap["serverInfo"]) // Surface the actual metadata so protocol regressions are easy to diagnose.
	}
}

// TestToolRegistryCatalogAndValidation verifies that the embedded tool registry exposes the full catalog and enforces JSON schema validation.
func TestToolRegistryCatalogAndValidation(t *testing.T) {
	registry := NewToolRegistry()              // Construct the real tool registry so the embedded tool definitions are validated through production code paths.
	tools := registry.List()                   // List every registered tool so the catalog size and key tool names can be asserted.
	if len(tools) != expectedToolCatalogSize { // Assert the current embedded tool count so catalog drift is detected immediately.
		t.Fatalf("expected %d tools, got %d", expectedToolCatalogSize, len(tools)) // Surface the actual count so catalog regressions are easy to diagnose.
	}
	for index := 1; index < len(tools); index++ { // Compare every adjacent pair to enforce deterministic lexicographic discovery order.
		if tools[index-1].Name >= tools[index].Name { // Reject duplicates and unstable ordering because clients cache the serialized catalog.
			t.Fatalf("expected tools sorted by name, found %q before %q", tools[index-1].Name, tools[index].Name) // Surface the first offending pair for catalog debugging.
		}
	}

	requiredToolNames := map[string]bool{ // Define representative tools across the major capability areas so the registry assertion is broader than a raw count.
		"startSession":          false,
		"executePlan":           false,
		"getSemanticSnapshot":   false,
		"findElement":           false,
		"tap":                   false,
		"scheduleDeviceFarmRun": false,
		"adbShell":              false,
	}
	for _, tool := range tools { // Walk the full catalog once so representative tool presence can be asserted without depending on list ordering.
		if _, exists := requiredToolNames[tool.Name]; exists { // Mark each required tool when it appears in the embedded catalog.
			requiredToolNames[tool.Name] = true // Record the presence of the required tool so missing capabilities fail explicitly below.
		}
	}
	for toolName, seen := range requiredToolNames { // Assert that every representative tool was actually present in the embedded catalog.
		if !seen { // Fail when any required tool is missing because that indicates either embed drift or accidental removal.
			t.Fatalf("expected tool %q to be registered", toolName) // Surface the missing tool so catalog regressions are easy to diagnose.
		}
	}

	tool, exists := registry.Get("startSession") // Retrieve one concrete tool so schema presence can be asserted directly.
	if !exists {                                 // Reject missing startSession because session creation is a core catalog capability.
		t.Fatal("expected startSession tool to exist") // Stop immediately because schema assertions depend on a retrieved tool value.
	}
	if tool.InputSchema == nil { // Reject nil schemas because MCP clients depend on per-tool input-schema metadata.
		t.Fatal("expected startSession input schema") // Stop immediately because schema validation assertions depend on an actual schema.
	}
	if tool.InputSchema["$schema"] != JSONSchema202012 { // Require tools/list to declare the current MCP default JSON Schema dialect explicitly.
		t.Fatalf("expected JSON Schema 2020-12 declaration, got %#v", tool.InputSchema["$schema"]) // Surface the advertised dialect when schema alignment regresses.
	}

	validArgs := json.RawMessage(`{"projectId":"test-project","w3cCapsJson":{"platformName":"iOS"}}`) // Build one minimal valid payload for the startSession schema.
	if err := registry.Validate("startSession", validArgs); err != nil {                              // Validate the minimal payload through the production schema path.
		t.Fatalf("expected valid startSession args, got error: %v", err) // Surface the unexpected validation error so schema regressions are easy to diagnose.
	}

	invalidArgs := json.RawMessage(`{"w3cCapsJson":{"platformName":"iOS"}}`)          // Build one invalid payload that omits the required projectId field.
	mcpErr := mustMCPError(t, registry.Validate("startSession", invalidArgs), -32602) // Validate the invalid payload and assert the schema-failure MCP code.
	if mcpErr.Message != "Schema validation failed" {                                 // Assert the stable validation message so clients can classify input failures consistently.
		t.Fatalf("expected schema validation failure message, got %q", mcpErr.Message) // Surface the actual message so validation regressions are easy to diagnose.
	}
}

// TestCompileToolInputSchemaSupportsDraft202012 verifies validation of a keyword whose cross-subschema behavior requires the MCP-mandated 2020-12 dialect.
func TestCompileToolInputSchemaSupportsDraft202012(t *testing.T) {
	tool := Tool{ // Build one isolated schema fixture that uses 2020-12 unevaluatedProperties semantics.
		Name: "draft2020Test", // Assign a safe unique name used only to construct the local schema resource URI.
		InputSchema: map[string]interface{}{ // Compose an object across allOf while disallowing properties left unevaluated afterward.
			"$schema": JSONSchema202012, // Explicitly select the current MCP default dialect for this regression fixture.
			"type":    "object",         // Require the validated tool arguments to use an object shape.
			"allOf": []interface{}{ // Evaluate the declared property inside a composed subschema.
				map[string]interface{}{ // Define the one permitted property within the allOf branch.
					"properties": map[string]interface{}{ // Mark name as evaluated when it satisfies the nested declaration.
						"name": map[string]interface{}{"type": "string"}, // Require the permitted name value to be a string.
					},
					"required": []interface{}{"name"}, // Require the permitted property so the accepted fixture is meaningful.
				},
			},
			"unevaluatedProperties": false, // Reject properties not evaluated by the composed 2020-12 schema.
		},
	}
	compiledSchema, err := compileToolInputSchema(tool) // Compile the fixture through the production dialect-pinned compiler.
	if err != nil {                                     // Fail when the server cannot compile a valid 2020-12 tool schema.
		t.Fatalf("expected JSON Schema 2020-12 compilation to succeed, got %v", err) // Surface the compiler diagnostic for dependency or dialect regressions.
	}
	if err := compiledSchema.Validate(map[string]interface{}{"name": "appium"}); err != nil { // Validate an object whose only property is evaluated inside allOf.
		t.Fatalf("expected composed 2020-12 schema to accept evaluated property, got %v", err) // Surface unexpected rejection of valid arguments.
	}
	if err := compiledSchema.Validate(map[string]interface{}{"name": "appium", "extra": true}); err == nil { // Validate an object containing one property left unevaluated by allOf.
		t.Fatal("expected 2020-12 unevaluatedProperties to reject extra property") // Prove the modern dialect keyword is enforced rather than silently ignored.
	}
}

// TestNewMCPHandlerWithConfigRemovesADBShell verifies that the gateway-level adbShell disable flag removes the tool from MCP discovery and invocation.
func TestNewMCPHandlerWithConfigRemovesADBShell(t *testing.T) {
	handler := NewMCPHandlerWithConfig(nil, config.GatewayConfig{DisableADBShellTool: true}) // Construct one MCP handler with adbShell explicitly disabled so discovery and call-path shaping can be asserted directly.
	tools := handler.registry.List()                                                         // Read the live filtered registry so the externally visible tool catalog can be asserted directly.
	if len(tools) != expectedToolCatalogSize-1 {                                             // Fail when disabling adbShell does not reduce the live tool count by one.
		t.Fatalf("expected %d tools with adbShell disabled, got %d", expectedToolCatalogSize-1, len(tools)) // Surface the unexpected count so kill-switch regressions are obvious.
	}
	if _, exists := handler.registry.Get("adbShell"); exists { // Fail when the disabled tool still exists in the validation registry.
		t.Fatal("expected adbShell to be removed from the MCP registry when disabled") // Surface the unexpected registry entry so kill-switch regressions are obvious.
	}
	params := json.RawMessage(`{"name":"adbShell","arguments":{"deviceSerial":"emulator-5554","command":["getprop","ro.build.version.sdk"]}}`) // Build one syntactically valid adbShell request so the production tools/call rejection path can be exercised directly.
	mcpErr := mustMCPError(t, func() error {                                                                                                   // Execute the production tools/call path and capture the disabled-tool rejection for structured assertions.
		_, err := handler.ToolsCall(context.Background(), params) // Invoke the production tools/call entry point with adbShell while the tool is disabled.
		return err                                                // Return the produced error so the shared MCP assertion helper can inspect it.
	}(), -32601)
	if mcpErr.Message != "Tool not found: adbShell" { // Fail when the disabled tool no longer maps to the standard tool-not-found protocol error.
		t.Fatalf("expected adbShell tool-not-found message, got %q", mcpErr.Message) // Surface the unexpected message so disablement regressions are easy to diagnose.
	}
}

// TestMCPHandlerToolsCallRejectsUnknownTool verifies that tools/call returns a structured not-found error for unregistered tools.
func TestMCPHandlerToolsCallRejectsUnknownTool(t *testing.T) {
	handler := NewMCPHandler(nil)                                          // Construct one handler without an orchestrator because the failure path stops before business logic is reached.
	params := json.RawMessage(`{"name":"nonExistentTool","arguments":{}}`) // Build one tools/call payload for a definitely unregistered tool name.

	mcpErr := mustMCPError(t, func() error { // Execute the real tools/call path and capture the returned error for structured assertions.
		_, err := handler.ToolsCall(context.Background(), params) // Invoke the production tools/call entry point with the unknown tool payload.
		return err                                                // Return the error so the shared MCP assertion helper can inspect it.
	}(), -32601)
	if mcpErr.Message != "Tool not found: nonExistentTool" { // Assert the stable not-found message so clients can surface human-readable feedback consistently.
		t.Fatalf("expected unknown-tool message, got %q", mcpErr.Message) // Surface the actual message so routing regressions are easy to diagnose.
	}
}

// TestMCPHandlerToolsCallReturnsInputErrorResult verifies that schema-invalid tool arguments use the MCP isError result contract instead of a JSON-RPC error.
func TestMCPHandlerToolsCallReturnsInputErrorResult(t *testing.T) {
	handler := NewMCPHandler(nil)                                                             // Construct one handler without an orchestrator because validation failures stop before business logic is reached.
	params := json.RawMessage(`{"name":"findElement","arguments":{"sessionId":"session-1"}}`) // Build one invalid findElement payload that omits required strategy and selector fields.

	result, err := handler.ToolsCall(context.Background(), params) // Invoke the production tools/call entry point with the invalid payload.
	if err != nil {                                                // Reject protocol-level errors because schema failures must complete as tool results.
		t.Fatalf("expected MCP tool error result, got protocol error: %v", err) // Surface accidental JSON-RPC error regressions directly.
	}
	resultMap, ok := result.(map[string]interface{}) // Assert the standard MCP tool-result object shape before inspecting its error marker.
	if !ok {                                         // Reject non-object results because tools/call must return a structured result.
		t.Fatalf("expected tool result map, got %T", result) // Surface the unexpected result type for quick protocol diagnosis.
	}
	if isError, ok := resultMap["isError"].(bool); !ok || !isError { // Require the explicit MCP application-error marker on schema failures.
		t.Fatalf("expected isError=true, got %#v", resultMap["isError"]) // Surface the malformed marker so clients do not silently treat the call as successful.
	}
}

// TestNewToolErrorResultDoesNotExposeRawCause verifies that tool-result failures preserve stable codes without returning sensitive wrapped diagnostics.
func TestNewToolErrorResultDoesNotExposeRawCause(t *testing.T) {
	result := newToolErrorResult("startSession", internalerrors.New(internalerrors.CodeStoreRead, "password=secret-value")) // Build one typed failure whose private message would be unsafe to return.
	encodedResult, err := json.Marshal(result)                                                                              // Serialize the complete client-visible result so every nested field is checked together.
	if err != nil {                                                                                                         // Fail when the generated tool result cannot be encoded as JSON.
		t.Fatalf("expected tool error result to marshal, got %v", err) // Surface malformed result values introduced by future changes.
	}
	if strings.Contains(string(encodedResult), "secret-value") { // Reject any nested reflection of the private downstream diagnostic.
		t.Fatalf("expected sensitive cause to be omitted, got %s", encodedResult) // Surface the unsafe response payload for immediate remediation.
	}
	if !strings.Contains(string(encodedResult), string(internalerrors.CodeStoreRead)) { // Require the stable machine-readable code to remain available after sanitization.
		t.Fatalf("expected internal code %s, got %s", internalerrors.CodeStoreRead, encodedResult) // Surface loss of programmatic error classification.
	}
}

// TestMCPHandlerResourcesListReturnsExpectedCatalog verifies that the static MCP resource catalog exposes the three documented resource types.
func TestMCPHandlerResourcesListReturnsExpectedCatalog(t *testing.T) {
	handler := NewMCPHandler(nil) // Construct one handler without an orchestrator because resources/list is a static metadata response.

	result, err := handler.ResourcesList(context.Background(), json.RawMessage(`{}`)) // Execute the real resources/list handler so the catalog is asserted end to end.
	if err != nil {                                                                   // Fail immediately when resources/list unexpectedly rejects an empty request.
		t.Fatalf("expected resources/list to succeed, got error: %v", err) // Surface the unexpected error so resource-catalog regressions are easy to diagnose.
	}
	resultMap, ok := result.(map[string]interface{}) // Type-assert the generic response into the expected object shape.
	if !ok {                                         // Reject non-map results because resources/list must return a structured MCP response object.
		t.Fatalf("expected resources/list response map, got %T", result) // Surface the actual response type so protocol regressions are easy to diagnose.
	}
	resources, ok := resultMap["resources"].([]map[string]interface{}) // Extract the resource list so the static resource URIs can be asserted directly.
	if !ok {                                                           // Reject malformed resources because MCP clients depend on this catalog shape.
		t.Fatalf("expected resources slice, got %#v", resultMap["resources"]) // Surface the actual value so protocol regressions are easy to diagnose.
	}
	if len(resources) != 3 { // Assert the documented resource count so catalog drift is detected immediately.
		t.Fatalf("expected 3 resources, got %d", len(resources)) // Surface the actual count so catalog regressions are easy to diagnose.
	}
	if _, hasTTL := resultMap["ttlMs"]; hasTTL { // Keep the era-neutral handler result compatible with initialization-era resource clients.
		t.Fatalf("expected base resources/list result without modern ttlMs, got %#v", resultMap) // Surface accidental current-protocol leakage into legacy output.
	}
	if _, hasScope := resultMap["cacheScope"]; hasScope { // Keep authorization-aware cache controls in modern result decoration only.
		t.Fatalf("expected base resources/list result without modern cacheScope, got %#v", resultMap) // Surface accidental current-protocol leakage into legacy output.
	}

	expectedURIs := map[string]bool{ // Define the documented MCP resource templates so the catalog assertion is order-independent.
		"mcp://appium/artifacts/{traceId}":  false,
		"mcp://appium/traces/{traceId}":     false,
		"mcp://appium/sessions/{sessionId}": false,
	}
	for _, resource := range resources { // Walk the returned resource list once so each expected URI can be marked when found.
		uri, ok := resource["uri"].(string) // Read the resource URI so the catalog entry can be checked against the expected set.
		if ok {                             // Only mark recognized string URIs because malformed entries should still fail below.
			if _, exists := expectedURIs[uri]; exists { // Recognize one expected URI regardless of list ordering.
				expectedURIs[uri] = true // Record the presence of the expected resource URI.
			}
		}
	}
	for uri, seen := range expectedURIs { // Assert that every documented resource URI was returned by the handler.
		if !seen { // Fail when any expected URI is missing because that indicates catalog drift.
			t.Fatalf("expected resource URI %q to be listed", uri) // Surface the missing URI so resource-catalog regressions are easy to diagnose.
		}
	}
}

// TestMCPHandlerResourcesReadRejectsInvalidURI verifies that resources/read rejects malformed resource URIs before any orchestrator access happens.
func TestMCPHandlerResourcesReadRejectsInvalidURI(t *testing.T) {
	handler := NewMCPHandler(nil)                                     // Construct one handler without an orchestrator because malformed URIs fail before resource loading is attempted.
	params := json.RawMessage(`{"uri":"http://example.com/not-mcp"}`) // Build one malformed URI payload that must fail protocol validation immediately.

	mcpErr := mustMCPError(t, func() error { // Execute the real resources/read path and capture the protocol validation failure for structured assertions.
		_, err := handler.ResourcesRead(context.Background(), params) // Invoke the production resources/read entry point with the invalid URI.
		return err                                                    // Return the error so the shared MCP assertion helper can inspect it.
	}(), -32602)
	if mcpErr.Message != "Invalid resource URI" { // Assert the stable invalid-resource message so clients can classify URI errors consistently.
		t.Fatalf("expected invalid resource URI message, got %q", mcpErr.Message) // Surface the actual message so protocol regressions are easy to diagnose.
	}
}

// TestDecorateModernResourceReadResultUsesPrivateCache verifies that modern user-specific resource content cannot be shared across authorization contexts.
func TestDecorateModernResourceReadResultUsesPrivateCache(t *testing.T) {
	baseResult := newResourceReadResult("mcp://appium/traces/trace-1", `{"trace":"value"}`) // Build the era-neutral resource payload without requiring an orchestrator or database.
	decorated, err := DecorateModernResult("resources/read", baseResult)                    // Apply the production current-protocol result policy for a resource read.
	if err != nil {                                                                         // Fail when a valid resource object cannot be decorated.
		t.Fatalf("expected modern resource decoration to succeed, got %v", err) // Surface unexpected normalization or result-shape failures.
	}
	result, ok := decorated.(map[string]interface{}) // Read the normalized modern result object for cache assertions.
	if !ok {                                         // Reject non-object output because all modern MCP successful results must be objects.
		t.Fatalf("expected decorated resource result object, got %#v", decorated) // Surface the malformed modern result.
	}
	if result["ttlMs"] != ResourceCacheTTLMS { // Require the short resource freshness window injected after JSON normalization.
		t.Fatalf("expected resource ttlMs %d, got %#v", ResourceCacheTTLMS, result["ttlMs"]) // Surface the actual cache duration on regression.
	}
	if result["cacheScope"] != CacheScopePrivate { // Require caller-isolated caching for potentially authorized test data.
		t.Fatalf("expected private resource cache scope, got %#v", result["cacheScope"]) // Surface unsafe public scope regressions immediately.
	}
}

// TestNewResourceReadErrorUsesCurrentProtocolCodes verifies that unavailable resources and backend failures use the JSON-RPC codes required by current MCP.
func TestNewResourceReadErrorUsesCurrentProtocolCodes(t *testing.T) {
	notFoundCause := internalerrors.New(internalerrors.CodeTraceNotFound, "trace not found")    // Build one repository-standard missing resource error without a database dependency.
	notFound := newResourceReadError("mcp://appium/traces/missing", "get trace", notFoundCause) // Map the missing trace through the production resource error helper.
	if notFound.Code != -32602 || notFound.Message != "Resource not found" {                    // Require the current resource-not-found code and stable message.
		t.Fatalf("expected resource not found -32602, got %#v", notFound) // Surface the complete malformed protocol error.
	}
	internalCause := internalerrors.New(internalerrors.CodeStoreRead, "database unavailable")          // Build one backend read failure that must not be reported as a missing URI.
	internalFailure := newResourceReadError("mcp://appium/traces/trace-1", "get trace", internalCause) // Map the backend failure through the same production helper.
	if internalFailure.Code != -32603 || internalFailure.Message != "Failed to read resource" {        // Require the standard JSON-RPC internal error rather than a legacy server code.
		t.Fatalf("expected resource internal error -32603, got %#v", internalFailure) // Surface the complete malformed protocol error.
	}
}

// TestParseResourceURI verifies the resource URI parser for valid and invalid MCP resource identifiers.
func TestParseResourceURI(t *testing.T) {
	testCases := []struct {
		name        string
		uri         string
		wantType    string
		wantID      string
		expectError bool
	}{
		{name: "artifacts URI", uri: "mcp://appium/artifacts/trace_abc123", wantType: "artifacts", wantID: "trace_abc123"},
		{name: "traces URI", uri: "mcp://appium/traces/trace_xyz789", wantType: "traces", wantID: "trace_xyz789"},
		{name: "sessions URI", uri: "mcp://appium/sessions/sess_def456", wantType: "sessions", wantID: "sess_def456"},
		{name: "missing prefix", uri: "http://appium/artifacts/trace_123", expectError: true},
		{name: "missing id", uri: "mcp://appium/artifacts/", expectError: true},
		{name: "missing resource type", uri: "mcp://appium/", expectError: true},
		{name: "empty URI", uri: "", expectError: true},
	}

	for _, testCase := range testCases { // Execute each URI parser scenario as one table-driven subtest so valid and invalid cases share the same assertions cleanly.
		t.Run(testCase.name, func(t *testing.T) {
			resourceType, resourceID, err := parseResourceURI(testCase.uri) // Invoke the production URI parser with the current test case input.
			if testCase.expectError {                                       // Assert the invalid-URI branch when the current case expects one parser failure.
				if err == nil { // Fail when the parser unexpectedly accepts one malformed URI.
					t.Fatal("expected resource URI parse error") // Surface the missing parse error because invalid-URI handling is the behavior under test.
				}
				return // Stop the current subtest once the expected invalid-URI failure has been observed.
			}
			if err != nil { // Fail when the parser unexpectedly rejects one valid URI.
				t.Fatalf("unexpected parse error: %v", err) // Surface the actual parser error so URI regressions are easy to diagnose.
			}
			if resourceType != testCase.wantType || resourceID != testCase.wantID { // Assert both parsed components because clients depend on exact URI decomposition.
				t.Fatalf("expected %q/%q, got %q/%q", testCase.wantType, testCase.wantID, resourceType, resourceID) // Surface the actual parse result so URI regressions are easy to diagnose.
			}
		})
	}
}
