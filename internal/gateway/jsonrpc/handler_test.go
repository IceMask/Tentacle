// handler_test.go verifies selected JSON-RPC handler and schema-validation behaviors directly at the package boundary.
package jsonrpc

import (
	"context"
	"encoding/json"
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
	handler := NewHandler(nil, nil)                                                                      // Construct one handler with no orchestrator dependency because unknown-method dispatch exits before any service call.
	request := &Request{JSONRPC: "2.0", Method: "methodThatDoesNotExist", Params: json.RawMessage(`{}`)} // Build one direct unknown-method request so the fallback dispatch path can be exercised directly.

	if _, err := handler.ProcessRequest(context.Background(), request); err == nil { // Execute the production JSON-RPC fallback path for one unknown method.
		t.Fatal("expected unknown method to fail") // Surface the missing failure because clients depend on stable method-not-found behavior.
	} else if mcpErr, ok := err.(*mcperrors.MCPError); !ok || mcpErr.Code != -32601 { // Fail the test when the fallback path no longer returns the MCP-compatible method-not-found shape.
		t.Fatalf("expected MCP method-not-found error, got %#v", err) // Surface the unexpected error so dispatch regressions are obvious.
	}
}
