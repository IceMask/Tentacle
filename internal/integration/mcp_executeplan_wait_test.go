// mcp_executeplan_wait_test.go verifies MCP executePlan waiting semantics and stdio progress notifications against the live monolith integration harness.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/gateway/capabilities"
	"mcp_for_appium/internal/gateway/mcp"
	"mcp_for_appium/internal/gateway/stdio"
)

// mcpToolsCallContent captures the production tools/call wrapper shape used by the MCP handler.
type mcpToolsCallContent struct {
	Content []struct {
		Type string `json:"type"` // Type stores the MCP content entry type so tests can assert the server returned text content.
		Text string `json:"text"` // Text stores the JSON-formatted tool payload emitted by the production MCP handler.
	} `json:"content"` // Content holds the ordered MCP response content entries returned by the production tools/call path.
	IsError bool `json:"isError"` // IsError mirrors the top-level MCP success indicator returned by the production tools/call path.
}

// decodeMCPToolResult extracts the JSON object embedded inside the first text content entry returned by one production tools/call response.
func decodeMCPToolResult(t *testing.T, response interface{}) map[string]interface{} {
	t.Helper() // Mark this helper so failures point at the calling integration test rather than the shared parser helper.

	rawResponse, err := json.Marshal(response) // Normalize the generic handler response into JSON so the wrapper shape can be decoded deterministically.
	if err != nil {                            // Fail immediately when the generic response cannot be marshaled because no tool-payload assertions can proceed after that.
		t.Fatalf("expected tools/call response to marshal, got error: %v", err) // Surface the marshal failure so MCP wrapper regressions are easy to diagnose.
	}

	var parsedResponse mcpToolsCallContent                               // Allocate the destination structure used to inspect the production tools/call wrapper fields.
	if err := json.Unmarshal(rawResponse, &parsedResponse); err != nil { // Decode the tools/call wrapper exactly as one MCP client would observe it on the wire.
		t.Fatalf("expected tools/call response wrapper to decode, got error: %v", err) // Surface the decode failure so MCP wrapper regressions are easy to diagnose.
	}
	if parsedResponse.IsError { // Reject error wrappers because this helper exists only for successful tools/call responses.
		t.Fatal("expected successful tools/call response, got isError=true") // Surface the unexpected wrapper flag because the happy-path integration test must see one success wrapper.
	}
	if len(parsedResponse.Content) == 0 || parsedResponse.Content[0].Type != "text" { // Reject missing text content because the production MCP handler currently returns JSON text content for tool results.
		t.Fatalf("expected first tools/call content entry to be text, got %#v", parsedResponse.Content) // Surface the actual wrapper content so regressions are easy to diagnose.
	}

	var decoded map[string]interface{}                                                       // Allocate the destination map used to inspect the JSON payload embedded inside the text content entry.
	if err := json.Unmarshal([]byte(parsedResponse.Content[0].Text), &decoded); err != nil { // Decode the embedded JSON payload produced by formatToolResult so field-level assertions can proceed.
		t.Fatalf("expected tool result JSON text to decode, got error: %v", err) // Surface the decode failure so MCP tool-result regressions are easy to diagnose.
	}

	return decoded // Return the decoded JSON object so each integration test can assert its own expected executePlan fields.
}

// waitRequestLine returns one JSON-RPC tools/call line that requests synchronous executePlan waiting and progress notifications.
func waitRequestLine(sessionID string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"executePlan","arguments":{"sessionId":"%s","plan":{"steps":[{"type":"sendKeys","selector":"id=username","params":{"text":"demo-user"}},{"type":"click","selector":"xpath=//button[@id='submit']"}]}},"_meta":{"progressToken":"progress-1"}}}`, sessionID) // Build one complete line-delimited JSON-RPC request that exercises the stdio MCP wait path with progress enabled.
}

// TestMCPExecutePlanWaitsForTerminalTraceByDefault verifies that MCP tools/call executePlan now waits for terminal completion by default and returns the final trace snapshot.
func TestMCPExecutePlanWaitsForTerminalTraceByDefault(t *testing.T) {
	harness := newMonolithFlowHarness(t) // Start one isolated monolith integration harness so the MCP handler exercises real persistence, Redis, and fake Appium calls.
	ctx := context.Background()          // Use one shared background context because the integration flow runs synchronously inside the current test.

	session, err := harness.service.StartSession(ctx, "integration-project", map[string]interface{}{"platformName": "Android", "appium:udid": "emulator"}) // Create one real session so executePlan can target one persisted session owned by the harness.
	if err != nil {                                                                                                                                        // Fail immediately when session creation unexpectedly fails because executePlan depends on one active session.
		t.Fatalf("expected MCP integration start session to succeed, got error: %v", err) // Surface the session failure so executePlan MCP regressions are easy to diagnose.
	}

	handler := mcp.NewMCPHandler(harness.service)                                                                                                                                                                                                                                        // Construct the production MCP handler over the live monolith service so tools/call exercises real orchestration behavior.
	result, err := handler.ToolsCall(ctx, json.RawMessage(`{"name":"executePlan","arguments":{"sessionId":"`+session.ID+`","plan":{"steps":[{"type":"sendKeys","selector":"id=username","params":{"text":"demo-user"}},{"type":"click","selector":"xpath=//button[@id='submit']"}]}}}`)) // Execute the production MCP tools/call path without one explicit wait flag so the default waiting contract is exercised end to end.
	if err != nil {                                                                                                                                                                                                                                                                      // Fail immediately when the MCP handler unexpectedly rejects one valid executePlan tool invocation.
		t.Fatalf("expected MCP executePlan tools/call to succeed, got error: %v", err) // Surface the handler failure so executePlan wait regressions are easy to diagnose.
	}

	decoded := decodeMCPToolResult(t, result) // Decode the embedded JSON result so final trace state and event replay can be asserted directly.
	if decoded["status"] != "completed" {     // Assert that the default wait behavior now returns the terminal trace status instead of always returning running.
		t.Fatalf("expected default MCP executePlan status completed, got %#v", decoded["status"]) // Surface the actual status so wait-contract regressions are easy to diagnose.
	}
	if decoded["waitedForCompletion"] != true { // Assert that the result explicitly records the synchronous wait contract that was used.
		t.Fatalf("expected waitedForCompletion=true, got %#v", decoded["waitedForCompletion"]) // Surface the actual flag so client-behavior regressions are easy to diagnose.
	}
	if decoded["waitTimedOut"] != false { // Assert that the synchronous wait completed within the default wait timeout for this short integration plan.
		t.Fatalf("expected waitTimedOut=false, got %#v", decoded["waitTimedOut"]) // Surface the actual flag so timeout-contract regressions are easy to diagnose.
	}
	trace, ok := decoded["trace"].(map[string]interface{}) // Extract the terminal trace snapshot so the persisted terminal state can be asserted directly.
	traceStatus := trace["status"]                         // Prefer one lower-case status key in case future result formatting normalizes struct fields into canonical JSON names.
	if traceStatus == nil {                                // Fall back to the current Go-struct field name because formatToolResult currently marshals the postgres.Trace struct directly.
		traceStatus = trace["Status"] // Read the exported struct-field JSON name emitted by the current production formatter.
	}
	if !ok || traceStatus != "completed" { // Reject missing or non-terminal trace objects because MCP clients depend on one final trace snapshot after a waited executePlan.
		t.Fatalf("expected completed trace payload, got %#v", decoded["trace"]) // Surface the actual trace payload so wait-result regressions are easy to diagnose.
	}
	events, ok := decoded["events"].([]interface{}) // Extract the replayable event list so the handler's terminal snapshot can be asserted beyond the trace row alone.
	if !ok || len(events) < 5 {                     // Expect running and passed events for both steps plus one trace-level terminal event.
		t.Fatalf("expected at least 5 events in waited MCP result, got %#v", decoded["events"]) // Surface the actual events payload so wait-result regressions are easy to diagnose.
	}
}

// TestMCPExecutePlanCanReturnImmediatelyWhenWaitingIsDisabled verifies that MCP tools/call executePlan can still preserve the old async-submit behavior when clients opt out of waiting.
func TestMCPExecutePlanCanReturnImmediatelyWhenWaitingIsDisabled(t *testing.T) {
	harness := newMonolithFlowHarness(t) // Start one isolated monolith integration harness so the MCP handler exercises the real enqueue path.
	ctx := context.Background()          // Use one shared background context because the integration flow runs synchronously inside the current test.

	session, err := harness.service.StartSession(ctx, "integration-project", map[string]interface{}{"platformName": "Android", "appium:udid": "emulator"}) // Create one real session so executePlan can target one persisted session owned by the harness.
	if err != nil {                                                                                                                                        // Fail immediately when session creation unexpectedly fails because executePlan depends on one active session.
		t.Fatalf("expected MCP integration start session to succeed, got error: %v", err) // Surface the session failure so async executePlan regressions are easy to diagnose.
	}

	handler := mcp.NewMCPHandler(harness.service)                                                                                                                                                               // Construct the production MCP handler over the live monolith service so tools/call exercises real orchestration behavior.
	result, err := handler.ToolsCall(ctx, json.RawMessage(`{"name":"executePlan","arguments":{"sessionId":"`+session.ID+`","waitForCompletion":false,"plan":{"steps":[{"type":"wait","params":{"ms":25}}]}}}`)) // Execute the production MCP tools/call path with waiting explicitly disabled so the async-submit compatibility path is exercised end to end.
	if err != nil {                                                                                                                                                                                             // Fail immediately when the MCP handler unexpectedly rejects one valid executePlan tool invocation.
		t.Fatalf("expected MCP executePlan async tools/call to succeed, got error: %v", err) // Surface the handler failure so async executePlan regressions are easy to diagnose.
	}

	decoded := decodeMCPToolResult(t, result) // Decode the embedded JSON result so async-submit fields can be asserted directly.
	if decoded["status"] != "running" {       // Assert that the compatibility path still returns the immediate running state for clients that explicitly opt out of waiting.
		t.Fatalf("expected async MCP executePlan status running, got %#v", decoded["status"]) // Surface the actual status so async-submit regressions are easy to diagnose.
	}
	if decoded["pollingRequired"] != true { // Assert that the compatibility path now states explicitly that the caller must use follow-up trace reads.
		t.Fatalf("expected pollingRequired=true, got %#v", decoded["pollingRequired"]) // Surface the actual flag so async-submit regressions are easy to diagnose.
	}
	if decoded["waitedForCompletion"] != false { // Assert that the result explicitly records that synchronous waiting was disabled by the caller.
		t.Fatalf("expected waitedForCompletion=false, got %#v", decoded["waitedForCompletion"]) // Surface the actual flag so async-submit regressions are easy to diagnose.
	}
}

// TestStdioTransportEmitsProgressNotificationsForExecutePlan verifies that the stdio transport forwards MCP progress notifications before the final waited executePlan response.
func TestStdioTransportEmitsProgressNotificationsForExecutePlan(t *testing.T) {
	harness := newMonolithFlowHarness(t) // Start one isolated monolith integration harness so the stdio transport exercises the live MCP handler and orchestrator service.
	ctx := context.Background()          // Use one shared background context because the transport processes exactly one request during this integration test.

	session, err := harness.service.StartSession(ctx, "integration-project", map[string]interface{}{"platformName": "Android", "appium:udid": "emulator"}) // Create one real session so the stdio tools/call request can target one active session.
	if err != nil {                                                                                                                                        // Fail immediately when session creation unexpectedly fails because executePlan depends on one active session.
		t.Fatalf("expected stdio integration start session to succeed, got error: %v", err) // Surface the session failure so stdio progress regressions are easy to diagnose.
	}

	var stdout bytes.Buffer                                                                                                                                                                // Capture the stdio response stream in memory so emitted progress notifications and the final response can be asserted directly.
	transport := stdio.NewTransportWithIO(harness.service, capabilities.NewService(config.GatewayConfig{}), strings.NewReader(waitRequestLine(session.ID)+"\n"), &stdout, &bytes.Buffer{}) // Construct one production stdio transport with in-memory streams so the integration test can inspect every emitted JSON-RPC line.
	if err := transport.Run(ctx); err != nil {                                                                                                                                             // Run the production stdio transport until it consumes the single request line and reaches EOF.
		t.Fatalf("expected stdio transport run to succeed, got error: %v", err) // Surface the transport failure so stdio progress regressions are easy to diagnose.
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n") // Split the captured stdio output into newline-delimited JSON-RPC messages so notifications and responses can be asserted independently.
	if len(lines) < 2 {                                              // Expect at least one progress notification plus one final response line from the waited executePlan request.
		t.Fatalf("expected at least one progress notification and one final response, got %d lines: %q", len(lines), stdout.String()) // Surface the raw output so transport regressions are easy to diagnose.
	}

	progressSeen := false        // Track whether at least one notifications/progress message was emitted before the final response.
	responseSeen := false        // Track whether the final tools/call response carrying the completed result was emitted.
	for _, line := range lines { // Decode every emitted JSON-RPC message so both notification and response paths are asserted against the real wire format.
		var envelope map[string]interface{}                             // Allocate the destination map used to inspect each emitted JSON-RPC envelope.
		if err := json.Unmarshal([]byte(line), &envelope); err != nil { // Decode one emitted JSON-RPC line so method, id, and params can be asserted directly.
			t.Fatalf("expected stdio output line to be valid JSON, got error: %v", err) // Surface the decode failure so wire-format regressions are easy to diagnose.
		}
		if envelope["method"] == "notifications/progress" { // Detect progress notifications emitted by the transport-backed MCP progress reporter.
			params, ok := envelope["params"].(map[string]interface{}) // Extract the notification params so the correlation token and status payload can be asserted directly.
			if !ok {                                                  // Reject malformed progress notifications because MCP clients depend on the standard params shape.
				t.Fatalf("expected progress notification params map, got %#v", envelope["params"]) // Surface the actual params payload so notification regressions are easy to diagnose.
			}
			if params["progressToken"] != "progress-1" { // Assert that the emitted progress notification reuses the exact client-supplied progress token.
				t.Fatalf("expected progress token progress-1, got %#v", params["progressToken"]) // Surface the actual token so MCP correlation regressions are easy to diagnose.
			}
			progressSeen = true // Record that the production stdio transport emitted at least one progress notification for the waited executePlan request.
			continue            // Continue because progress notifications do not carry the final JSON-RPC response payload.
		}
		if envelope["id"] == float64(1) { // Detect the final JSON-RPC response correlated to the single request id used by this integration test.
			responseSeen = true // Record that the production stdio transport emitted the final waited executePlan response.
		}
	}

	if !progressSeen { // Fail when the waited stdio executePlan path emitted no MCP progress notifications at all.
		t.Fatalf("expected stdio transport to emit at least one progress notification, got output %q", stdout.String()) // Surface the raw output so notification regressions are easy to diagnose.
	}
	if !responseSeen { // Fail when the stdio transport never emitted the final response line for the waited executePlan request.
		t.Fatalf("expected stdio transport to emit the final response line, got output %q", stdout.String()) // Surface the raw output so response regressions are easy to diagnose.
	}
}
