package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/devicefarm"
	internalerrors "mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/telemetry"
	"mcp_for_appium/internal/worker"
)

// MCPHandler handles MCP protocol methods
type MCPHandler struct {
	registry         *ToolRegistry
	orch             *orchestrator.Service
	adbShellDisabled bool
}

const (
	defaultExecutePlanWaitTimeout = 60 * time.Second       // defaultExecutePlanWaitTimeout bounds MCP synchronous executePlan waits when the caller does not supply one explicit timeout.
	executePlanPollInterval       = 100 * time.Millisecond // executePlanPollInterval defines how often the MCP wait loop re-reads the trace while waiting for terminal completion.
)

// NewMCPHandler creates a new MCP handler
func NewMCPHandler(orch *orchestrator.Service) *MCPHandler {
	return NewMCPHandlerWithConfig(orch, config.GatewayConfig{}) // Construct the default handler with the repository's normal tool surface when no gateway config is supplied.
}

// NewMCPHandlerWithConfig creates one MCP handler whose tool registry reflects the operator-level gateway tool disablement flags.
func NewMCPHandlerWithConfig(orch *orchestrator.Service, gatewayCfg config.GatewayConfig) *MCPHandler {
	registry := NewToolRegistry()       // Load the full embedded MCP tool registry before applying operator-level disablement flags.
	if gatewayCfg.DisableADBShellTool { // Remove adbShell from discovery and validation entirely when the operator disables the tool.
		delete(registry.tools, "adbShell") // Delete the tool definition from the live registry so tools/list and tools/call both stop exposing it.
	}
	return &MCPHandler{
		registry:         registry,                       // Preserve the filtered live registry so discovery and validation stay aligned.
		orch:             orch,                           // Preserve the orchestrator dependency for tool execution paths.
		adbShellDisabled: gatewayCfg.DisableADBShellTool, // Preserve the operator-level disable flag for direct safety checks inside adbShell execution.
	}
}

// Initialize negotiates initialization-based MCP clients while deliberately excluding the stateless 2026-07-28 revision that removed this method.
func (h *MCPHandler) Initialize(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct {
		ProtocolVersion string                 `json:"protocolVersion"`
		Capabilities    map[string]interface{} `json:"capabilities"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}

	if err := json.Unmarshal(params, &p); err != nil { // Decode the legacy handshake fields before choosing one initialization-based protocol revision.
		return nil, &MCPError{
			Code:    -32602,           // Use the standard invalid-params error for a malformed initialize payload.
			Message: "Invalid params", // Keep the legacy error message stable for existing clients.
			Data:    err.Error(),      // Preserve the JSON decoding diagnostic for client troubleshooting.
		}
	}

	protocolVersion := NegotiateLegacyProtocolVersion(p.ProtocolVersion) // Select a supported legacy revision without ever advertising modern stateless semantics through initialize.
	return map[string]interface{}{                                       // Return the initialization result shape required by legacy MCP clients.
		"protocolVersion": protocolVersion,      // Echo the selected initialization-based revision for subsequent legacy requests.
		"capabilities":    ServerCapabilities(), // Advertise the same implemented tool and resource features exposed by modern discovery.
		"serverInfo":      ServerInfo(),         // Identify the server implementation using the shared cross-era metadata source.
	}, nil
}

// ToolsList returns the complete deterministic tool catalog; modern result decoration adds current-protocol result and cache metadata.
func (h *MCPHandler) ToolsList(ctx context.Context, params json.RawMessage) (interface{}, error) {
	tools := h.registry.List() // Read the deterministic registry snapshot used by both legacy and modern clients.

	return map[string]interface{}{ // Return the legacy-compatible base shape before era-specific result decoration runs.
		"tools": tools, // Expose every enabled tool in deterministic name order.
	}, nil
}

// ToolsCall handles the tools/call request
func (h *MCPHandler) ToolsCall(ctx context.Context, params json.RawMessage) (interface{}, error) {
	logger := telemetry.WithContext(ctx) // Build per-request logger with trace metadata when available.
	startedAt := time.Now()              // Capture tool call start for duration logging.

	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}

	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid params",
			Data:    err.Error(),
		}
	}
	logger.Info("mcp tools/call begin", "tool", p.Name) // Log tool invocation entry before validation and execution.

	// Validate tool exists and arguments
	if err := h.registry.Validate(p.Name, p.Arguments); err != nil {
		logger.Error("mcp tools/call validate failed", "tool", p.Name, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log schema/validation failures as explicit step failures.
		if mcpErr, ok := err.(*MCPError); ok && mcpErr.Code == -32602 {                                                                   // Return caller-correctable tool input failures through the MCP tool-result contract.
			return newToolErrorResult(p.Name, err), nil // Mark invalid arguments with isError instead of turning them into a JSON-RPC protocol failure.
		}
		return nil, err // Preserve unknown-tool and internal registry defects as protocol-level JSON-RPC errors.
	}

	// Route to the appropriate business method
	result, err := h.executeTool(ctx, p.Name, p.Arguments)
	if err != nil {
		logger.Error("mcp tools/call execute failed", "tool", p.Name, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log tool execution failures with elapsed time.
		return newToolErrorResult(p.Name, err), nil                                                                                      // Return business and downstream failures as normal MCP tool results marked with isError.
	}

	// Wrap result in MCP format
	response := map[string]interface{}{ // Build MCP success payload after tool execution completes.
		"content": []map[string]interface{}{
			{
				"type": "text",
				"text": formatToolResult(p.Name, result),
			},
		},
		"structuredContent": result, // Preserve the native tool result as arbitrary JSON for clients that prefer structured outputs.
		"isError":           false,  // Mark the completed tool execution as successful for both modern and legacy clients.
	}
	logger.Info("mcp tools/call done", "tool", p.Name, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful tool completion with elapsed time.
	return response, nil                                                                                    // Return standard MCP response and nil error.
}

// internalCodePattern matches internal error markers like [E.CONFIG.MISSING] inside wrapped error strings.
var internalCodePattern = regexp.MustCompile(`\[(E\.[A-Z0-9._]+)\]`) // Compile once to avoid per-request regex allocations.

// newToolErrorResult converts one input, business, or downstream failure into the normal MCP tools/call result shape without exposing wrapped backend details.
func newToolErrorResult(toolName string, err error) map[string]interface{} {
	publicMessage := "Tool execution failed" // Start with a stable diagnostic for untyped failures whose text may contain sensitive backend data.
	rawDiagnostic := err.Error()             // Retain the full server-side diagnostic only for code extraction and predefined hint selection.
	if mcpErr, ok := err.(*MCPError); ok {   // Reuse protocol handler classifications when a tool path already supplied one.
		publicMessage = mcpErr.Message                       // Expose the concise MCP message while keeping its potentially sensitive data private.
		rawDiagnostic = stringifyErrorData(mcpErr.Data, err) // Use detailed data only to derive a stable internal code and remediation hint.
	}
	internalCode := extractInternalCode(rawDiagnostic) // Recover an embedded repository code from wrapped MCP diagnostics when present.
	if code, ok := internalerrors.CodeOf(err); ok {    // Prefer a directly typed repository code over text extraction when available.
		internalCode = string(code) // Preserve the stable machine-readable classification without returning the wrapped cause.
	}
	hint := toolFailureHint(toolName, rawDiagnostic) // Select a predefined actionable hint from the private diagnostic.
	structuredContent := map[string]interface{}{     // Build a machine-readable failure payload containing only sanitized fields.
		"interface":    "tools/call",  // Identify the protocol interface that completed with an application-level failure.
		"tool":         toolName,      // Identify the selected tool without reflecting any argument values.
		"message":      publicMessage, // Return the concise public classification supplied by the tool layer.
		"internalCode": internalCode,  // Return the stable internal code when one could be determined.
		"hint":         hint,          // Return a safe remediation hint that does not quote the backend error.
	}
	return map[string]interface{}{ // Return a successful JSON-RPC result whose MCP-level outcome is explicitly an error.
		"content": []map[string]interface{}{ // Provide a text fallback for clients that do not consume structuredContent.
			{
				"type": "text",                                                // Identify the fallback content block as plain text.
				"text": fmt.Sprintf("%s failed: %s", toolName, publicMessage), // Summarize the tool failure without including raw downstream data.
			},
		},
		"structuredContent": structuredContent, // Preserve the sanitized machine-readable failure details for modern clients.
		"isError":           true,              // Mark the normal tools/call result as a failed tool execution per the MCP contract.
	}
}

// stringifyErrorData executes this operation.
func stringifyErrorData(data interface{}, fallback error) string {
	if s, ok := data.(string); ok && strings.TrimSpace(s) != "" { // Prefer explicit string data set by existing handlers.
		return s // Keep exact handler-provided message to avoid losing details.
	}
	if data != nil { // Serialize non-string payloads for backward-compatible visibility.
		if raw, err := json.Marshal(data); err == nil { // Best-effort JSON serialization for arbitrary data payloads.
			return string(raw) // Return serialized payload as diagnostic raw text.
		}
	}
	return fallback.Error() // Fallback to the wrapped error text when no data payload exists.
}

// extractInternalCode executes this operation.
func extractInternalCode(raw string) string {
	matches := internalCodePattern.FindAllStringSubmatch(raw, -1) // Collect all embedded internal codes from wrapped error chains.
	if len(matches) == 0 {                                        // Return empty code when source text has no standardized marker.
		return ""
	}
	last := matches[len(matches)-1] // Use the innermost/root cause code from the wrapped chain.
	if len(last) == 2 {             // Ensure capture group exists before indexing.
		return last[1] // Return root internal code value like E.CONFIG.INVALID.
	}
	return "" // Fall back to empty when regex capture format is unexpected.
}

// toolFailureHint executes this operation.
func toolFailureHint(toolName string, raw string) string {
	lower := strings.ToLower(raw) // Normalize for substring matching across mixed-case provider errors.
	switch {                      // Match common environment/provider failures first.
	case strings.Contains(lower, "could not resolve host"), strings.Contains(lower, "no such host"):
		return "Check DNS/network egress from the server runtime."
	case strings.Contains(lower, "connection refused"):
		return "Target service is not listening; verify host/port and process health."
	case strings.Contains(lower, "role \"postgres\" does not exist"):
		return "Update storage.postgres.dsn or create the postgres role locally."
	case strings.Contains(lower, "appium_url is empty"):
		return "Set worker.appium_url in config.yaml."
	case strings.Contains(lower, "non-standard capabilities should have a vendor prefix"):
		return "Use W3C caps with appium: prefixes (for example appium:udid, appium:automationName)."
	case strings.Contains(lower, "devicefarm mode is not run_api"):
		return "Set devicefarm.mode=run_api before calling Device Farm tools."
	case strings.Contains(lower, "devicefarm client not initialized"):
		return "Verify aws.region/profile credentials and Device Farm config."
	case strings.Contains(lower, "exec: \"adb\": executable file not found"):
		return "Install Android platform-tools and ensure adb is in PATH."
	}
	return fmt.Sprintf("Inspect server logs for %s and fix the upstream dependency or configuration.", toolName) // Provide a deterministic fallback without promising raw backend details in the client response.
}

// executeTool routes tool calls to business logic
func (h *MCPHandler) executeTool(ctx context.Context, toolName string, arguments json.RawMessage) (interface{}, error) {
	switch toolName {
	case "startSession":
		return h.handleStartSession(ctx, arguments)
	case "executePlan":
		return h.handleExecutePlan(ctx, arguments)
	case "endSession":
		return h.handleEndSession(ctx, arguments)
	case "getSemanticSnapshot":
		return h.handleGetSemanticSnapshot(ctx, arguments)
	case "takeScreenshot":
		return h.handleTakeScreenshot(ctx, arguments)
	case "cancelPlan":
		return h.handleCancelPlan(ctx, arguments)
	case "getTrace":
		return h.handleGetTrace(ctx, arguments)
	case "healthCheck":
		return h.handleHealthCheck(ctx, arguments)
	// Interactive element operations
	case "findElement":
		return h.handleFindElement(ctx, arguments)
	case "clickElement":
		return h.handleClickElement(ctx, arguments)
	case "sendKeysToElement":
		return h.handleSendKeysToElement(ctx, arguments)
	case "clearElement":
		return h.handleClearElement(ctx, arguments)
	case "getElementText":
		return h.handleGetElementText(ctx, arguments)
	case "getElementAttribute":
		return h.handleGetElementAttribute(ctx, arguments)
	case "isElementDisplayed":
		return h.handleIsElementDisplayed(ctx, arguments)
	case "tap":
		return h.handleTap(ctx, arguments)
	case "swipe":
		return h.handleSwipe(ctx, arguments)
	case "longPress":
		return h.handleLongPress(ctx, arguments)
	case "pressBack":
		return h.handlePressBack(ctx, arguments)
	case "hideKeyboard":
		return h.handleHideKeyboard(ctx, arguments)
	case "scheduleDeviceFarmRun":
		return h.handleScheduleDeviceFarmRun(ctx, arguments)
	case "createDeviceFarmUpload":
		return h.handleCreateDeviceFarmUpload(ctx, arguments)
	case "getDeviceFarmUpload":
		return h.handleGetDeviceFarmUpload(ctx, arguments)
	case "getDeviceFarmRuntimeContext":
		return h.handleGetDeviceFarmRuntimeContext(ctx, arguments)
	case "getDeviceFarmRun":
		return h.handleGetDeviceFarmRun(ctx, arguments)
	case "adbShell":
		return h.handleAdbShell(ctx, arguments)
	default:
		return nil, &MCPError{
			Code:    -32601,
			Message: "Tool not found: " + toolName,
		}
	}
}

// handleScheduleDeviceFarmRun executes this operation.
func (h *MCPHandler) handleScheduleDeviceFarmRun(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		RunName        string `json:"runName"`
		ProjectARN     string `json:"projectArn"`
		AppARN         string `json:"appArn"`
		DevicePoolARN  string `json:"devicePoolArn"`
		TestType       string `json:"testType"`
		TestPackageARN string `json:"testPackageArn"`
		TestSpecARN    string `json:"testSpecArn"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}

	result, err := h.orch.ScheduleDeviceFarmRun(ctx, devicefarm.ScheduleRunRequest{
		RunName:        args.RunName,
		ProjectARN:     args.ProjectARN,
		AppARN:         args.AppARN,
		DevicePoolARN:  args.DevicePoolARN,
		TestType:       args.TestType,
		TestPackageARN: args.TestPackageARN,
		TestSpecARN:    args.TestSpecARN,
	})
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to schedule device farm run", Data: err.Error()}
	}
	return result, nil
}

// handleCreateDeviceFarmUpload executes this operation.
func (h *MCPHandler) handleCreateDeviceFarmUpload(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		ProjectARN  string `json:"projectArn"`
		Name        string `json:"name"`
		Type        string `json:"type"`
		ContentType string `json:"contentType"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}

	result, err := h.orch.CreateDeviceFarmUpload(ctx, devicefarm.CreateUploadRequest{
		ProjectARN:  args.ProjectARN,
		Name:        args.Name,
		Type:        args.Type,
		ContentType: args.ContentType,
	})
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to create device farm upload", Data: err.Error()}
	}
	return result, nil
}

// handleGetDeviceFarmUpload executes this operation.
func (h *MCPHandler) handleGetDeviceFarmUpload(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		UploadARN string `json:"uploadArn"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}

	result, err := h.orch.GetDeviceFarmUpload(ctx, args.UploadARN)
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to get device farm upload", Data: err.Error()}
	}
	return result, nil
}

// handleGetDeviceFarmRuntimeContext executes this operation.
func (h *MCPHandler) handleGetDeviceFarmRuntimeContext(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		ProjectARN string `json:"projectArn"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}

	result, err := h.orch.GetDeviceFarmRuntimeContext(ctx, args.ProjectARN)
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to get device farm runtime context", Data: err.Error()}
	}
	return result, nil
}

// handleGetDeviceFarmRun executes this operation.
func (h *MCPHandler) handleGetDeviceFarmRun(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		RunARN string `json:"runArn"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}

	result, err := h.orch.GetDeviceFarmRun(ctx, args.RunARN)
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to get device farm run", Data: err.Error()}
	}
	return result, nil
}

// handleAdbShell executes this operation.
func (h *MCPHandler) handleAdbShell(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	if h.adbShellDisabled { // Reject direct adbShell execution when the operator-level disable flag has removed the tool from the public registry.
		return nil, &MCPError{Code: -32601, Message: "Tool not found: adbShell"} // Return the standard tool-not-found protocol error so callers observe the same behavior as an absent tool definition.
	}
	var args struct {
		DeviceSerial string   `json:"deviceSerial"`
		Command      []string `json:"command"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}

	result, err := h.orch.AdbShell(ctx, args.DeviceSerial, args.Command)
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to execute adb shell command", Data: err.Error()}
	}
	return result, nil
}

// Business method handlers

// handleStartSession executes this operation.
func (h *MCPHandler) handleStartSession(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		ProjectID   string                 `json:"projectId"`
		ActorID     string                 `json:"actorId"`
		Labels      map[string]string      `json:"labels"`
		W3cCapsJson map[string]interface{} `json:"w3cCapsJson"`
	}

	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid arguments",
			Data:    err.Error(),
		}
	}

	// Call orchestrator
	sess, err := h.orch.StartSession(ctx, args.ProjectID, args.W3cCapsJson)
	if err != nil {
		return nil, &MCPError{
			Code:    -32000,
			Message: "Failed to start session",
			Data:    err.Error(),
		}
	}

	return map[string]interface{}{
		"sessionId":    sess.ID,
		"capabilities": sess.Capabilities,
		"status":       "created",
	}, nil
}

// handleExecutePlan executes this operation.
func (h *MCPHandler) handleExecutePlan(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID         string          `json:"sessionId"`
		Plan              json.RawMessage `json:"plan"`
		TraceID           string          `json:"traceId"`
		WaitForCompletion *bool           `json:"waitForCompletion"`
		WaitTimeoutMs     int             `json:"waitTimeoutMs"`
	}

	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid arguments",
			Data:    err.Error(),
		}
	}

	// Call orchestrator
	traceID, err := h.orch.ExecutePlanWithTrace(ctx, args.SessionID, args.TraceID, args.Plan)
	if err != nil {
		return nil, &MCPError{
			Code:    -32000,
			Message: "Failed to execute plan",
			Data:    err.Error(),
		}
	}

	waitForCompletion := true          // Default MCP executePlan to synchronous waiting so stdio clients can receive one terminal result when feasible.
	if args.WaitForCompletion != nil { // Respect the caller's explicit wait preference when one was supplied in the tool arguments.
		waitForCompletion = *args.WaitForCompletion // Override the default behavior with the caller-provided value.
	}
	if !waitForCompletion { // Preserve the legacy asynchronous submit path when the caller opts out of synchronous waiting explicitly.
		return map[string]interface{}{ // Return the accepted trace identifier immediately so polling clients can continue to use getTrace manually.
			"traceId":             traceID,
			"status":              "running",
			"waitedForCompletion": false,
			"pollingRequired":     true,
		}, nil
	}

	totalSteps := int64(0)                                               // Default the total step count to zero so progress notifications can omit totals when parsing fails here.
	if steps, parseErr := worker.ParsePlan(args.Plan); parseErr == nil { // Parse the submitted plan best-effort so progress notifications can expose a useful total without changing scheduling semantics.
		totalSteps = int64(len(steps)) // Store the parsed step count so the wait loop can emit stable progress totals and return them in the final payload.
	}

	waitTimeout := defaultExecutePlanWaitTimeout // Start from the default wait timeout so MCP callers receive bounded synchronous behavior even without one explicit timeout.
	if args.WaitTimeoutMs > 0 {                  // Respect the caller-supplied timeout when one positive millisecond value was provided.
		waitTimeout = time.Duration(args.WaitTimeoutMs) * time.Millisecond // Convert the caller-provided millisecond timeout into one duration used by the wait loop.
	}

	waitCtx := ctx              // Start from the request context so authentication metadata and cancellation propagate into the wait loop.
	cancelWait := func() {}     // Default to one no-op cancel so the defer below remains unconditional even when no derived timeout context is created.
	if args.WaitTimeoutMs > 0 { // Respect an explicit per-call wait timeout even when the parent request context already carries its own deadline.
		waitCtx, cancelWait = context.WithTimeout(ctx, waitTimeout) // Derive one bounded wait context so the caller-supplied timeout participates in the wait loop.
	} else if _, hasDeadline := ctx.Deadline(); !hasDeadline { // Add one default local timeout only when the caller context itself does not already impose one deadline.
		waitCtx, cancelWait = context.WithTimeout(ctx, waitTimeout) // Derive one bounded wait context so stdio tools/call does not block forever on long-running traces by default.
	}
	defer cancelWait() // Release the derived timeout timer promptly once the wait loop exits.

	_ = ReportProgress(ctx, 0, totalSteps, "trace accepted") // Emit one initial progress notification best-effort so MCP clients can render immediate feedback before polling begins.

	result, err := h.waitForTraceCompletion(waitCtx, ctx, traceID, totalSteps) // Block until the trace becomes terminal or the optional local wait timeout elapses.
	if err != nil {                                                            // Map wait-loop failures into one structured MCP tool error so clients still receive tool-level context.
		return nil, &MCPError{ // Preserve the underlying wait-loop error text inside the MCP error payload for diagnosis.
			Code:    -32000,
			Message: "Failed to wait for trace completion",
			Data:    err.Error(),
		}
	}

	return result, nil // Return the terminal or timed-out trace snapshot after the synchronous wait path completes.
}

// waitForTraceCompletion polls one trace until it reaches a terminal state or the local MCP wait timeout elapses.
func (h *MCPHandler) waitForTraceCompletion(waitCtx context.Context, fallbackCtx context.Context, traceID string, totalSteps int64) (map[string]interface{}, error) {
	ticker := time.NewTicker(executePlanPollInterval) // Poll the live trace on one fixed interval so short plans complete promptly without busy waiting.
	defer ticker.Stop()                               // Release ticker resources promptly once the wait loop exits for any reason.

	lastStatus := ""          // Track the last emitted trace status so duplicate progress notifications are avoided when nothing meaningful changes.
	lastComplete := int64(-1) // Track the last emitted completed-step count so duplicate progress notifications are avoided during polling.

	for { // Continue polling until the trace becomes terminal or one cancellation/timeout path ends the wait loop.
		trace, events, err := h.orch.GetTrace(waitCtx, traceID) // Read the current trace snapshot and replayable events through the production orchestrator API.
		if err != nil {                                         // Decide whether the read failure came from wait cancellation or from one actual service-layer error.
			if waitCtx.Err() != nil { // Break cleanly when the derived wait context has ended because the caller cancelled or the local timeout expired.
				break // Exit the polling loop so timeout/cancellation handling below can choose between one partial result and one hard error.
			}
			return nil, err // Surface non-timeout trace read failures directly because they indicate one actual orchestrator or storage problem.
		}

		completedSteps := countCompletedPlanSteps(events)                 // Count unique terminal step events so progress notifications reflect actual plan-step completion.
		if completedSteps != lastComplete || trace.Status != lastStatus { // Emit progress only when either step completion or trace status changed since the previous poll.
			_ = ReportProgress(waitCtx, completedSteps, totalSteps, "trace "+trace.Status) // Emit one best-effort progress notification so stdio MCP clients receive real-time updates.
			lastComplete = completedSteps                                                  // Record the emitted completed-step count so duplicate notifications are suppressed on later polls.
			lastStatus = trace.Status                                                      // Record the emitted trace status so duplicate notifications are suppressed on later polls.
		}
		if isTerminalTraceStatus(trace.Status) { // Stop waiting once the trace has reached one terminal lifecycle state in the authoritative store.
			return buildExecutePlanWaitResult(traceID, trace, events, totalSteps, completedSteps, false), nil // Return the terminal trace snapshot and replayable events to the MCP client.
		}

		select {
		case <-waitCtx.Done(): // Stop waiting promptly when the caller cancels or the local wait timeout expires.
			break // Exit the polling loop so timeout/cancellation handling below can decide whether to return one partial result or one hard error.
		case <-ticker.C: // Wait for the next polling interval before re-reading the trace state.
			continue // Continue the polling loop on the next interval so terminal state changes are observed promptly.
		}
		break // Exit the polling loop after the wait context ended.
	}

	if waitCtx.Err() != nil && fallbackCtx.Err() == nil { // Return one partial result only when the local wait timeout elapsed while the parent request context is still alive.
		trace, events, err := h.orch.GetTrace(fallbackCtx, traceID) // Re-read the current trace snapshot through the still-live parent context so timeout responses include the freshest persisted state.
		if err != nil {                                             // Fall back to one minimal timeout payload when the trace can no longer be read even through the parent request context.
			return map[string]interface{}{ // Return the accepted trace id and timeout marker so the client still knows which trace to inspect later.
				"traceId":             traceID,
				"status":              "running",
				"waitedForCompletion": true,
				"waitTimedOut":        true,
				"pollingRequired":     true,
			}, nil
		}
		return buildExecutePlanWaitResult(traceID, trace, events, totalSteps, countCompletedPlanSteps(events), true), nil // Return one partial timeout snapshot that still includes the current trace state and replayable events.
	}

	return nil, waitCtx.Err() // Surface caller-driven cancellation or parent-deadline expiry as one hard error because the request itself is no longer valid to complete.
}

// buildExecutePlanWaitResult assembles one consistent MCP executePlan wait payload for terminal and timed-out traces alike.
func buildExecutePlanWaitResult(traceID string, trace *postgres.Trace, events []*postgres.PlanEvent, totalSteps int64, completedSteps int64, waitTimedOut bool) map[string]interface{} {
	return map[string]interface{}{ // Return one stable result shape so clients can rely on the same fields for both terminal and timed-out wait outcomes.
		"traceId":             traceID,
		"status":              trace.Status,
		"waitedForCompletion": true,
		"waitTimedOut":        waitTimedOut,
		"pollingRequired":     waitTimedOut,
		"trace":               trace,
		"events":              events,
		"completedStepCount":  completedSteps,
		"totalStepCount":      totalSteps,
	}
}

// countCompletedPlanSteps counts unique plan steps that already emitted one terminal step-level event.
func countCompletedPlanSteps(events []*postgres.PlanEvent) int64 {
	completed := make(map[int]struct{}) // Track unique step indexes so retries or duplicate callbacks do not over-count one logical plan step.
	for _, event := range events {      // Walk every replayable event in order so completed step indexes can be accumulated deterministically.
		if event == nil || event.StepIndex < 0 { // Ignore nil and trace-level events because only non-negative step indexes represent concrete plan steps.
			continue // Skip events that cannot contribute to plan-step completion progress.
		}
		if event.Status != "passed" && event.Status != "failed" { // Count only terminal step-level states because running events do not represent completed progress yet.
			continue // Skip non-terminal step events so progress reflects actual completed steps only.
		}
		completed[event.StepIndex] = struct{}{} // Record the step index so later duplicate events for the same step do not inflate progress.
	}
	return int64(len(completed)) // Return the number of unique completed step indexes as the current progress value.
}

// isTerminalTraceStatus reports whether one persisted trace status is terminal from the MCP client's perspective.
func isTerminalTraceStatus(status string) bool {
	switch status { // Match the trace terminal states that should stop the MCP synchronous wait loop immediately.
	case "completed", "failed", "cancelled":
		return true // Report terminal status so the wait loop can stop polling and return the final trace snapshot.
	default:
		return false // Keep polling for any non-terminal trace status.
	}
}

// handleEndSession executes this operation.
func (h *MCPHandler) handleEndSession(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
	}

	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid arguments",
			Data:    err.Error(),
		}
	}

	if err := h.orch.EndSession(ctx, args.SessionID); err != nil {
		return nil, &MCPError{
			Code:    -32000,
			Message: "Failed to end session",
			Data:    err.Error(),
		}
	}

	return map[string]interface{}{
		"success": true,
	}, nil
}

// handleGetSemanticSnapshot executes this operation.
func (h *MCPHandler) handleGetSemanticSnapshot(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		SinceRev  string `json:"sinceRev"`
	}

	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid arguments",
			Data:    err.Error(),
		}
	}

	result, err := h.orch.GetSemanticSnapshot(ctx, args.SessionID, args.SinceRev)
	if err != nil {
		return nil, &MCPError{
			Code:    -32000,
			Message: "Failed to get semantic snapshot",
			Data:    err.Error(),
		}
	}
	return result, nil
}

// handleTakeScreenshot executes this operation.
func (h *MCPHandler) handleTakeScreenshot(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID    string `json:"sessionId"`
		TraceID      string `json:"traceId"`
		IncludeThumb bool   `json:"includeThumb"`
	}

	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid arguments",
			Data:    err.Error(),
		}
	}

	result, err := h.orch.TakeScreenshot(ctx, args.SessionID, args.TraceID, args.IncludeThumb)
	if err != nil {
		return nil, &MCPError{
			Code:    -32000,
			Message: "Failed to take screenshot",
			Data:    err.Error(),
		}
	}
	return result, nil
}

// handleCancelPlan executes this operation.
func (h *MCPHandler) handleCancelPlan(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		TraceID string `json:"traceId"`
	}

	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid arguments",
			Data:    err.Error(),
		}
	}

	if err := h.orch.CancelPlan(ctx, args.TraceID); err != nil {
		return nil, &MCPError{
			Code:    -32000,
			Message: "Failed to cancel plan",
			Data:    err.Error(),
		}
	}
	return map[string]interface{}{
		"success": true,
	}, nil
}

// handleGetTrace executes this operation.
func (h *MCPHandler) handleGetTrace(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		TraceID string `json:"traceId"`
	}

	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid arguments",
			Data:    err.Error(),
		}
	}

	trace, events, err := h.orch.GetTrace(ctx, args.TraceID)
	if err != nil {
		return nil, &MCPError{
			Code:    -32000,
			Message: "Failed to get trace",
			Data:    err.Error(),
		}
	}
	return map[string]interface{}{
		"trace":  trace,
		"events": events,
	}, nil
}

// handleHealthCheck executes this operation.
func (h *MCPHandler) handleHealthCheck(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	return h.orch.HealthCheck(ctx), nil
}

// ResourcesList returns the historical template-like resource catalog retained for initialization-era client compatibility.
func (h *MCPHandler) ResourcesList(_ context.Context, _ json.RawMessage) (interface{}, error) {
	resources := make([]map[string]interface{}, 0, 3)    // Allocate the fixed legacy catalog while preserving JSON array output when templates are transformed.
	for _, resource := range appiumResourceTemplates() { // Convert each standard resource template into the non-standard shape returned by earlier repository versions.
		resource["uri"] = resource["uriTemplate"] // Preserve the historical uri field so existing initialization-era clients do not lose catalog entries.
		delete(resource, "uriTemplate")           // Remove the modern template field because legacy consumers expect the old resource object shape.
		resources = append(resources, resource)   // Append the transformed request-local map without mutating any shared catalog state.
	}
	return map[string]interface{}{ // Return the legacy-compatible base shape without modern cache or result metadata.
		"resources": resources, // Expose each historical parameterized Appium resource under its old field name.
	}, nil
}

// ModernResourcesList returns the concrete resources currently enumerable for a modern caller, which is empty because this server resolves only ID-parameterized resources.
func (h *MCPHandler) ModernResourcesList(_ context.Context, _ json.RawMessage) (interface{}, error) {
	resources := make([]map[string]interface{}, 0) // Allocate an explicit empty array because no trace or session IDs can be enumerated without caller-supplied identifiers.
	return map[string]interface{}{                 // Return the standard resources/list base shape before modern cache and result decoration.
		"resources": resources, // Report the empty concrete resource set while directing clients to resources/templates/list for URI patterns.
	}, nil
}

// ResourcesTemplatesList returns the standard parameterized Appium resource templates used to construct artifacts, traces, and session URIs.
func (h *MCPHandler) ResourcesTemplatesList(_ context.Context, _ json.RawMessage) (interface{}, error) {
	return map[string]interface{}{ // Return the standard template-list base shape before era-specific result decoration.
		"resourceTemplates": appiumResourceTemplates(), // Expose all supported URI templates under the protocol-defined result field.
	}, nil
}

// appiumResourceTemplates returns a new static catalog of parameterized resource definitions so callers may transform entries without shared mutation.
func appiumResourceTemplates() []map[string]interface{} {
	return []map[string]interface{}{ // Allocate request-local maps because legacy compatibility rewrites the URI field in place.
		{
			"uriTemplate": "mcp://appium/artifacts/{traceId}", // Define the trace identifier placeholder using the MCP resource-template field.
			"name":        "Test Artifacts",                   // Provide a stable programmatic display name for artifact data.
			"description": "获取测试执行产生的工件（截图、日志等）",              // Explain that this template resolves screenshots, logs, and related run artifacts.
			"mimeType":    "application/json",                 // Declare that resolved artifact content is serialized JSON text.
		},
		{
			"uriTemplate": "mcp://appium/traces/{traceId}", // Define the trace identifier placeholder for detailed execution history.
			"name":        "Execution Trace",               // Provide a stable programmatic display name for trace data.
			"description": "获取测试执行的详细追踪记录",                 // Explain that this template resolves detailed test execution traces.
			"mimeType":    "application/json",              // Declare that resolved trace content is serialized JSON text.
		},
		{
			"uriTemplate": "mcp://appium/sessions/{sessionId}", // Define the Appium session identifier placeholder for session metadata.
			"name":        "Session Info",                      // Provide a stable programmatic display name for session data.
			"description": "获取会话详细信息",                          // Explain that this template resolves detailed Appium session information.
			"mimeType":    "application/json",                  // Declare that resolved session content is serialized JSON text.
		},
	}
}

// ResourcesRead resolves one Appium resource URI and returns JSON text whose modern decoration uses a short private cache lifetime.
func (h *MCPHandler) ResourcesRead(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct {
		URI string `json:"uri"`
	}

	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid params",
			Data:    err.Error(),
		}
	}

	// Parse URI: mcp://appium/{resource}/{id}
	resource, id, err := parseResourceURI(p.URI)
	if err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid resource URI",
			Data:    err.Error(),
		}
	}

	var content interface{}
	switch resource {
	case "artifacts":
		artifacts, err := h.orch.GetArtifacts(ctx, id)
		if err != nil {
			return nil, newResourceReadError(p.URI, "get artifacts", err) // Map missing and internal artifact failures to current resource error semantics.
		}
		content = map[string]interface{}{"traceId": id, "artifacts": artifacts}

	case "traces":
		trace, events, err := h.orch.GetTrace(ctx, id)
		if err != nil {
			return nil, newResourceReadError(p.URI, "get trace", err) // Map missing and internal trace failures to current resource error semantics.
		}
		content = map[string]interface{}{"trace": trace, "events": events}

	case "sessions":
		sess, err := h.orch.GetSession(ctx, id)
		if err != nil {
			return nil, newResourceReadError(p.URI, "get session", err) // Map missing and internal session failures to current resource error semantics.
		}
		var caps interface{}
		if sess.Capabilities != nil {
			_ = json.Unmarshal(sess.Capabilities, &caps)
		}
		content = map[string]interface{}{
			"sessionId":    sess.ID,
			"projectId":    sess.ProjectID,
			"status":       sess.Status,
			"capabilities": caps,
			"createdAt":    sess.CreatedAt,
		}

	default:
		return nil, &MCPError{
			Code:    -32602,
			Message: "Unknown resource type: " + resource,
		}
	}

	text, _ := json.MarshalIndent(content, "", "  ")
	return newResourceReadResult(p.URI, string(text)), nil // Build the legacy-compatible resource payload before modern private cache metadata is attached.
}

// newResourceReadError maps unavailable or unauthorized resources to Invalid Params and all other backend failures to the standard JSON-RPC internal error.
func newResourceReadError(uri string, operation string, err error) *MCPError {
	if internalerrors.IsCode(err, internalerrors.CodeTraceNotFound) || internalerrors.IsCode(err, internalerrors.CodeSessionNotFound) || internalerrors.IsCode(err, internalerrors.CodePermissionDenied) { // Treat unavailable and unauthorized caller-scoped resources identically to avoid disclosing their existence.
		return &MCPError{ // Return the exact current-protocol resource-not-found error family.
			Code:    -32602,               // Use Invalid Params because MCP 2026-07-28 retired the legacy resource-not-found server code.
			Message: "Resource not found", // Give clients the canonical resource availability diagnostic.
			Data: map[string]interface{}{ // Include only the requested identifier so clients can correct or discard the stale URI.
				"uri": uri, // Echo the unavailable resource URI without exposing backend or authorization details.
			},
		}
	}
	return &MCPError{ // Return a standard internal failure for storage, decoding, and other server-side resource errors.
		Code:    -32603,                    // Use JSON-RPC Internal error instead of allocating a forbidden legacy MCP server code.
		Message: "Failed to read resource", // Identify the failed protocol operation without incorrectly reporting absence.
		Data: map[string]interface{}{ // Preserve stable request context while keeping backend diagnostics in server logs only.
			"uri":       uri,       // Identify the resource whose backend read failed.
			"operation": operation, // Identify the internal read stage that returned the error.
		},
	}
}

// newResourceReadResult builds one legacy-compatible JSON text resource response for subsequent era-specific result decoration.
func newResourceReadResult(uri string, text string) map[string]interface{} {
	return map[string]interface{}{ // Return one resource payload without adding fields unknown to initialization-era clients.
		"contents": []map[string]interface{}{ // Provide the standard MCP resource contents array even though one URI resolves to one document.
			{
				"uri":      uri,                // Echo the exact resource identifier that produced this content.
				"mimeType": "application/json", // Identify the text payload as a serialized JSON document.
				"text":     text,               // Return the preformatted JSON document as MCP text resource content.
			},
		},
	}
}

// parseResourceURI parses mcp://appium/{resource}/{id} into resource type and id
func parseResourceURI(uri string) (string, string, error) {
	const prefix = "mcp://appium/"
	if !strings.HasPrefix(uri, prefix) {
		return "", "", &MCPError{Code: -32602, Message: "URI must start with mcp://appium/"}
	}

	path := strings.TrimPrefix(uri, prefix)
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", &MCPError{Code: -32602, Message: "URI must be mcp://appium/{resource}/{id}"}
	}

	return parts[0], parts[1], nil
}

// formatToolResult formats the tool execution result as a human-readable string
func formatToolResult(toolName string, result interface{}) string {
	data, _ := json.MarshalIndent(result, "", "  ")
	return string(data)
}

// Interactive element operation handlers

// handleFindElement executes this operation.
func (h *MCPHandler) handleFindElement(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		Strategy  string `json:"strategy"`
		Selector  string `json:"selector"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	elementID, err := h.orch.FindElement(ctx, args.SessionID, args.Strategy, args.Selector)
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to find element", Data: err.Error()}
	}
	return map[string]interface{}{"elementId": elementID}, nil
}

// handleClickElement executes this operation.
func (h *MCPHandler) handleClickElement(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		ElementID string `json:"elementId"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	if err := h.orch.ClickElement(ctx, args.SessionID, args.ElementID); err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to click element", Data: err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}

// handleSendKeysToElement executes this operation.
func (h *MCPHandler) handleSendKeysToElement(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		ElementID string `json:"elementId"`
		Text      string `json:"text"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	if err := h.orch.SendKeysToElement(ctx, args.SessionID, args.ElementID, args.Text); err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to send keys", Data: err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}

// handleClearElement executes this operation.
func (h *MCPHandler) handleClearElement(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		ElementID string `json:"elementId"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	if err := h.orch.ClearElement(ctx, args.SessionID, args.ElementID); err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to clear element", Data: err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}

// handleGetElementText executes this operation.
func (h *MCPHandler) handleGetElementText(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		ElementID string `json:"elementId"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	text, err := h.orch.GetElementText(ctx, args.SessionID, args.ElementID)
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to get element text", Data: err.Error()}
	}
	return map[string]interface{}{"text": text}, nil
}

// handleGetElementAttribute executes this operation.
func (h *MCPHandler) handleGetElementAttribute(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		ElementID string `json:"elementId"`
		Attribute string `json:"attribute"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	value, err := h.orch.GetElementAttribute(ctx, args.SessionID, args.ElementID, args.Attribute)
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to get attribute", Data: err.Error()}
	}
	return map[string]interface{}{"attribute": args.Attribute, "value": value}, nil
}

// handleIsElementDisplayed executes this operation.
func (h *MCPHandler) handleIsElementDisplayed(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		ElementID string `json:"elementId"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	displayed, err := h.orch.IsElementDisplayed(ctx, args.SessionID, args.ElementID)
	if err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to check if element displayed", Data: err.Error()}
	}
	return map[string]interface{}{"displayed": displayed}, nil
}

// handleTap executes this operation.
func (h *MCPHandler) handleTap(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
		X         int    `json:"x"`
		Y         int    `json:"y"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	if err := h.orch.Tap(ctx, args.SessionID, args.X, args.Y); err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to tap", Data: err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}

// handleSwipe executes this operation.
func (h *MCPHandler) handleSwipe(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID  string `json:"sessionId"`
		StartX     int    `json:"startX"`
		StartY     int    `json:"startY"`
		EndX       int    `json:"endX"`
		EndY       int    `json:"endY"`
		DurationMs int    `json:"durationMs"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	if args.DurationMs == 0 {
		args.DurationMs = 200
	}
	if err := h.orch.Swipe(ctx, args.SessionID, args.StartX, args.StartY, args.EndX, args.EndY, args.DurationMs); err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to swipe", Data: err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}

// handleLongPress executes this operation.
func (h *MCPHandler) handleLongPress(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID  string `json:"sessionId"`
		ElementID  string `json:"elementId"`
		DurationMs int    `json:"durationMs"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	if args.DurationMs == 0 {
		args.DurationMs = 1000
	}
	if err := h.orch.LongPress(ctx, args.SessionID, args.ElementID, args.DurationMs); err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to long press", Data: err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}

// handlePressBack executes this operation.
func (h *MCPHandler) handlePressBack(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	if err := h.orch.PressBack(ctx, args.SessionID); err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to press back", Data: err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}

// handleHideKeyboard executes this operation.
func (h *MCPHandler) handleHideKeyboard(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, &MCPError{Code: -32602, Message: "Invalid arguments", Data: err.Error()}
	}
	if err := h.orch.HideKeyboard(ctx, args.SessionID); err != nil {
		return nil, &MCPError{Code: -32000, Message: "Failed to hide keyboard", Data: err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}
