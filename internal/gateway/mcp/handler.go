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

// Initialize handles the MCP initialize request
func (h *MCPHandler) Initialize(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct {
		ProtocolVersion string                 `json:"protocolVersion"`
		Capabilities    map[string]interface{} `json:"capabilities"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}

	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &MCPError{
			Code:    -32602,
			Message: "Invalid params",
			Data:    err.Error(),
		}
	}

	// Return server capabilities
	return map[string]interface{}{
		"protocolVersion": "2024-11-05",
		"capabilities": map[string]interface{}{
			"tools": map[string]interface{}{
				"listChanged": false, // Tools don't change dynamically
			},
			"resources": map[string]interface{}{
				"subscribe":   false, // Advertise no MCP resource subscriptions because this server does not implement resources/subscribe today.
				"listChanged": false,
			},
		},
		"serverInfo": map[string]interface{}{
			"name":    "MCP Mobile Worker",
			"version": "1.0.0",
		},
	}, nil
}

// ToolsList handles the tools/list request
func (h *MCPHandler) ToolsList(ctx context.Context, params json.RawMessage) (interface{}, error) {
	tools := h.registry.List()

	return map[string]interface{}{
		"tools": tools,
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
		return nil, err
	}

	// Route to the appropriate business method
	result, err := h.executeTool(ctx, p.Name, p.Arguments)
	if err != nil {
		logger.Error("mcp tools/call execute failed", "tool", p.Name, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log tool execution failures with elapsed time.
		return nil, normalizeToolError(p.Name, err)                                                                                      // Normalize per-tool errors into structured MCP payloads.
	}

	// Wrap result in MCP format
	response := map[string]interface{}{ // Build MCP success payload after tool execution completes.
		"content": []map[string]interface{}{
			{
				"type": "text",
				"text": formatToolResult(p.Name, result),
			},
		},
		"isError": false,
	}
	logger.Info("mcp tools/call done", "tool", p.Name, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful tool completion with elapsed time.
	return response, nil                                                                                    // Return standard MCP response and nil error.
}

// internalCodePattern matches internal error markers like [E.CONFIG.MISSING] inside wrapped error strings.
var internalCodePattern = regexp.MustCompile(`\[(E\.[A-Z0-9._]+)\]`) // Compile once to avoid per-request regex allocations.

// normalizeToolError executes this operation.
func normalizeToolError(toolName string, err error) error {
	if mcpErr, ok := err.(*MCPError); ok { // Reuse existing MCP error objects produced by handler methods.
		rawError := stringifyErrorData(mcpErr.Data, err) // Preserve the most specific raw error text for users and debugging.
		data := map[string]interface{}{                  // Return structured data so clients can render richer diagnostics.
			"interface":    "tools/call",                        // Identify the failed interface category for consumers.
			"tool":         toolName,                            // Identify the exact failing tool for quick triage.
			"rawError":     rawError,                            // Keep original downstream/native error message if available.
			"internalCode": extractInternalCode(rawError),       // Parse standardized internal code when present.
			"hint":         toolFailureHint(toolName, rawError), // Add actionable hint without hiding source error.
		}
		mcpErr.Data = data // Overwrite flat string data with structured details while keeping top-level MCP code/message.
		return mcpErr      // Return same MCPError instance to preserve code/message semantics.
	}

	// Convert non-MCP errors to MCP server errors with full context.
	return &MCPError{ // Wrap unknown error types so clients still receive consistent diagnostic fields.
		Code:    -32000, // Use server error code for unexpected tool execution failures.
		Message: "Tool execution failed",
		Data: map[string]interface{}{
			"interface":    "tools/call",                           // Mark this as a tool-call execution failure.
			"tool":         toolName,                               // Include tool name for routing/debugging on client side.
			"rawError":     err.Error(),                            // Preserve original error text from lower layers.
			"internalCode": extractInternalCode(err.Error()),       // Extract internal code when wrapped text includes it.
			"hint":         toolFailureHint(toolName, err.Error()), // Provide lightweight next-step guidance.
		},
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
	return fmt.Sprintf("Inspect rawError for %s and fix the upstream dependency/configuration.", toolName) // Provide a deterministic fallback hint.
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

// ResourcesList handles the resources/list request
func (h *MCPHandler) ResourcesList(ctx context.Context, params json.RawMessage) (interface{}, error) {
	// Define available resources
	resources := []map[string]interface{}{
		{
			"uri":         "mcp://appium/artifacts/{traceId}",
			"name":        "Test Artifacts",
			"description": "获取测试执行产生的工件（截图、日志等）",
			"mimeType":    "application/json",
		},
		{
			"uri":         "mcp://appium/traces/{traceId}",
			"name":        "Execution Trace",
			"description": "获取测试执行的详细追踪记录",
			"mimeType":    "application/json",
		},
		{
			"uri":         "mcp://appium/sessions/{sessionId}",
			"name":        "Session Info",
			"description": "获取会话详细信息",
			"mimeType":    "application/json",
		},
	}

	return map[string]interface{}{
		"resources": resources,
	}, nil
}

// ResourcesRead handles the resources/read request
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
			return nil, &MCPError{Code: -32000, Message: "Failed to get artifacts", Data: err.Error()}
		}
		content = map[string]interface{}{"traceId": id, "artifacts": artifacts}

	case "traces":
		trace, events, err := h.orch.GetTrace(ctx, id)
		if err != nil {
			return nil, &MCPError{Code: -32000, Message: "Failed to get trace", Data: err.Error()}
		}
		content = map[string]interface{}{"trace": trace, "events": events}

	case "sessions":
		sess, err := h.orch.GetSession(ctx, id)
		if err != nil {
			return nil, &MCPError{Code: -32000, Message: "Failed to get session", Data: err.Error()}
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
	return map[string]interface{}{
		"contents": []map[string]interface{}{
			{
				"uri":      p.URI,
				"mimeType": "application/json",
				"text":     string(text),
			},
		},
	}, nil
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
