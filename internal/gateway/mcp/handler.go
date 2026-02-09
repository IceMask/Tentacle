package mcp

import (
	"context"
	"encoding/json"
	"strings"

	"mcp_for_appium/internal/orchestrator"
)

// MCPHandler handles MCP protocol methods
type MCPHandler struct {
	registry *ToolRegistry
	orch     *orchestrator.Service
}

// NewMCPHandler creates a new MCP handler
func NewMCPHandler(orch *orchestrator.Service) *MCPHandler {
	return &MCPHandler{
		registry: NewToolRegistry(),
		orch:     orch,
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
				"subscribe":   true, // Support resource subscriptions
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

	// Validate tool exists and arguments
	if err := h.registry.Validate(p.Name, p.Arguments); err != nil {
		return nil, err
	}

	// Route to the appropriate business method
	result, err := h.executeTool(ctx, p.Name, p.Arguments)
	if err != nil {
		return nil, err
	}

	// Wrap result in MCP format
	return map[string]interface{}{
		"content": []map[string]interface{}{
			{
				"type": "text",
				"text": formatToolResult(p.Name, result),
			},
		},
		"isError": false,
	}, nil
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
	default:
		return nil, &MCPError{
			Code:    -32601,
			Message: "Tool not found: " + toolName,
		}
	}
}

// Business method handlers

func (h *MCPHandler) handleStartSession(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		ProjectID  string                 `json:"projectId"`
		ActorID    string                 `json:"actorId"`
		Labels     map[string]string      `json:"labels"`
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

func (h *MCPHandler) handleExecutePlan(ctx context.Context, arguments json.RawMessage) (interface{}, error) {
	var args struct {
		SessionID string          `json:"sessionId"`
		Plan      json.RawMessage `json:"plan"`
		TraceID   string          `json:"traceId"`
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

	return map[string]interface{}{
		"traceId": traceID,
		"status":  "running",
	}, nil
}

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
