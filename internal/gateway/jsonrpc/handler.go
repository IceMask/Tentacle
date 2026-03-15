package jsonrpc

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/capabilities"
	"mcp_for_appium/internal/gateway/mcp"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/telemetry"
)

type Handler struct {
	orch       *orchestrator.Service
	mcpHandler *mcp.MCPHandler
	capService *capabilities.Service
	validator  *Validator
}

// NewHandler executes this operation.
func NewHandler(orch *orchestrator.Service, capSvc *capabilities.Service) *Handler {
	return &Handler{
		orch:       orch,
		mcpHandler: mcp.NewMCPHandler(orch),
		capService: capSvc,
		validator:  NewValidator(),
	}
}

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id"`
}

type Response struct {
	JSONRPC string               `json:"jsonrpc"`
	Result  interface{}          `json:"result,omitempty"`
	Error   *errors.JSONRPCError `json:"error,omitempty"`
	ID      interface{}          `json:"id"`
}

// ProcessRequest processes a JSON-RPC request and returns the result or error
// This method is used by both HTTP and stdio transports
func (h *Handler) ProcessRequest(ctx context.Context, req *Request) (interface{}, error) {
	logger := telemetry.WithContext(ctx) // Build context-enriched logger so per-request logs carry trace ids when available.
	startedAt := time.Now()              // Capture dispatch start timestamp for duration reporting.
	logger.Info("jsonrpc process begin", "method", req.Method, "id", req.ID) // Log every JSON-RPC method entry for step-by-step tracing.

	var result interface{}
	var err error

	if h.validator != nil {
		if vErr := h.validator.Validate(req.Method, req.Params); vErr != nil {
			return nil, vErr
		}
	}

	switch req.Method {
	// ===== MCP Protocol Methods =====
	case "initialize":
		result, err = h.mcpHandler.Initialize(ctx, req.Params)
	case "tools/list":
		result, err = h.mcpHandler.ToolsList(ctx, req.Params)
	case "tools/call":
		result, err = h.mcpHandler.ToolsCall(ctx, req.Params)
	case "resources/list":
		result, err = h.mcpHandler.ResourcesList(ctx, req.Params)
	case "resources/read":
		result, err = h.mcpHandler.ResourcesRead(ctx, req.Params)

	// ===== Legacy Direct Methods (for backwards compatibility) =====
	case "startSession":
		var params struct {
			ProjectID   string                 `json:"projectId"`
			ActorID     string                 `json:"actorId"`
			Labels      map[string]string      `json:"labels"`
			W3cCapsJson map[string]interface{} `json:"w3cCapsJson"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{
				Code:    -32602,
				Message: "Invalid params",
				Data:    "failed to decode startSession params",
			}
		}

		var sess *postgres.Session
		sess, err = h.orch.StartSession(ctx, params.ProjectID, params.W3cCapsJson)
		if err == nil {
			result = map[string]string{"sessionId": sess.ID}
		}
	case "executePlan":
		var params struct {
			SessionID string          `json:"sessionId"`
			Plan      json.RawMessage `json:"plan"`
			TraceID   string          `json:"traceId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{
				Code:    -32602,
				Message: "Invalid params",
				Data:    "failed to decode executePlan params",
			}
		}
		var traceID string
		traceID, err = h.orch.ExecutePlanWithTrace(ctx, params.SessionID, params.TraceID, params.Plan)
		if err == nil {
			result = map[string]string{"traceId": traceID}
		}
	case "endSession":
		var params struct {
			SessionID string `json:"sessionId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{
				Code:    -32602,
				Message: "Invalid params",
				Data:    "failed to decode endSession params",
			}
		}
		err = h.orch.EndSession(ctx, params.SessionID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "getSemanticSnapshot":
		var params struct {
			SessionID string `json:"sessionId"`
			SinceRev  string `json:"sinceRev"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{
				Code:    -32602,
				Message: "Invalid params",
				Data:    "failed to decode getSemanticSnapshot params",
			}
		}
		result, err = h.orch.GetSemanticSnapshot(ctx, params.SessionID, params.SinceRev)
	case "takeScreenshot":
		var params struct {
			SessionID    string `json:"sessionId"`
			TraceID      string `json:"traceId"`
			IncludeThumb bool   `json:"includeThumb"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{
				Code:    -32602,
				Message: "Invalid params",
				Data:    "failed to decode takeScreenshot params",
			}
		}
		result, err = h.orch.TakeScreenshot(ctx, params.SessionID, params.TraceID, params.IncludeThumb)
	case "cancelPlan":
		var params struct {
			TraceID string `json:"traceId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{
				Code:    -32602,
				Message: "Invalid params",
				Data:    "failed to decode cancelPlan params",
			}
		}
		err = h.orch.CancelPlan(ctx, params.TraceID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "getTrace":
		var params struct {
			TraceID string `json:"traceId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{
				Code:    -32602,
				Message: "Invalid params",
				Data:    "failed to decode getTrace params",
			}
		}
		var trace *postgres.Trace
		var events []*postgres.PlanEvent
		trace, events, err = h.orch.GetTrace(ctx, params.TraceID)
		if err == nil {
			result = map[string]interface{}{
				"trace":  trace,
				"events": events,
			}
		}
	case "getArtifacts":
		var params struct {
			TraceID string `json:"traceId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{
				Code:    -32602,
				Message: "Invalid params",
				Data:    "failed to decode getArtifacts params",
			}
		}
		var artifacts []*postgres.Artifact
		artifacts, err = h.orch.GetArtifacts(ctx, params.TraceID)
		if err == nil {
			result = map[string]interface{}{"artifacts": artifacts}
		}
	case "describeCapabilities":
		if h.capService == nil {
			err = errors.New(errors.CodeInternal, "capabilities service not configured")
			break
		}
		result = h.capService.List(ctx)
	case "healthCheck":
		result = h.orch.HealthCheck(ctx)
	// Interactive element operations
	case "findElement":
		var params struct {
			SessionID string `json:"sessionId"`
			Strategy  string `json:"strategy"`
			Selector  string `json:"selector"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode findElement params"}
		}
		var elementID string
		elementID, err = h.orch.FindElement(ctx, params.SessionID, params.Strategy, params.Selector)
		if err == nil {
			result = map[string]string{"elementId": elementID}
		}
	case "clickElement":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode clickElement params"}
		}
		err = h.orch.ClickElement(ctx, params.SessionID, params.ElementID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "sendKeysToElement":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
			Text      string `json:"text"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode sendKeysToElement params"}
		}
		err = h.orch.SendKeysToElement(ctx, params.SessionID, params.ElementID, params.Text)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "clearElement":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode clearElement params"}
		}
		err = h.orch.ClearElement(ctx, params.SessionID, params.ElementID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "getElementText":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode getElementText params"}
		}
		var text string
		text, err = h.orch.GetElementText(ctx, params.SessionID, params.ElementID)
		if err == nil {
			result = map[string]string{"text": text}
		}
	case "getElementAttribute":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
			Attribute string `json:"attribute"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode getElementAttribute params"}
		}
		var value string
		value, err = h.orch.GetElementAttribute(ctx, params.SessionID, params.ElementID, params.Attribute)
		if err == nil {
			result = map[string]interface{}{"attribute": params.Attribute, "value": value}
		}
	case "isElementDisplayed":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode isElementDisplayed params"}
		}
		var displayed bool
		displayed, err = h.orch.IsElementDisplayed(ctx, params.SessionID, params.ElementID)
		if err == nil {
			result = map[string]bool{"displayed": displayed}
		}
	case "tap":
		var params struct {
			SessionID string `json:"sessionId"`
			X         int    `json:"x"`
			Y         int    `json:"y"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode tap params"}
		}
		err = h.orch.Tap(ctx, params.SessionID, params.X, params.Y)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "swipe":
		var params struct {
			SessionID  string `json:"sessionId"`
			StartX     int    `json:"startX"`
			StartY     int    `json:"startY"`
			EndX       int    `json:"endX"`
			EndY       int    `json:"endY"`
			DurationMs int    `json:"durationMs"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode swipe params"}
		}
		if params.DurationMs == 0 {
			params.DurationMs = 200
		}
		err = h.orch.Swipe(ctx, params.SessionID, params.StartX, params.StartY, params.EndX, params.EndY, params.DurationMs)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "longPress":
		var params struct {
			SessionID  string `json:"sessionId"`
			ElementID  string `json:"elementId"`
			DurationMs int    `json:"durationMs"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode longPress params"}
		}
		if params.DurationMs == 0 {
			params.DurationMs = 1000
		}
		err = h.orch.LongPress(ctx, params.SessionID, params.ElementID, params.DurationMs)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "pressBack":
		var params struct {
			SessionID string `json:"sessionId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode pressBack params"}
		}
		err = h.orch.PressBack(ctx, params.SessionID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "hideKeyboard":
		var params struct {
			SessionID string `json:"sessionId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode hideKeyboard params"}
		}
		err = h.orch.HideKeyboard(ctx, params.SessionID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "replay", "subscribe", "unsubscribe":
		return nil, errors.New(errors.CodeStepUnsupported, "method not implemented: "+req.Method)
	default:
		return nil, &mcp.MCPError{
			Code:    -32601,
			Message: "Method not found",
		}
	}

	if err != nil {
		logger.Error("jsonrpc process failed", "method", req.Method, "id", req.ID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log method failures with latency and root error.
		return nil, err
	}

	logger.Info("jsonrpc process done", "method", req.Method, "id", req.ID, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful completion with total dispatch latency.
	return result, nil
}

// ServeHTTP implements http.Handler for HTTP-based JSON-RPC
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, nil, -32700, "Parse error", nil)
		return
	}

	if req.JSONRPC != "2.0" {
		h.writeError(w, req.ID, -32600, "Invalid Request", nil)
		return
	}

	ctx := r.Context()
	result, err := h.ProcessRequest(ctx, &req)

	if err != nil {
		// Check if it's an MCP error
		if mcpErr, ok := err.(*mcp.MCPError); ok {
			h.writeResponse(w, Response{
				JSONRPC: "2.0",
				Error: &errors.JSONRPCError{
					Code:    mcpErr.Code,
					Message: mcpErr.Message,
					Data:    mcpErr.Data,
				},
				ID: req.ID,
			})
			return
		}

		// Map other errors
		jsonErr := errors.MapToJSONRPC(err)
		h.writeResponse(w, Response{
			JSONRPC: "2.0",
			Error:   jsonErr,
			ID:      req.ID,
		})
		return
	}

	h.writeResponse(w, Response{
		JSONRPC: "2.0",
		Result:  result,
		ID:      req.ID,
	})
}

// writeError executes this operation.
func (h *Handler) writeError(w http.ResponseWriter, id interface{}, code int, msg string, data interface{}) {
	h.writeResponse(w, Response{
		JSONRPC: "2.0",
		Error: &errors.JSONRPCError{
			Code:    code,
			Message: msg,
			Data:    data,
		},
		ID: id,
	})
}

// writeResponse executes this operation.
func (h *Handler) writeResponse(w http.ResponseWriter, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
