package jsonrpc

import (
	"context"
	"encoding/json"
	"net/http"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/capabilities"
	"mcp_for_appium/internal/gateway/mcp"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
)

type Handler struct {
	orch       *orchestrator.Service
	mcpHandler *mcp.MCPHandler
	capService *capabilities.Service
	validator  *Validator
}

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
	case "replay", "subscribe", "unsubscribe":
		return nil, errors.New(errors.CodeStepUnsupported, "method not implemented: "+req.Method)
	default:
		return nil, &mcp.MCPError{
			Code:    -32601,
			Message: "Method not found",
		}
	}

	if err != nil {
		return nil, err
	}

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

func (h *Handler) writeResponse(w http.ResponseWriter, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
