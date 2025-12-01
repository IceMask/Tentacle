package jsonrpc

import (
	"encoding/json"
	"net/http"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
)

type Handler struct {
	orch *orchestrator.Service
}

func NewHandler(orch *orchestrator.Service) *Handler {
	return &Handler{orch: orch}
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

	var result interface{}
	var err error

	ctx := r.Context()

	switch req.Method {
	case "startSession":
		var params struct {
			ProjectID string                 `json:"projectId"`
			Caps      map[string]interface{} `json:"caps"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			h.writeError(w, req.ID, -32602, "Invalid params", "failed to decode startSession params")
			return
		}

		var sess *postgres.Session
		sess, err = h.orch.StartSession(ctx, params.ProjectID, params.Caps)
		if err == nil {
			result = map[string]string{"sessionId": sess.ID}
		}
	case "executePlan":
		var params struct {
			SessionID string          `json:"sessionId"`
			Plan      json.RawMessage `json:"plan"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			h.writeError(w, req.ID, -32602, "Invalid params", "failed to decode executePlan params")
			return
		}
		var traceID string
		traceID, err = h.orch.ExecutePlan(ctx, params.SessionID, params.Plan)
		if err == nil {
			result = map[string]string{"traceId": traceID}
		}
	default:
		h.writeError(w, req.ID, -32601, "Method not found", nil)
		return
	}

	if err != nil {
		// Map error
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
