package rest

import (
	"encoding/json"
	"net/http"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/orchestrator"
)

type Router struct {
	orch *orchestrator.Service
}

func NewRouter(orch *orchestrator.Service) *Router {
	return &Router{orch: orch}
}

func (rt *Router) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sessions", rt.handleSessions)
	mux.HandleFunc("/api/v1/plans", rt.handlePlans)
}

func (rt *Router) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		var req struct {
			ProjectID string                 `json:"projectId"`
			Caps      map[string]interface{} `json:"caps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		sess, err := rt.orch.StartSession(r.Context(), req.ProjectID, req.Caps)
		if err != nil {
			http.Error(w, err.Error(), errors.MapToHTTP(err))
			return
		}

		writeJSON(w, http.StatusCreated, map[string]string{"sessionId": sess.ID})
		return
	}
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func (rt *Router) handlePlans(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		var req struct {
			SessionID string          `json:"sessionId"`
			Plan      json.RawMessage `json:"plan"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		traceID, err := rt.orch.ExecutePlan(r.Context(), req.SessionID, req.Plan)
		if err != nil {
			http.Error(w, err.Error(), errors.MapToHTTP(err))
			return
		}

		writeJSON(w, http.StatusAccepted, map[string]string{"traceId": traceID})
		return
	}
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
