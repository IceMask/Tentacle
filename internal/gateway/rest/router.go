package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/capabilities"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/redis"
)

type Router struct {
	orch            *orchestrator.Service
	cache           *redis.Cache
	capService      *capabilities.Service
	hmacValidator   *auth.HMACValidator
	idempotencyTTL  time.Duration
}

func NewRouter(orch *orchestrator.Service, cache *redis.Cache, capSvc *capabilities.Service) *Router {
	return &Router{
		orch:           orch,
		cache:          cache,
		capService:     capSvc,
		idempotencyTTL: 24 * time.Hour,
	}
}

func (rt *Router) SetHMACValidator(validator *auth.HMACValidator) {
	rt.hmacValidator = validator
}

func (rt *Router) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sessions", rt.handleSessions)
	mux.HandleFunc("/api/v1/plans:execute", rt.handlePlanExecute)
	mux.HandleFunc("/api/v1/plans/", rt.handlePlanCancel)
	mux.HandleFunc("/api/v1/traces/", rt.handleTraces)
	mux.HandleFunc("/api/v1/artifacts", rt.handleArtifacts)
	mux.HandleFunc("/api/v1/capabilities", rt.handleCapabilities)
}

func (rt *Router) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if err := rt.verifyHMAC(r, body); err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		if rt.maybeReturnIdempotent(w, r, body) {
			return
		}

		var req struct {
			ProjectID   string                 `json:"projectId"`
			W3cCapsJson map[string]interface{} `json:"w3cCapsJson"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		sess, err := rt.orch.StartSession(r.Context(), req.ProjectID, req.W3cCapsJson)
		if err != nil {
			http.Error(w, err.Error(), errors.MapToHTTP(err))
			return
		}

		payload := map[string]string{"sessionId": sess.ID}
		rt.storeIdempotent(r, payload)
		writeJSON(w, http.StatusCreated, payload)
		return
	}
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func (rt *Router) handlePlanExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if err := rt.verifyHMAC(r, body); err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		if rt.maybeReturnIdempotent(w, r, body) {
			return
		}

		var req struct {
			SessionID string          `json:"sessionId"`
			Plan      json.RawMessage `json:"plan"`
			TraceID   string          `json:"traceId"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		traceID, err := rt.orch.ExecutePlanWithTrace(r.Context(), req.SessionID, req.TraceID, req.Plan)
		if err != nil {
			http.Error(w, err.Error(), errors.MapToHTTP(err))
			return
		}

		payload := map[string]string{"traceId": traceID}
		rt.storeIdempotent(r, payload)
		writeJSON(w, http.StatusAccepted, payload)
		return
	}
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func (rt *Router) handlePlanCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := rt.verifyHMAC(r, nil); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if !strings.HasSuffix(r.URL.Path, ":cancel") {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/plans/"), ":")
	if len(parts) != 2 || parts[0] == "" {
		http.Error(w, "invalid trace id", http.StatusBadRequest)
		return
	}

	if err := rt.orch.CancelPlan(r.Context(), parts[0]); err != nil {
		http.Error(w, err.Error(), errors.MapToHTTP(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (rt *Router) handleTraces(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/traces/")
	if path == "" {
		http.Error(w, "trace id required", http.StatusBadRequest)
		return
	}

	if strings.HasSuffix(path, "/events") {
		traceID := strings.TrimSuffix(path, "/events")
		since := int64(0)
		if val := r.URL.Query().Get("since"); val != "" {
			if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
				since = parsed
			}
		}

		events, err := rt.orch.GetEvents(r.Context(), traceID, since)
		if err != nil {
			http.Error(w, err.Error(), errors.MapToHTTP(err))
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"events": events})
		return
	}

	trace, events, err := rt.orch.GetTrace(r.Context(), path)
	if err != nil {
		http.Error(w, err.Error(), errors.MapToHTTP(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"trace":  trace,
		"events": events,
	})
}

func (rt *Router) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	traceID := r.URL.Query().Get("traceId")
	if traceID == "" {
		http.Error(w, "traceId required", http.StatusBadRequest)
		return
	}
	artifacts, err := rt.orch.GetArtifacts(r.Context(), traceID)
	if err != nil {
		http.Error(w, err.Error(), errors.MapToHTTP(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"artifacts": artifacts})
}

func (rt *Router) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if rt.capService == nil {
		http.Error(w, "capabilities unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, rt.capService.List(r.Context()))
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (rt *Router) verifyHMAC(r *http.Request, body []byte) error {
	if rt.hmacValidator == nil {
		return nil
	}
	sig := r.Header.Get("X-MCP-Signature")
	ts := r.Header.Get("X-MCP-Timestamp")
	nonce := r.Header.Get("X-MCP-Nonce")
	keyID := r.Header.Get("X-MCP-Key-Id")
	if sig == "" || ts == "" || nonce == "" || keyID == "" {
		return errors.New(errors.CodeUnauthenticated, "missing HMAC headers")
	}
	_, err := rt.hmacValidator.Verify(r.Context(), sig, r.Method, r.URL.Path, string(body), ts, nonce, keyID)
	return err
}

func (rt *Router) maybeReturnIdempotent(w http.ResponseWriter, r *http.Request, body []byte) bool {
	if rt.cache == nil {
		return false
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		return false
	}
	redisKey := "idempotency:" + key
	acquired, val, err := rt.cache.CheckAndSet(r.Context(), redisKey, "", rt.idempotencyTTL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return true
	}
	if acquired {
		return false
	}
	if val == "" {
		http.Error(w, "duplicate request", http.StatusConflict)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(val))
	return true
}

func (rt *Router) storeIdempotent(r *http.Request, payload interface{}) {
	if rt.cache == nil {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		return
	}
	body, _ := json.Marshal(payload)
	_ = rt.cache.Set(r.Context(), "idempotency:"+key, string(body), rt.idempotencyTTL)
}
