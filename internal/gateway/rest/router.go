package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
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
	orch           *orchestrator.Service
	cache          *redis.Cache
	capService     *capabilities.Service
	hmacValidator  *auth.HMACValidator
	idempotencyTTL time.Duration
}

// NewRouter executes this operation.
func NewRouter(orch *orchestrator.Service, cache *redis.Cache, capSvc *capabilities.Service) *Router {
	return &Router{
		orch:           orch,
		cache:          cache,
		capService:     capSvc,
		idempotencyTTL: 24 * time.Hour,
	}
}

// SetHMACValidator executes this operation.
func (rt *Router) SetHMACValidator(validator *auth.HMACValidator) {
	rt.hmacValidator = validator
}

// RegisterRoutes executes this operation.
func (rt *Router) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sessions", rt.handleSessions)
	mux.HandleFunc("/api/v1/plans:execute", rt.handlePlanExecute)
	mux.HandleFunc("/api/v1/plans/", rt.handlePlanCancel)
	mux.HandleFunc("/api/v1/traces/", rt.handleTraces)
	mux.HandleFunc("/api/v1/artifacts", rt.handleArtifacts)
	mux.HandleFunc("/api/v1/capabilities", rt.handleCapabilities)
}

// handleSessions executes this operation.
func (rt *Router) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid body", err) // Return structured parse error details for invalid request bodies.
			return
		}
		if err := rt.verifyHMAC(r, body); err != nil {
			writeAPIError(w, http.StatusUnauthorized, "HMAC verification failed", err) // Preserve auth failure root cause in structured error payload.
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
			writeAPIError(w, http.StatusBadRequest, "invalid body", err) // Return JSON decode failure details to callers.
			return
		}

		sess, err := rt.orch.StartSession(r.Context(), req.ProjectID, req.W3cCapsJson)
		if err != nil {
			writeAPIError(w, errors.MapToHTTP(err), "failed to start session", err) // Surface downstream Appium/DB errors with internal code and raw text.
			return
		}

		payload := map[string]string{"sessionId": sess.ID}
		rt.storeIdempotent(r, payload)
		writeJSON(w, http.StatusCreated, payload)
		return
	}
	writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed", nil) // Return structured HTTP method error for unsupported verbs.
}

// handlePlanExecute executes this operation.
func (rt *Router) handlePlanExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid body", err) // Return structured parse error details for invalid request bodies.
			return
		}
		if err := rt.verifyHMAC(r, body); err != nil {
			writeAPIError(w, http.StatusUnauthorized, "HMAC verification failed", err) // Preserve auth verification failure details.
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
			writeAPIError(w, http.StatusBadRequest, "invalid body", err) // Return decode details for malformed execute request payload.
			return
		}

		traceID, err := rt.orch.ExecutePlanWithTrace(r.Context(), req.SessionID, req.TraceID, req.Plan)
		if err != nil {
			writeAPIError(w, errors.MapToHTTP(err), "failed to execute plan", err) // Surface orchestrator/dispatch failures with raw details.
			return
		}

		payload := map[string]string{"traceId": traceID}
		rt.storeIdempotent(r, payload)
		writeJSON(w, http.StatusAccepted, payload)
		return
	}
	writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed", nil) // Return structured HTTP method error for unsupported verbs.
}

// handlePlanCancel executes this operation.
func (rt *Router) handlePlanCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed", nil) // Return structured method error to caller.
		return
	}

	if err := rt.verifyHMAC(r, nil); err != nil {
		writeAPIError(w, http.StatusUnauthorized, "HMAC verification failed", err) // Return structured auth failure details.
		return
	}

	if !strings.HasSuffix(r.URL.Path, ":cancel") {
		writeAPIError(w, http.StatusNotFound, "not found", nil) // Return structured not-found response for unexpected path shape.
		return
	}

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/plans/"), ":")
	if len(parts) != 2 || parts[0] == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid trace id", nil) // Return validation error for malformed cancel path argument.
		return
	}

	if err := rt.orch.CancelPlan(r.Context(), parts[0]); err != nil {
		writeAPIError(w, errors.MapToHTTP(err), "failed to cancel plan", err) // Surface cancel failures from orchestrator and worker propagation.
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

// handleTraces executes this operation.
func (rt *Router) handleTraces(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/traces/")
	if path == "" {
		writeAPIError(w, http.StatusBadRequest, "trace id required", nil) // Return structured validation error when trace id is missing.
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
			writeAPIError(w, errors.MapToHTTP(err), "failed to get events", err) // Surface event listing failures with detailed downstream cause.
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"events": events})
		return
	}

	trace, events, err := rt.orch.GetTrace(r.Context(), path)
	if err != nil {
		writeAPIError(w, errors.MapToHTTP(err), "failed to get trace", err) // Surface trace read failures with structured diagnostics.
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"trace":  trace,
		"events": events,
	})
}

// handleArtifacts executes this operation.
func (rt *Router) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed", nil) // Return structured method error for unsupported verbs.
		return
	}

	traceID := r.URL.Query().Get("traceId")
	if traceID == "" {
		writeAPIError(w, http.StatusBadRequest, "traceId required", nil) // Return structured validation error when traceId query parameter is missing.
		return
	}
	artifacts, err := rt.orch.GetArtifacts(r.Context(), traceID)
	if err != nil {
		writeAPIError(w, errors.MapToHTTP(err), "failed to get artifacts", err) // Surface artifact retrieval failures with raw downstream context.
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"artifacts": artifacts})
}

// handleCapabilities executes this operation.
func (rt *Router) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed", nil) // Return structured method error for unsupported verbs.
		return
	}
	if rt.capService == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "capabilities unavailable", nil) // Return structured service-unavailable error for missing capability provider.
		return
	}
	writeJSON(w, http.StatusOK, rt.capService.List(r.Context()))
}

// writeJSON executes this operation.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// verifyHMAC executes this operation.
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

// maybeReturnIdempotent executes this operation.
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
		writeAPIError(w, http.StatusInternalServerError, "idempotency store error", err) // Surface cache-layer errors with internal codes and raw cause.
		return true
	}
	if acquired {
		return false
	}
	if val == "" {
		writeAPIError(w, http.StatusConflict, "duplicate request", nil) // Return structured conflict for duplicate idempotency key without stored payload.
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(val))
	return true
}

// internalCodePattern matches internal error tokens like [E.STORE.CONN] from wrapped errors.
var internalCodePattern = regexp.MustCompile(`\[(E\.[A-Z0-9._]+)\]`) // Compile regex once for stable and low-overhead extraction.

// writeAPIError executes this operation.
func writeAPIError(w http.ResponseWriter, status int, message string, err error) {
	internalCode := "" // Initialize empty internal code for errors without standardized wrapper.
	rawError := ""     // Initialize raw error text that will carry source/native details to callers.
	if err != nil {    // Fill structured fields when a downstream error is available.
		rawError = err.Error()               // Preserve exact wrapped/native error text.
		if c, ok := errors.CodeOf(err); ok { // Prefer typed internal code when err is internal Error.
			internalCode = string(c) // Record standardized internal code for machine handling.
		} else if match := internalCodePattern.FindStringSubmatch(rawError); len(match) == 2 { // Fallback parse for wrapped string errors.
			internalCode = match[1] // Extract token-form internal code from raw error text.
		}
	}
	if strings.TrimSpace(message) == "" { // Ensure top-level message is never empty.
		if strings.TrimSpace(rawError) != "" { // Prefer raw error when explicit message is missing.
			message = rawError // Use raw error text as user-visible message fallback.
		} else {
			message = "request failed" // Final fallback message when neither explicit nor raw error exists.
		}
	}
	payload := map[string]interface{}{ // Build consistent REST error envelope for all endpoints.
		"error": map[string]interface{}{ // Nest details under error key for stable client parsing.
			"message":      message,                            // Human-readable top-level failure summary.
			"internalCode": internalCode,                       // Stable internal code when available.
			"rawError":     rawError,                           // Full downstream/native error details.
			"hint":         restFailureHint(rawError, message), // Actionable suggestion derived from failure text.
		},
	}
	writeJSON(w, status, payload) // Return JSON error body with requested HTTP status.
}

// restFailureHint executes this operation.
func restFailureHint(raw string, message string) string {
	text := strings.ToLower(strings.TrimSpace(raw + " " + message)) // Normalize source and summary text for robust pattern checks.
	switch {                                                        // Match frequent operational failure categories first.
	case strings.Contains(text, "could not resolve host"), strings.Contains(text, "no such host"):
		return "Check DNS/network egress from the server runtime."
	case strings.Contains(text, "connection refused"):
		return "Target dependency is not listening; verify host, port, and process health."
	case strings.Contains(text, "role \"postgres\" does not exist"):
		return "Update storage.postgres.dsn or create the required Postgres role."
	case strings.Contains(text, "invalid body"):
		return "Validate request JSON schema and field types."
	case strings.Contains(text, "hmac"):
		return "Verify HMAC headers, timestamp skew, nonce uniqueness, and shared secret."
	case strings.Contains(text, "duplicate request"):
		return "Use a new Idempotency-Key for a new operation, or reuse the previous response payload."
	}
	return fmt.Sprintf("Inspect rawError and server logs for endpoint-specific root cause: %s.", message) // Provide deterministic fallback guidance.
}

// storeIdempotent executes this operation.
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
