// router.go exposes the minimal non-A2A HTTP helper endpoint used to mint short-lived WebSocket subscription tokens for browser clients.
package rest

import (
	"encoding/json"
	"net/http"
	"strings"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/errors"
	gatewaywebsocket "mcp_for_appium/internal/gateway/websocket"
	"mcp_for_appium/internal/orchestrator"
)

// Router serves the remaining non-A2A HTTP helper route used by browser WebSocket clients.
type Router struct {
	orch                   *orchestrator.Service
	subscriptionTokenStore *gatewaywebsocket.SubscriptionTokenStore
}

// NewRouter constructs the HTTP helper router around the orchestrator service used for trace ownership checks.
func NewRouter(orch *orchestrator.Service) *Router {
	return &Router{
		orch: orch, // Persist the orchestrator dependency so the token endpoint can authorize trace ownership through the source-of-truth service layer.
	}
}

// SetSubscriptionTokenStore injects the Redis-backed WebSocket subscription-token store used by the browser subscription issuance endpoint.
func (rt *Router) SetSubscriptionTokenStore(store *gatewaywebsocket.SubscriptionTokenStore) {
	rt.subscriptionTokenStore = store // Store the shared token store so the browser token endpoint can mint opaque tokens backed by the same authority used during the WebSocket handshake.
}

// RegisterRoutes mounts the remaining browser helper route after the legacy A2A-style REST business endpoints have been removed.
func (rt *Router) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/ws/traces/", rt.handleTraceSubscriptionTokenRequest) // Register the non-A2A browser helper route that mints short-lived WebSocket subscription tokens for one authorized trace.
}

// handleTraceSubscriptionTokenRequest routes one browser token-issuance request to the trace-scoped helper after validating the path shape.
func (rt *Router) handleTraceSubscriptionTokenRequest(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/ws/traces/") // Remove the stable route prefix so only the trace-scoped suffix remains for validation and extraction.
	if path == "" {                                           // Reject empty suffixes because every browser token request must target exactly one trace identifier.
		writeAPIError(w, http.StatusBadRequest, "trace id required", nil) // Return a stable validation error when the browser helper route omits the required trace id segment.
		return                                                            // Stop after the validation error because no trace-scoped token can be issued without a concrete trace id.
	}
	if !strings.HasSuffix(path, "/subscription-token") { // Reject unknown child paths because this router no longer exposes any A2A-style trace read or event endpoints.
		writeAPIError(w, http.StatusNotFound, "not found", nil) // Return a stable not-found error for unsupported helper paths under the /api/ws/traces prefix.
		return                                                  // Stop after the not-found error because the request did not target the token-issuance helper route.
	}

	traceID := strings.TrimSuffix(path, "/subscription-token") // Extract the authoritative trace id segment that the browser token should bind to.
	if traceID == "" || strings.Contains(traceID, "/") {       // Reject malformed trace identifiers because path traversal or empty ids must never reach the token issuer.
		writeAPIError(w, http.StatusBadRequest, "invalid trace id", nil) // Return a stable validation error when the trace id path segment is empty or malformed.
		return                                                           // Stop after the validation error because no trace-scoped token can be issued from an invalid path.
	}

	rt.handleTraceSubscriptionToken(w, r, traceID) // Delegate the now-validated request to the shared trace-scoped token-issuance helper.
}

// handleTraceSubscriptionToken issues one short-lived opaque WebSocket subscription token bound to the authenticated caller and one trace identifier.
func (rt *Router) handleTraceSubscriptionToken(w http.ResponseWriter, r *http.Request, traceID string) {
	if r.Method != http.MethodPost { // Reject unsupported verbs because the browser token endpoint is intentionally write-only.
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed", nil) // Return a stable method error for unsupported verbs hitting the browser token endpoint.
		return                                                                   // Stop after the method error because token issuance must not continue on unsupported verbs.
	}
	if rt.subscriptionTokenStore == nil { // Reject requests when the gateway has not wired the token store because token issuance cannot proceed without a shared backing store.
		writeAPIError(w, http.StatusServiceUnavailable, "subscription tokens unavailable", nil) // Return a stable service-unavailable error for missing startup wiring.
		return                                                                                  // Stop after the service-unavailable error because token issuance cannot proceed safely.
	}

	subject, ok := auth.SubjectFrom(r.Context()) // Resolve the authenticated caller injected by the gateway auth middleware before issuing a trace-scoped subscription token.
	if !ok {                                     // Reject missing authenticated subjects because subscription tokens must never be issued anonymously.
		writeAPIError(w, http.StatusUnauthorized, "authentication required", nil) // Return a stable auth failure for requests that reached the endpoint without a subject context.
		return                                                                    // Stop after the auth failure because token issuance requires a caller identity.
	}
	if _, _, err := rt.orch.GetTrace(r.Context(), traceID); err != nil { // Load the authoritative trace through the orchestrator before issuing a token so the request must pass persisted ownership validation.
		writeAPIError(w, errors.MapToHTTP(err), "failed to authorize trace subscription", err) // Surface missing or unauthorized traces before any short-lived WebSocket token is minted.
		return                                                                                 // Stop after the authorization failure because the caller must not receive a usable subscription token.
	}

	issuedToken, err := rt.subscriptionTokenStore.Issue(r.Context(), subject, traceID) // Issue the short-lived opaque token bound to the caller and requested trace ID.
	if err != nil {                                                                    // Stop immediately when token issuance fails because no WebSocket subscription can be established without it.
		writeAPIError(w, errors.MapToHTTP(err), "failed to issue subscription token", err) // Surface token-store failures with a stable internal code and no backend cause text.
		return                                                                             // Stop after the issuance failure because no valid response payload exists.
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{ // Return the issued token payload in a compact browser-friendly response shape.
		"traceId":           issuedToken.TraceID,   // Echo the trace binding so the caller can correlate the returned token with its intended subscription.
		"subscriptionToken": issuedToken.Token,     // Return the opaque short-lived token that the browser will supply on the WebSocket handshake.
		"expiresAt":         issuedToken.ExpiresAt, // Return the token expiry so the caller knows when it must refresh the subscription credential.
		"scope":             issuedToken.Scope,     // Return the granted scope so clients can reason about what the token authorizes.
	})
}

// writeJSON writes one JSON response body with the supplied HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json") // Mark every helper response as JSON so browser and API clients can decode it predictably.
	w.WriteHeader(status)                              // Emit the chosen HTTP status before the JSON body is serialized to the response writer.
	_ = json.NewEncoder(w).Encode(v)                   // Serialize the response payload as JSON while intentionally ignoring best-effort write errors on already-started responses.
}

// writeAPIError writes one structured helper-endpoint error payload that preserves the internal error code when available.
func writeAPIError(w http.ResponseWriter, status int, message string, err error) {
	internalCode := "" // Initialize the internal code field so untyped errors still produce a stable response shape.
	if err != nil {    // Fill the structured error fields only when a downstream error has actually been supplied.
		if code, ok := errors.CodeOf(err); ok { // Prefer the typed internal error code whenever the downstream error uses the repository-standard error wrapper.
			internalCode = string(code) // Copy the stable machine-readable internal code into the helper error envelope.
		}
	}

	writeJSON(w, status, map[string]interface{}{ // Serialize the stable helper error envelope used by the remaining browser token route.
		"error": map[string]interface{}{ // Nest the details under the conventional error key so clients can parse failures uniformly.
			"message":      message,      // Return the human-readable summary describing why the helper request failed.
			"internalCode": internalCode, // Return the stable machine-readable internal code when one was available downstream.
		},
	})
}
