// handler.go authorizes, audits, upgrades, and serves trace-scoped WebSocket event subscriptions.
package websocket

import (
	"log/slog"
	"net/http"
	"strings"

	"mcp_for_appium/internal/audit"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/telemetry"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Handler struct {
	hub           *Hub
	tokenStore    *SubscriptionTokenStore
	upgrader      websocket.Upgrader
	auditRecorder audit.Recorder
	logger        *slog.Logger
}

// NewHandler constructs one WebSocket handler that enforces origin checks plus short-lived subscription-token validation.
func NewHandler(hub *Hub, tokenStore *SubscriptionTokenStore, allowedOrigins []string) *Handler {
	handler := &Handler{
		hub:        hub,
		tokenStore: tokenStore,
		logger:     telemetry.Logger(),
		upgrader: websocket.Upgrader{
			CheckOrigin: newOriginChecker(allowedOrigins), // Enforce same-origin or explicit allowlist checks during the WebSocket handshake instead of accepting every browser origin.
		},
	}
	if tokenStore != nil { // Share the token store's recorder so issuance and connection lifecycle events land in the same sink.
		handler.auditRecorder = tokenStore.auditRecorder // Reuse optional audit wiring without expanding the public constructor signature.
	}
	return handler // Return the fully configured WebSocket handler.
}

// ServeHTTP upgrades one authorized HTTP request into a trace-scoped WebSocket subscription.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID")) // Reuse upstream request correlation when the handshake came through another trusted middleware.
	if requestID == "" {                                         // Generate a local correlation identifier for the public WebSocket route.
		requestID = uuid.NewString() // Assign one random request identifier without deriving it from the token or query string.
	}
	w.Header().Set("X-Request-ID", requestID) // Return the effective correlation id on rejected HTTP handshakes and successful upgrade responses.
	claims, err := h.resolveSubscription(r)   // Resolve and validate the short-lived subscription token before any WebSocket upgrade occurs.
	if err != nil {                           // Stop immediately when the request is missing or presents an invalid subscription token.
		h.recordWebSocketAudit(r, requestID, claims, "denied", websocketAuditReason(err)) // Record denied and expired attempts without storing the opaque query token.
		message := "websocket authorization failed"                                       // Return one stable public diagnostic for unknown and malformed subscription credentials.
		if errors.IsCode(err, errors.CodeTokenExpired) {                                  // Preserve an actionable public distinction for short-lived credential refresh.
			message = "subscription token expired" // Tell the browser to request a fresh token without exposing stored claims.
		}
		http.Error(w, message, errors.MapToHTTP(err)) // Return the mapped auth status with sanitized text.
		return                                        // Stop before the WebSocket handshake begins because the subscription is not authorized.
	}

	conn, err := h.upgrader.Upgrade(w, r, nil) // Perform the WebSocket upgrade only after the subscription token and origin policy have both been validated.
	if err != nil {
		h.recordWebSocketAudit(r, requestID, claims, "denied", "websocket_upgrade_failed") // Record origin, protocol, and handshake rejection without copying the upgrader error text.
		return
	}
	h.recordWebSocketAudit(r, requestID, claims, "success", "") // Record successful connection establishment under the authoritative token claims.
	defer func() {                                              // Record connection closure and release the network resource together.
		h.recordWebSocketAudit(r, requestID, claims, "closed", "") // Append the normal connection lifecycle terminal event without message payloads.
		_ = conn.Close()                                           // Close the WebSocket connection after lifecycle recording returns.
	}()

	client := h.hub.Subscribe(r.Context(), claims.TraceID, conn.RemoteAddr().String()) // Bind the new client subscription to the trace authorized by the opaque subscription token.
	defer h.hub.Unsubscribe(client)

	done := make(chan struct{})
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				close(done)
				return
			}
		}
	}()

	for {
		select {
		case msg, ok := <-client.send:
			if !ok {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

// resolveSubscription validates the short-lived subscription token and returns the authoritative trace binding for the current request.
func (h *Handler) resolveSubscription(r *http.Request) (*SubscriptionTokenClaims, error) {
	if h == nil || h.tokenStore == nil { // Reject requests when the handler has no token store because authorized trace subscriptions cannot be resolved safely.
		return nil, errors.New(errors.CodeInternal, "websocket subscription token store is not configured") // Surface missing startup wiring as an internal error instead of panicking.
	}

	subscriptionToken := strings.TrimSpace(r.URL.Query().Get("subscriptionToken")) // Read the opaque subscription token from the agreed browser query parameter transport.
	claims, err := h.tokenStore.Lookup(r.Context(), subscriptionToken)             // Resolve the subscription token against the authoritative Redis-backed token store.
	if err != nil {                                                                // Stop immediately when the token is missing, expired, or unknown.
		return claims, err // Preserve safe stored claims for expired-token audit attribution while returning nil for unknown tokens.
	}
	requestTraceID := strings.TrimSpace(r.URL.Query().Get("traceId")) // Read the optional traceId query parameter so callers can still include it for debugging or compatibility checks.
	if requestTraceID != "" && requestTraceID != claims.TraceID {     // Reject mismatched trace IDs because the query string must never override the authorized trace binding stored in the token.
		return nil, errors.New(errors.CodePermissionDenied, "traceId does not match subscription token") // Surface mismatched trace bindings as a permission failure instead of subscribing to the wrong trace.
	}

	return claims, nil // Return the authoritative trace binding carried by the subscription token so the handler can open the correct trace stream.
}

// recordWebSocketAudit appends one sanitized WebSocket connection lifecycle event without storing opaque tokens or message payloads.
func (h *Handler) recordWebSocketAudit(r *http.Request, requestID string, claims *SubscriptionTokenClaims, result string, reason string) {
	if h == nil || h.auditRecorder == nil { // Skip optional audit work when gateway startup did not configure a recorder.
		return // Preserve WebSocket behavior in isolated tests and deployments without PostgreSQL audit persistence.
	}
	event := audit.Event{ // Build a pre-authentication-safe event before optionally attaching authoritative claims.
		ActorID:      "anonymous",                                     // Attribute unknown-token attempts without inventing a principal.
		ActorType:    "anonymous",                                     // Classify denied callers explicitly for security review.
		AuthScheme:   "subscription_token",                            // Record the short-lived WebSocket credential scheme without its value.
		Action:       "ws.connect",                                    // Use one stable action for connection success, denial, expiry, and closure.
		ResourceType: "trace",                                         // Classify trace event delivery as the authorized resource.
		ResourceID:   strings.TrimSpace(r.URL.Query().Get("traceId")), // Preserve only the optional trace identifier, never the subscriptionToken query value.
		Result:       strings.TrimSpace(result),                       // Record success, denied, or closed lifecycle outcome.
		Reason:       strings.TrimSpace(reason),                       // Record a stable error code or sanitized upgrade reason.
		SourceIP:     audit.RequestSourceIP(r),                        // Record the directly observed handshake peer.
		RequestID:    strings.TrimSpace(requestID),                    // Correlate the WebSocket lifecycle with HTTP response telemetry.
	}
	if claims != nil { // Attribute successful and expired known-token attempts using authoritative stored metadata.
		event.ActorID = strings.TrimSpace(claims.SubjectID)  // Record the principal originally authorized to receive trace events.
		event.ActorType = "subject"                          // Classify the known token owner as an authenticated subject.
		event.CredentialID = strings.TrimSpace(claims.JTI)   // Record only the independent JTI, not the opaque token value.
		event.TenantID = strings.TrimSpace(claims.TenantID)  // Preserve tenant scope for audit filtering.
		event.ResourceID = strings.TrimSpace(claims.TraceID) // Use the authoritative token trace binding instead of caller query data.
		event.Metadata = map[string]interface{}{             // Preserve safe connection authorization metadata.
			"scope":     strings.TrimSpace(claims.Scope), // Record the granted trace-events scope.
			"expiresAt": claims.ExpiresAt.UTC(),          // Record the credential expiry used during authorization.
		}
		if result == "success" || result == "closed" { // Attach a trace foreign key only for a connection whose trace was accepted and therefore known to exist.
			event.TraceID = strings.TrimSpace(claims.TraceID) // Enable indexed trace audit lookup for successful connection lifecycle records.
		}
	}
	if err := h.auditRecorder.Record(r.Context(), event); err != nil { // Append best-effort without changing handshake authorization or live connection behavior.
		logger := h.logger // Reuse the production logger initialized by NewHandler.
		if logger == nil { // Fall back for zero-value handlers used by direct unit tests.
			logger = telemetry.Logger() // Preserve audit sink failure observability without panicking.
		}
		logger.WarnContext(r.Context(), "failed to record websocket audit event", "request_id", requestID, "result", result, "error", err) // Log no opaque token or message data.
	}
}

// websocketAuditReason returns one stable repository error code or a generic authorization marker without wrapped error text.
func websocketAuditReason(err error) string {
	if code, ok := errors.CodeOf(err); ok { // Prefer the repository's machine-readable authentication or expiry code.
		return string(code) // Store only the stable code in the append-only audit event.
	}
	return "websocket_authorization_failed" // Classify untyped handshake failures without copying their text.
}
