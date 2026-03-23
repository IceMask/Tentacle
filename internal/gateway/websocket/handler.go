package websocket

import (
	"net/http"
	"strings"

	"mcp_for_appium/internal/errors"

	"github.com/gorilla/websocket"
)

type Handler struct {
	hub        *Hub
	tokenStore *SubscriptionTokenStore
	upgrader   websocket.Upgrader
}

// NewHandler constructs one WebSocket handler that enforces origin checks plus short-lived subscription-token validation.
func NewHandler(hub *Hub, tokenStore *SubscriptionTokenStore, allowedOrigins []string) *Handler {
	return &Handler{
		hub:        hub,
		tokenStore: tokenStore,
		upgrader: websocket.Upgrader{
			CheckOrigin: newOriginChecker(allowedOrigins), // Enforce same-origin or explicit allowlist checks during the WebSocket handshake instead of accepting every browser origin.
		},
	}
}

// ServeHTTP upgrades one authorized HTTP request into a trace-scoped WebSocket subscription.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	claims, err := h.resolveSubscription(r) // Resolve and validate the short-lived subscription token before any WebSocket upgrade occurs.
	if err != nil {                         // Stop immediately when the request is missing or presents an invalid subscription token.
		http.Error(w, err.Error(), errors.MapToHTTP(err)) // Return the mapped auth error so callers can distinguish missing tokens from expired or invalid ones.
		return                                            // Stop before the WebSocket handshake begins because the subscription is not authorized.
	}

	conn, err := h.upgrader.Upgrade(w, r, nil) // Perform the WebSocket upgrade only after the subscription token and origin policy have both been validated.
	if err != nil {
		return
	}
	defer conn.Close()

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
		return nil, err // Preserve the token lookup error so the HTTP layer can map it to the correct auth status.
	}
	requestTraceID := strings.TrimSpace(r.URL.Query().Get("traceId")) // Read the optional traceId query parameter so callers can still include it for debugging or compatibility checks.
	if requestTraceID != "" && requestTraceID != claims.TraceID {     // Reject mismatched trace IDs because the query string must never override the authorized trace binding stored in the token.
		return nil, errors.New(errors.CodePermissionDenied, "traceId does not match subscription token") // Surface mismatched trace bindings as a permission failure instead of subscribing to the wrong trace.
	}

	return claims, nil // Return the authoritative trace binding carried by the subscription token so the handler can open the correct trace stream.
}
