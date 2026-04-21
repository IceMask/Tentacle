// handler.go implements the MCP 2025-06-18 Streamable HTTP transport endpoint over the shared JSON-RPC dispatcher.
package mcphttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	internalerrors "mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/jsonrpc"
	"mcp_for_appium/internal/gateway/mcp"
)

const (
	protocolVersionHeader = "MCP-Protocol-Version" // protocolVersionHeader is the HTTP header carrying the negotiated MCP protocol version.
	contentTypeJSON       = "application/json"     // contentTypeJSON is the JSON response content type used for one-shot request responses.
	contentTypeSSE        = "text/event-stream"    // contentTypeSSE is the SSE content type clients must advertise for Streamable HTTP.
)

// Handler serves the standard MCP Streamable HTTP endpoint while delegating protocol methods to jsonrpc.Handler.
type Handler struct {
	rpc            *jsonrpc.Handler
	allowedOrigins []string
}

// wireMessage captures just enough JSON-RPC envelope metadata to classify Streamable HTTP inputs before dispatch.
type wireMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

// NewHandler constructs one Streamable HTTP handler over an existing JSON-RPC handler and a comma-separated browser origin allowlist.
func NewHandler(rpcHandler *jsonrpc.Handler, rawAllowedOrigins string) *Handler {
	return &Handler{
		rpc:            rpcHandler,                             // Reuse the caller-provided JSON-RPC handler so /mcp and /jsonrpc share dispatch and cancellation state.
		allowedOrigins: parseAllowedOrigins(rawAllowedOrigins), // Normalize configured origins once so per-request checks are exact and cheap.
	}
}

// ServeHTTP routes Streamable HTTP requests by method after applying the transport-level Origin defense.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.originAllowed(r) { // Enforce browser Origin checks before reading request bodies to reduce DNS rebinding exposure.
		http.Error(w, "forbidden origin", http.StatusForbidden) // Return a plain HTTP 403 because this failure happens before JSON-RPC message acceptance.
		return                                                  // Stop request processing after rejecting the origin.
	}

	switch r.Method { // Route the single MCP endpoint by HTTP method as required by Streamable HTTP.
	case http.MethodPost:
		h.handlePost(w, r) // Process one client-to-server JSON-RPC message sent as a POST body.
	case http.MethodGet:
		h.handleGet(w, r) // Report whether this server offers standalone SSE listening streams.
	default:
		w.Header().Set("Allow", "GET, POST")                             // Advertise the two methods defined for the MCP endpoint.
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed) // Reject methods outside the Streamable HTTP transport surface.
	}
}

// handleGet rejects standalone SSE streams because this server currently emits server-to-client messages only on request-scoped transports.
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	if !acceptsContentType(r.Header.Get("Accept"), contentTypeSSE) { // Require clients to advertise SSE support before considering a listening stream.
		http.Error(w, "missing text/event-stream accept header", http.StatusNotAcceptable) // Return 406 when the client cannot accept the only valid GET success content type.
		return                                                                             // Stop request processing because the GET precondition failed.
	}
	if !h.validProtocolHeader(r) { // Require the negotiated protocol version on GET because GET happens after initialization.
		http.Error(w, "unsupported MCP protocol version", http.StatusBadRequest) // Return 400 for missing or unsupported MCP protocol versions.
		return                                                                   // Stop request processing after protocol-version rejection.
	}

	w.Header().Set("Allow", "POST")                                        // Advertise POST as the supported operation because standalone SSE streams are intentionally unavailable.
	http.Error(w, "SSE stream not available", http.StatusMethodNotAllowed) // Return 405 as allowed by the Streamable HTTP specification when no GET SSE stream is offered.
}

// handlePost accepts exactly one JSON-RPC message and applies Streamable HTTP response semantics for requests, notifications, and responses.
func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	if !acceptsContentType(r.Header.Get("Accept"), contentTypeJSON) || !acceptsContentType(r.Header.Get("Accept"), contentTypeSSE) { // Require both response formats listed by the Streamable HTTP POST contract.
		http.Error(w, "missing MCP Streamable HTTP accept headers", http.StatusNotAcceptable) // Return 406 so clients can fix the Accept header before retrying.
		return                                                                                // Stop request processing because content negotiation failed.
	}

	var msg wireMessage                                          // Allocate the lightweight envelope used to classify the incoming JSON-RPC message.
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil { // Decode the POST body as exactly one JSON-RPC message envelope.
		h.writeError(w, http.StatusBadRequest, nil, -32700, "Parse error", err.Error()) // Return a JSON-RPC parse error with HTTP 400 for malformed JSON bodies.
		return                                                                          // Stop request processing after the parse failure.
	}
	if msg.JSONRPC != "2.0" { // Reject messages that do not use JSON-RPC 2.0 before dispatching protocol methods.
		h.writeError(w, http.StatusBadRequest, msg.ID, -32600, "Invalid Request", "jsonrpc must be 2.0") // Return an invalid-request envelope so clients see the precise protocol error.
		return                                                                                           // Stop request processing after the invalid JSON-RPC version.
	}
	if msg.Method != "initialize" && !h.validProtocolHeader(r) { // Require MCP-Protocol-Version for every post-initialize HTTP message.
		h.writeError(w, http.StatusBadRequest, msg.ID, -32600, "Invalid Request", "unsupported MCP protocol version") // Return a JSON-RPC invalid-request envelope for version negotiation failures.
		return                                                                                                        // Stop request processing after the protocol-version rejection.
	}
	if isJSONRPCResponse(msg) { // Accept client responses without dispatch because this server does not initiate HTTP client requests yet.
		w.WriteHeader(http.StatusAccepted) // Return 202 with no body for accepted JSON-RPC responses per Streamable HTTP semantics.
		return                             // Stop request processing because responses do not produce server responses.
	}

	req := &jsonrpc.Request{JSONRPC: msg.JSONRPC, Method: msg.Method, Params: msg.Params, ID: msg.ID} // Convert the envelope into the shared JSON-RPC request type used by existing dispatch.
	if jsonrpc.IsNotification(req) {                                                                  // Apply notification semantics before request response handling.
		h.handleNotification(w, r, req) // Process the notification and return the Streamable HTTP accepted response shape.
		return                          // Stop request processing because notifications never produce JSON-RPC response bodies.
	}
	if !jsonrpc.IsRequest(req) { // Reject methodless non-response messages because they are not valid JSON-RPC requests or notifications.
		h.writeError(w, http.StatusBadRequest, msg.ID, -32600, "Invalid Request", "method or response payload is required") // Return a JSON-RPC invalid-request envelope for malformed envelopes.
		return                                                                                                              // Stop request processing after the invalid envelope.
	}

	h.handleRequest(w, r, req) // Dispatch the request and write one application/json JSON-RPC response.
}

// handleNotification dispatches one JSON-RPC notification and returns 202 Accepted when the server accepts it.
func (h *Handler) handleNotification(w http.ResponseWriter, r *http.Request, req *jsonrpc.Request) {
	if _, err := h.rpc.ProcessRequest(r.Context(), req); err != nil { // Dispatch the notification so initialized/cancelled side effects are applied.
		h.writeHandlerError(w, http.StatusBadRequest, nil, err) // Return a no-id JSON-RPC error when the notification cannot be accepted.
		return                                                  // Stop request processing after reporting the notification acceptance failure.
	}

	w.WriteHeader(http.StatusAccepted) // Return 202 with no body for accepted notifications per Streamable HTTP semantics.
}

// handleRequest dispatches one JSON-RPC request and writes exactly one JSON-RPC response body.
func (h *Handler) handleRequest(w http.ResponseWriter, r *http.Request, req *jsonrpc.Request) {
	ctx := r.Context()                                       // Start from the HTTP request context so disconnects and shutdown still cancel request work.
	requestCtx, cancel := context.WithCancel(ctx)            // Create one cancellable child context for notifications/cancelled.
	cleanup := h.rpc.RegisterInFlightRequest(req.ID, cancel) // Register the request id before dispatch so concurrent cancellation notifications can find it.
	defer cleanup()                                          // Remove the in-flight request when dispatch completes.
	defer cancel()                                           // Release child context resources after dispatch.

	result, err := h.rpc.ProcessRequest(requestCtx, req) // Dispatch the request through the shared JSON-RPC handler.
	if err != nil {                                      // Convert handler errors into JSON-RPC error responses.
		h.writeHandlerError(w, http.StatusOK, req.ID, err) // Preserve JSON-RPC-over-HTTP request semantics by returning the error envelope with HTTP 200.
		return                                             // Stop request processing after writing the JSON-RPC error response.
	}

	w.Header().Set(protocolVersionHeader, mcp.SupportedProtocolVersion)                             // Echo the accepted MCP protocol version so clients can keep using it on subsequent requests.
	h.writeResponse(w, http.StatusOK, jsonrpc.Response{JSONRPC: "2.0", Result: result, ID: req.ID}) // Write the successful JSON-RPC response envelope as application/json.
}

// writeHandlerError maps one handler error into a JSON-RPC error envelope and writes it with the requested HTTP status.
func (h *Handler) writeHandlerError(w http.ResponseWriter, status int, id interface{}, err error) {
	if mcpErr, ok := err.(*mcp.MCPError); ok { // Preserve MCP protocol errors without remapping their JSON-RPC code or data.
		h.writeResponse(w, status, jsonrpc.Response{JSONRPC: "2.0", Error: &internalerrors.JSONRPCError{Code: mcpErr.Code, Message: mcpErr.Message, Data: mcpErr.Data}, ID: id}) // Write the MCP error envelope with the caller-selected HTTP status.
		return                                                                                                                                                                   // Stop after writing the typed MCP error response.
	}

	h.writeResponse(w, status, jsonrpc.Response{JSONRPC: "2.0", Error: internalerrors.MapToJSONRPC(err), ID: id}) // Map repository errors into JSON-RPC error envelopes consistently with the compatibility transport.
}

// writeError writes one JSON-RPC error envelope with an explicit error code, message, and optional data payload.
func (h *Handler) writeError(w http.ResponseWriter, status int, id interface{}, code int, message string, data interface{}) {
	h.writeResponse(w, status, jsonrpc.Response{JSONRPC: "2.0", Error: &internalerrors.JSONRPCError{Code: code, Message: message, Data: data}, ID: id}) // Write the direct protocol error response.
}

// writeResponse serializes one JSON-RPC response object as application/json with the supplied HTTP status.
func (h *Handler) writeResponse(w http.ResponseWriter, status int, response jsonrpc.Response) {
	w.Header().Set("Content-Type", contentTypeJSON)                     // Mark the response as a one-shot JSON response rather than an SSE stream.
	w.Header().Set(protocolVersionHeader, mcp.SupportedProtocolVersion) // Echo the supported MCP protocol version on every JSON response.
	w.WriteHeader(status)                                               // Write the transport status before encoding the JSON-RPC envelope.
	_ = json.NewEncoder(w).Encode(response)                             // Encode the response body while ignoring late write failures because net/http has already accepted the response.
}

// validProtocolHeader reports whether the request carries the exact MCP protocol version supported by this server.
func (h *Handler) validProtocolHeader(r *http.Request) bool {
	return strings.TrimSpace(r.Header.Get(protocolVersionHeader)) == mcp.SupportedProtocolVersion // Accept only the target protocol version for this alignment batch.
}

// isJSONRPCResponse reports whether the envelope is a client response message rather than a request or notification.
func isJSONRPCResponse(msg wireMessage) bool {
	return msg.Method == "" && (msg.Result != nil || msg.Error != nil) // Classify methodless envelopes with result or error as JSON-RPC responses.
}

// acceptsContentType reports whether an HTTP Accept header explicitly allows one required content type.
func acceptsContentType(header string, required string) bool {
	for _, part := range strings.Split(header, ",") { // Walk each comma-separated Accept value because clients can list multiple media ranges.
		mediaType := strings.TrimSpace(strings.SplitN(part, ";", 2)[0]) // Drop optional parameters like q-values before comparing media types.
		if strings.EqualFold(mediaType, required) {                     // Compare media types case-insensitively as HTTP header values.
			return true // Report success as soon as the required media type is listed.
		}
	}
	return false // Report that the required media type was not explicitly advertised.
}

// parseAllowedOrigins normalizes a comma-separated origin allowlist into canonical scheme://host values.
func parseAllowedOrigins(rawOrigins string) []string {
	allowedOrigins := make([]string, 0)                        // Start with an empty allowlist so same-origin fallback remains active when no config is supplied.
	for _, rawOrigin := range strings.Split(rawOrigins, ",") { // Walk every comma-delimited origin entry from config.
		trimmedOrigin := strings.TrimSpace(rawOrigin) // Remove surrounding whitespace commonly present in YAML or environment lists.
		if trimmedOrigin == "" {                      // Ignore empty entries so accidental extra commas do not create invalid origins.
			continue // Skip blank entries because they cannot match a browser Origin header.
		}
		parsedOrigin, err := url.Parse(trimmedOrigin) // Parse the configured origin before normalizing its scheme and host.
		if err != nil {                               // Ignore malformed entries defensively because startup validation owns operator-facing errors.
			continue // Skip malformed origins rather than panicking during request handling.
		}
		if parsedOrigin.Scheme == "" || parsedOrigin.Host == "" { // Reject partial origins because browser Origin always includes scheme and host.
			continue // Skip incomplete origins because they cannot be compared safely.
		}
		allowedOrigins = append(allowedOrigins, strings.ToLower(parsedOrigin.Scheme+"://"+parsedOrigin.Host)) // Store canonical lowercase origin values for exact matching.
	}
	return allowedOrigins // Return the normalized allowlist used by originAllowed.
}

// originAllowed enforces explicit allowlist or same-origin fallback for browser-originated MCP HTTP requests.
func (h *Handler) originAllowed(r *http.Request) bool {
	originHeader := strings.TrimSpace(r.Header.Get("Origin")) // Read and trim the Origin header once before validation.
	if originHeader == "" {                                   // Permit non-browser clients because CLI MCP clients commonly omit Origin entirely.
		return true // Accept requests without Origin because DNS rebinding protection applies to browser-originated traffic.
	}

	parsedOrigin, err := url.Parse(originHeader) // Parse the browser-supplied Origin header into scheme and host.
	if err != nil {                              // Reject malformed Origin values because they cannot be safely compared.
		return false // Deny malformed browser origins.
	}
	if parsedOrigin.Scheme == "" || parsedOrigin.Host == "" { // Reject partial Origin headers because they cannot establish a concrete authority.
		return false // Deny incomplete browser origins.
	}

	normalizedOrigin := strings.ToLower(parsedOrigin.Scheme + "://" + parsedOrigin.Host) // Normalize the incoming Origin to the same representation as configured allowlist entries.
	if len(h.allowedOrigins) > 0 {                                                       // Prefer explicit allowlist checks when configured by the operator.
		for _, allowedOrigin := range h.allowedOrigins { // Walk the normalized allowlist because it is expected to stay very small.
			if allowedOrigin == normalizedOrigin { // Match exact origin strings so scheme and host both matter.
				return true // Accept explicitly allowed browser origins.
			}
		}
		return false // Deny browser origins that are absent from the explicit allowlist.
	}

	return strings.EqualFold(strings.TrimSpace(r.Host), parsedOrigin.Host) // Fall back to strict same-host validation when no explicit allowlist is configured.
}
