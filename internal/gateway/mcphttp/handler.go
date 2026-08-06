// handler.go implements dual-era MCP Streamable HTTP with stateless 2026-07-28 validation and initialization-based legacy compatibility.
package mcphttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	internalerrors "mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/httpinput"
	"mcp_for_appium/internal/gateway/jsonrpc"
	"mcp_for_appium/internal/gateway/mcp"
)

const (
	protocolVersionHeader = "MCP-Protocol-Version" // protocolVersionHeader is the HTTP header carrying the negotiated MCP protocol version.
	methodHeader          = "Mcp-Method"           // methodHeader mirrors the JSON-RPC method for modern intermediary routing and validation.
	nameHeader            = "Mcp-Name"             // nameHeader mirrors the tool, resource, or prompt identifier for selected modern methods.
	contentTypeJSON       = "application/json"     // contentTypeJSON is the JSON response content type used for one-shot request responses.
	contentTypeSSE        = "text/event-stream"    // contentTypeSSE is the SSE content type clients must advertise for Streamable HTTP.
	base64ValuePrefix     = "=?base64?"            // base64ValuePrefix starts the exact modern MCP sentinel for unsafe HTTP header values.
	base64ValueSuffix     = "?="                   // base64ValueSuffix ends the exact modern MCP sentinel for unsafe HTTP header values.
)

// Handler serves the standard MCP Streamable HTTP endpoint while delegating protocol methods to jsonrpc.Handler.
type Handler struct {
	rpc            *jsonrpc.Handler
	allowedOrigins []string
}

// wireMessage captures just enough JSON-RPC envelope metadata to classify Streamable HTTP inputs before dispatch.
type wireMessage struct {
	JSONRPC   string          `json:"jsonrpc"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params"`
	ID        interface{}     `json:"id"`
	Result    json.RawMessage `json:"result"`
	Error     json.RawMessage `json:"error"`
	IDPresent bool            `json:"-"` // IDPresent distinguishes an omitted notification id from an explicitly invalid null id.
}

// UnmarshalJSON decodes one Streamable HTTP envelope while retaining whether its top-level id member was present on the wire.
func (m *wireMessage) UnmarshalJSON(data []byte) error {
	type wireMessageAlias wireMessage                 // Disable recursive UnmarshalJSON calls while preserving every wire field tag.
	var decoded wireMessageAlias                      // Hold ordinarily decoded request, notification, or response fields.
	decoder := json.NewDecoder(bytes.NewReader(data)) // Decode the exact wire envelope while controlling generic number representation.
	decoder.UseNumber()                               // Preserve arbitrary-size JSON-RPC numeric ids for validation, dispatch, and exact response echoing.
	if err := decoder.Decode(&decoded); err != nil {  // Decode the complete lightweight JSON-RPC envelope first.
		return err // Preserve malformed JSON diagnostics for the transport's parse-error response.
	}
	var fields map[string]json.RawMessage                 // Decode top-level members separately so JSON null remains distinguishable from omission.
	if err := json.Unmarshal(data, &fields); err != nil { // Require an object-shaped envelope before transport classification.
		return err // Preserve the object decoding failure for the transport's parse-error response.
	}
	*m = wireMessage(decoded)     // Copy all decoded wire fields into the target message.
	_, m.IDPresent = fields["id"] // Record exact id-member presence for downstream request classification.
	return nil                    // Confirm that values and wire-presence metadata were decoded successfully.
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
		h.handleGet(w) // Reject GET because MCP 2026-07-28 removed standalone Streamable HTTP listening streams.
	default:
		w.Header().Set("Allow", "POST")                                  // Advertise only POST because the modern transport removed GET.
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed) // Reject methods outside the Streamable HTTP transport surface.
	}
}

// handleGet rejects standalone HTTP streams because MCP 2026-07-28 permits client traffic only through POST on this endpoint.
func (h *Handler) handleGet(w http.ResponseWriter) {
	w.Header().Set("Allow", "POST")                                  // Tell modern and legacy clients that request-scoped POST is the only supported operation.
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed) // Return the mandatory method rejection without opening a legacy GET stream.
}

// handlePost accepts one JSON-RPC message, selects modern or legacy semantics from that message, and enforces the corresponding Streamable HTTP contract.
func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	if !acceptsContentType(r.Header.Get("Accept"), contentTypeJSON) || !acceptsContentType(r.Header.Get("Accept"), contentTypeSSE) { // Require both response formats listed by the Streamable HTTP POST contract.
		http.Error(w, "missing MCP Streamable HTTP accept headers", http.StatusNotAcceptable) // Return 406 so clients can fix the Accept header before retrying.
		return                                                                                // Stop request processing because content negotiation failed.
	}
	if !hasJSONContentType(r.Header.Get("Content-Type")) { // Require the request body media type mandated by both modern and legacy Streamable HTTP revisions.
		h.writeError(w, http.StatusUnsupportedMediaType, nil, -32600, "Invalid Request", "Content-Type must be application/json", "") // Return a JSON-RPC diagnostic alongside HTTP 415 for an unsupported request representation.
		return                                                                                                                        // Stop before decoding a body whose representation was not declared as JSON.
	}

	if r.ContentLength > httpinput.MaxRequestBodyBytes { // Reject a declared oversized body before allocating a decoder or reading any payload bytes.
		h.writeError(w, http.StatusRequestEntityTooLarge, nil, -32600, "Invalid Request", "request body too large", "") // Return HTTP 413 with a sanitized JSON-RPC diagnostic.
		return                                                                                                          // Stop request processing after the declared-size rejection.
	}
	r.Body = http.MaxBytesReader(w, r.Body, httpinput.MaxRequestBodyBytes) // Enforce the transport ceiling for unknown-length or streaming bodies.
	var msg wireMessage                                                    // Allocate the lightweight envelope used to classify the incoming JSON-RPC message.
	if err := httpinput.DecodeSingleJSON(r.Body, &msg); err != nil {       // Decode exactly one bounded JSON-RPC message and reject concatenated documents.
		if httpinput.IsBodyTooLarge(err) { // Map the typed request-size sentinel before ordinary JSON parse failures.
			h.writeError(w, http.StatusRequestEntityTooLarge, nil, -32600, "Invalid Request", "request body too large", "") // Return HTTP 413 with a sanitized JSON-RPC diagnostic.
			return                                                                                                          // Stop request processing after the request-size rejection.
		}
		h.writeError(w, http.StatusBadRequest, nil, -32700, "Parse error", err.Error(), "") // Return a JSON-RPC parse error with HTTP 400 for malformed or trailing JSON.
		return                                                                              // Stop request processing after the parse failure.
	}
	if msg.JSONRPC != "2.0" { // Reject messages that do not use JSON-RPC 2.0 before dispatching protocol methods.
		h.writeError(w, http.StatusBadRequest, msg.ID, -32600, "Invalid Request", "jsonrpc must be 2.0", "") // Return an invalid-request envelope so clients see the precise protocol error.
		return                                                                                               // Stop request processing after the invalid JSON-RPC version.
	}

	metadata, err := mcp.ParseRequestMetadata(msg.Params) // Inspect request-local protocol fields before selecting one of the dual-era transport contracts.
	if err != nil {                                       // Reject malformed params._meta before validating or trusting mirrored HTTP headers.
		h.writeHandlerError(w, http.StatusBadRequest, msg.ID, err, mcp.CurrentProtocolVersion) // Return the parser's required invalid-params error as a modern HTTP 400 response.
		return                                                                                 // Stop after rejecting malformed request metadata.
	}
	headerVersion := strings.TrimSpace(r.Header.Get(protocolVersionHeader))                                                    // Normalize optional whitespace before comparing the transport protocol version.
	modern := metadata.ProtocolFieldsPresent || msg.Method == "server/discover" || headerVersion == mcp.CurrentProtocolVersion // Select stateless semantics from modern namespaced fields, discovery, or the current header without misclassifying legacy progress metadata.
	responseVersion := headerVersion                                                                                           // Echo a valid negotiated legacy revision by default.
	if modern {                                                                                                                // Identify modern responses using the revision this server actually implements.
		responseVersion = mcp.CurrentProtocolVersion // Avoid echoing an unsupported requested revision as though the server implemented it.
	}
	if msg.Method == "initialize" && responseVersion == "" { // Preserve a protocol response header for legacy handshake clients that have not selected a version yet.
		responseVersion = mcp.LegacyProtocolVersion // Use the historical fallback until the initialize result supplies the final negotiated version.
	}

	if isJSONRPCResponse(msg) { // Classify client response envelopes before request-header validation because they have no method to mirror.
		if modern { // Reject client responses because the modern Streamable HTTP binding permits only client requests and notifications.
			h.writeError(w, http.StatusBadRequest, msg.ID, -32600, "Invalid Request", "clients must not send JSON-RPC responses over modern Streamable HTTP", responseVersion) // Return an explicit modern transport violation.
			return                                                                                                                                                             // Stop without dispatching the methodless response envelope.
		}
		w.WriteHeader(http.StatusAccepted) // Preserve legacy acceptance for clients using the initialization-based Streamable HTTP contract.
		return                             // Stop request processing because legacy client responses do not produce server responses.
	}
	req := &jsonrpc.Request{JSONRPC: msg.JSONRPC, Method: msg.Method, Params: msg.Params, ID: msg.ID, IDPresent: msg.IDPresent} // Convert values and id presence into the shared JSON-RPC request type used by existing dispatch.
	if modern && jsonrpc.IsNotification(req) {                                                                                  // Reject modern notifications before header validation because current Streamable HTTP defines no notification request headers.
		h.writeError(w, http.StatusBadRequest, nil, -32600, "Invalid Request", "modern Streamable HTTP does not accept client notifications", responseVersion) // Return the allowed no-id error body with an explicit transport diagnostic.
		return                                                                                                                                                 // Stop before header validation or dispatch so HTTP cancellation cannot diverge from response-stream closure semantics.
	}
	if modern { // Enforce mirrored headers only for the 2026-07-28 per-request metadata era.
		if err := validateModernHeaders(r, msg, metadata); err != nil { // Compare every required standard header against the decoded JSON-RPC body.
			h.writeHandlerError(w, http.StatusBadRequest, msg.ID, err, responseVersion) // Return HTTP 400 and the protocol-reserved HeaderMismatch error.
			return                                                                      // Stop before dispatch so intermediaries and the application cannot act on different request identities.
		}
		if err := mcp.ValidateModernRequestMetadata(metadata); err != nil { // Require body-local version and capabilities even when the current HTTP header selected modern semantics.
			h.writeHandlerError(w, http.StatusBadRequest, msg.ID, err, responseVersion) // Return malformed metadata or retryable UnsupportedProtocolVersion errors as HTTP 400.
			return                                                                      // Stop before shared dispatch so header-only requests cannot fall through to legacy methods.
		}
	} else if msg.Method != "initialize" && !validLegacyProtocolHeader(headerVersion) { // Require one supported negotiated version after the legacy initialize handshake.
		h.writeError(w, http.StatusBadRequest, msg.ID, -32600, "Invalid Request", "unsupported MCP protocol version", responseVersion) // Preserve the existing legacy negotiation diagnostic.
		return                                                                                                                         // Stop request processing after the legacy version-header rejection.
	}

	if jsonrpc.IsNotification(req) { // Apply notification semantics before request response handling.
		h.handleNotification(w, r, req, responseVersion) // Process the notification and return the Streamable HTTP accepted response shape.
		return                                           // Stop request processing because notifications never produce JSON-RPC response bodies.
	}
	if !jsonrpc.IsRequest(req) { // Reject methodless non-response messages because they are not valid JSON-RPC requests or notifications.
		h.writeError(w, http.StatusBadRequest, msg.ID, -32600, "Invalid Request", "method or response payload is required", responseVersion) // Return a JSON-RPC invalid-request envelope for malformed envelopes.
		return                                                                                                                               // Stop request processing after the invalid envelope.
	}

	h.handleRequest(w, r, req, modern, responseVersion) // Dispatch the request and apply era-specific HTTP status and response-version semantics.
}

// handleNotification dispatches one JSON-RPC notification and returns an empty 202 response when its side effects are accepted.
func (h *Handler) handleNotification(w http.ResponseWriter, r *http.Request, req *jsonrpc.Request, protocolVersion string) {
	if _, err := h.rpc.ProcessRequest(r.Context(), req); err != nil { // Dispatch the notification so initialized/cancelled side effects are applied.
		h.writeHandlerError(w, http.StatusBadRequest, nil, err, protocolVersion) // Return a no-id JSON-RPC error when the notification cannot be accepted.
		return                                                                   // Stop request processing after reporting the notification acceptance failure.
	}

	w.Header().Set(protocolVersionHeader, protocolVersion) // Identify the accepted protocol revision without emitting a JSON-RPC notification response body.
	w.WriteHeader(http.StatusAccepted)                     // Return 202 with no body for accepted notifications per Streamable HTTP semantics.
}

// handleRequest dispatches one JSON-RPC request and maps modern protocol errors to their required HTTP status while preserving legacy HTTP 200 envelopes.
func (h *Handler) handleRequest(w http.ResponseWriter, r *http.Request, req *jsonrpc.Request, modern bool, protocolVersion string) {
	ctx := r.Context()                                       // Start from the HTTP request context so disconnects and shutdown still cancel request work.
	requestCtx, cancel := context.WithCancel(ctx)            // Create one cancellable child context for notifications/cancelled.
	cleanup := h.rpc.RegisterInFlightRequest(req.ID, cancel) // Register the request id before dispatch so concurrent cancellation notifications can find it.
	defer cleanup()                                          // Remove the in-flight request when dispatch completes.
	defer cancel()                                           // Release child context resources after dispatch.

	result, err := h.rpc.ProcessRequest(requestCtx, req) // Dispatch the request through the shared JSON-RPC handler.
	if err != nil {                                      // Convert handler errors into JSON-RPC error responses.
		h.writeHandlerError(w, handlerErrorStatus(err, modern), req.ID, err, protocolVersion) // Apply current MCP transport statuses without changing legacy JSON-RPC-over-HTTP behavior.
		return                                                                                // Stop request processing after writing the JSON-RPC error response.
	}

	protocolVersion = negotiatedResponseVersion(result, protocolVersion)                                             // Read the selected revision from a successful legacy initialize result when present.
	h.writeResponse(w, http.StatusOK, jsonrpc.Response{JSONRPC: "2.0", Result: result, ID: req.ID}, protocolVersion) // Write the successful JSON-RPC response envelope with the actual modern or negotiated legacy version.
}

// writeHandlerError preserves typed MCP errors, maps repository errors, and writes one JSON-RPC envelope with the selected protocol response version.
func (h *Handler) writeHandlerError(w http.ResponseWriter, status int, id interface{}, err error, protocolVersion string) {
	if mcpErr, ok := err.(*mcp.MCPError); ok { // Preserve MCP protocol errors without remapping their JSON-RPC code or data.
		h.writeResponse(w, status, jsonrpc.Response{JSONRPC: "2.0", Error: &internalerrors.JSONRPCError{Code: mcpErr.Code, Message: mcpErr.Message, Data: mcpErr.Data}, ID: id}, protocolVersion) // Write the MCP error envelope with the caller-selected HTTP status and revision.
		return                                                                                                                                                                                    // Stop after writing the typed MCP error response.
	}

	h.writeResponse(w, status, jsonrpc.Response{JSONRPC: "2.0", Error: internalerrors.MapToJSONRPC(err), ID: id}, protocolVersion) // Map repository errors into JSON-RPC error envelopes consistently with the compatibility transport.
}

// writeError writes one direct JSON-RPC error envelope with explicit transport status, code, diagnostic data, and protocol response version.
func (h *Handler) writeError(w http.ResponseWriter, status int, id interface{}, code int, message string, data interface{}, protocolVersion string) {
	h.writeResponse(w, status, jsonrpc.Response{JSONRPC: "2.0", Error: &internalerrors.JSONRPCError{Code: code, Message: message, Data: data}, ID: id}, protocolVersion) // Write the direct protocol error response with era-appropriate headers.
}

// writeResponse serializes one JSON-RPC response as application/json and includes a protocol header only when the request era is known.
func (h *Handler) writeResponse(w http.ResponseWriter, status int, response jsonrpc.Response, protocolVersion string) {
	w.Header().Set("Content-Type", contentTypeJSON) // Mark the response as a one-shot JSON response rather than an SSE stream.
	if protocolVersion != "" {                      // Avoid inventing an era for parse errors received before any version could be identified.
		w.Header().Set(protocolVersionHeader, protocolVersion) // Echo the implemented or negotiated revision for validly classified MCP exchanges.
	}
	w.WriteHeader(status)                   // Write the transport status before encoding the JSON-RPC envelope.
	_ = json.NewEncoder(w).Encode(response) // Encode the response body while ignoring late write failures because net/http has already accepted the response.
}

// validLegacyProtocolHeader reports whether one already-normalized header names a supported initialization-based MCP revision.
func validLegacyProtocolHeader(protocolVersion string) bool {
	return protocolVersion == mcp.LatestLegacyProtocolVersion || protocolVersion == mcp.LegacyProtocolVersion // Accept both the latest legacy revision and the repository's previous target.
}

// validateModernHeaders compares every required standard MCP 2026-07-28 HTTP header with its authoritative JSON-RPC body field.
func validateModernHeaders(r *http.Request, msg wireMessage, metadata mcp.RequestMetadata) error {
	protocolVersion := strings.TrimSpace(r.Header.Get(protocolVersionHeader)) // Read the mirrored protocol version while tolerating transport-level optional whitespace.
	if protocolVersion == "" {                                                // Reject omission before any intermediary can infer a protocol era from incomplete headers.
		return newHeaderMismatchError(protocolVersionHeader + " header is required") // Use the specification-reserved error for every missing standard header.
	}
	if metadata.ProtocolVersion != "" && protocolVersion != metadata.ProtocolVersion { // Compare the transport version with the request-local metadata version when the body field exists.
		return newHeaderMismatchError(protocolVersionHeader + " header does not match params._meta protocol version") // Prevent routing and application layers from selecting different protocol revisions.
	}

	requestMethod := r.Header.Get(methodHeader) // Read the exact case-sensitive method value mirrored for intermediaries.
	if requestMethod == "" {                    // Reject a missing method header on every modern request.
		return newHeaderMismatchError(methodHeader + " header is required") // Name the absent standard header for direct client remediation.
	}
	if requestMethod != msg.Method { // Require byte-for-byte method agreement because MCP method names are case-sensitive.
		return newHeaderMismatchError(methodHeader + " header value does not match JSON-RPC method") // Reject ambiguous routing identities before dispatch.
	}

	targetName, required, err := requestTargetName(msg.Method, msg.Params) // Extract params.name or params.uri only for methods that require Mcp-Name.
	if err != nil {                                                        // Convert missing or malformed body targets into the same header-validation error family.
		return err // Preserve the precise HeaderMismatch diagnostic created by the target extractor.
	}
	if !required { // Complete validation immediately for methods whose identity is fully represented by Mcp-Method.
		return nil // Confirm that all standard headers applicable to this request match its body.
	}
	rawHeaderName := r.Header.Get(nameHeader) // Read the potentially sentinel-encoded target identity from the standard header.
	if rawHeaderName == "" {                  // Reject omission for tools/call, resources/read, and prompts/get.
		return newHeaderMismatchError(nameHeader + " header is required for " + msg.Method) // Identify both the missing header and method-specific requirement.
	}
	decodedHeaderName, err := decodeMCPHeaderValue(rawHeaderName) // Decode the exact Base64 sentinel form before comparing non-ASCII or whitespace-sensitive values.
	if err != nil {                                               // Reject malformed encodings and unsafe plain header bytes.
		return newHeaderMismatchError(nameHeader + " header is malformed") // Avoid reflecting attacker-controlled header bytes while identifying the failing field.
	}
	if decodedHeaderName != targetName { // Compare the decoded header identity with the authoritative body value exactly.
		return newHeaderMismatchError(nameHeader + " header value does not match request params") // Prevent intermediaries and tool/resource dispatch from using different names.
	}
	return nil // Confirm that all modern standard HTTP headers are present, safe, and body-consistent.
}

// requestTargetName returns the body identifier mirrored by Mcp-Name and reports whether the current method requires that standard header.
func requestTargetName(method string, params json.RawMessage) (string, bool, error) {
	parameterName := "" // Start without a mirrored parameter for methods that require only Mcp-Method.
	switch method {     // Select the exact body field defined by the modern standard-header table.
	case "tools/call", "prompts/get":
		parameterName = "name" // Mirror the tool or prompt name for intermediary routing and policy checks.
	case "resources/read":
		parameterName = "uri" // Mirror the resource URI under the shared Mcp-Name HTTP header.
	default:
		return "", false, nil // Report that this method has no standard target-name header requirement.
	}

	var paramsObject map[string]json.RawMessage                                          // Decode only the selected target field from the JSON-RPC params object.
	if err := json.Unmarshal(params, &paramsObject); err != nil || paramsObject == nil { // Require method parameters to be an object before extracting a mirrored identity.
		return "", true, newHeaderMismatchError("request params must contain " + parameterName + " for " + method) // Reject a body that cannot supply the required comparison source.
	}
	rawTarget, exists := paramsObject[parameterName] // Read the authoritative body target selected for this method.
	if !exists {                                     // Reject omission because the transport cannot validate a required Mcp-Name header without its body source.
		return "", true, newHeaderMismatchError("request params must contain " + parameterName + " for " + method) // Name the missing target parameter and method.
	}
	var target string                                                          // Decode the mirrored identity as the string type required by all three standard methods.
	if err := json.Unmarshal(rawTarget, &target); err != nil || target == "" { // Reject null, non-string, and empty target identities.
		return "", true, newHeaderMismatchError("request params " + parameterName + " must be a non-empty string") // Return a stable body/header validation diagnostic.
	}
	return target, true, nil // Return the exact authoritative value for decoded Mcp-Name comparison.
}

// decodeMCPHeaderValue validates a plain safe ASCII value or decodes the exact =?base64?...?= sentinel into UTF-8 text.
func decodeMCPHeaderValue(value string) (string, error) {
	hasPrefix := strings.HasPrefix(value, base64ValuePrefix) // Detect the exact case-sensitive opening sentinel defined by MCP.
	hasSuffix := strings.HasSuffix(value, base64ValueSuffix) // Detect the exact case-sensitive closing sentinel defined by MCP.
	if hasPrefix && hasSuffix {                              // Decode only the complete sentinel pattern because a single marker remains unambiguous plain ASCII.
		encodedValue := strings.TrimSuffix(strings.TrimPrefix(value, base64ValuePrefix), base64ValueSuffix) // Remove only the exact outer sentinel markers.
		decodedValue, err := base64.StdEncoding.DecodeString(encodedValue)                                  // Decode standard padded Base64 bytes from the sentinel payload.
		if err != nil || !utf8.Valid(decodedValue) {                                                        // Require both valid Base64 syntax and valid UTF-8 request text.
			return "", newHeaderMismatchError("invalid Base64 or UTF-8 header value") // Reject malformed encoded identities before comparison.
		}
		return string(decodedValue), nil // Return the original Unicode or whitespace-sensitive body value represented by the header.
	}

	if strings.TrimSpace(value) != value { // Require values with leading or trailing whitespace to use sentinel encoding.
		return "", newHeaderMismatchError("unsafe plain header whitespace") // Reject ambiguous whitespace that HTTP implementations may normalize differently.
	}
	for index := 0; index < len(value); index++ { // Inspect every byte because compliant plain values are restricted to ASCII HTTP field content.
		currentByte := value[index] // Read one byte without decoding runes because any non-ASCII UTF-8 byte is forbidden in plain form.
		if currentByte == '\t' {    // Permit internal horizontal tab as allowed by the modern MCP value-encoding rules.
			continue // Skip the ordinary visible-ASCII range check for this one permitted control byte.
		}
		if currentByte < 0x20 || currentByte > 0x7e { // Reject controls, DEL, and every non-ASCII byte in unencoded header values.
			return "", newHeaderMismatchError("unsafe plain header characters") // Require sentinel encoding for values outside the safe HTTP field range.
		}
	}
	return value, nil // Return a header-safe plain ASCII value unchanged for exact body comparison.
}

// newHeaderMismatchError creates the protocol-reserved modern error used for missing, malformed, or body-inconsistent HTTP headers.
func newHeaderMismatchError(detail string) *mcp.MCPError {
	return &mcp.MCPError{Code: mcp.ErrorCodeHeaderMismatch, Message: "Header mismatch: " + detail} // Return the canonical error family without duplicating attacker-controlled data fields.
}

// handlerErrorStatus maps modern method, metadata, capability, and version errors to required HTTP statuses while retaining legacy HTTP 200 behavior.
func handlerErrorStatus(err error, modern bool) int {
	if !modern { // Preserve initialization-based Streamable HTTP behavior for existing clients.
		return http.StatusOK // Carry legacy method errors inside a normal JSON-RPC HTTP response envelope.
	}
	mcpErr, ok := err.(*mcp.MCPError) // Inspect only typed protocol errors because repository business errors retain ordinary JSON-RPC HTTP handling.
	if !ok {                          // Avoid assigning protocol statuses to unrelated application failures.
		return http.StatusOK // Return the business error in a valid modern JSON-RPC response envelope.
	}
	switch mcpErr.Code { // Apply the explicit transport status requirements defined by MCP 2026-07-28.
	case -32601:
		return http.StatusNotFound // Distinguish an unimplemented modern RPC from a missing legacy endpoint.
	case -32600, -32602, mcp.ErrorCodeHeaderMismatch, -32021, mcp.ErrorCodeUnsupportedVersion:
		return http.StatusBadRequest // Reject malformed requests, missing capabilities, header mismatches, and unsupported revisions as HTTP 400.
	default:
		return http.StatusOK // Preserve JSON-RPC envelope semantics for execution and application errors without mandated HTTP mappings.
	}
}

// negotiatedResponseVersion reads a supported legacy initialize version from a successful result and otherwise preserves the supplied fallback revision.
func negotiatedResponseVersion(result interface{}, fallback string) string {
	resultObject, ok := result.(map[string]interface{}) // Initialize returns a map before any JSON serialization transforms its values.
	if !ok {                                            // Leave ordinary modern and legacy method responses on the request-selected revision.
		return fallback // Return the transport's previously classified protocol version unchanged.
	}
	protocolVersion, ok := resultObject["protocolVersion"].(string) // Read the selected revision emitted only by the legacy initialize handler.
	if !ok || !validLegacyProtocolHeader(protocolVersion) {         // Never echo arbitrary result data as a negotiated protocol revision.
		return fallback // Preserve the trusted request-era fallback when initialize metadata is absent or invalid.
	}
	return protocolVersion // Echo the exact mutually supported legacy revision selected by initialization.
}

// isJSONRPCResponse reports whether the envelope is a client response message rather than a request or notification.
func isJSONRPCResponse(msg wireMessage) bool {
	return msg.Method == "" && (msg.Result != nil || msg.Error != nil) // Classify methodless envelopes with result or error as JSON-RPC responses.
}

// acceptsContentType reports whether an HTTP Accept header explicitly allows one required content type.
func acceptsContentType(header string, required string) bool {
	for _, part := range strings.Split(header, ",") { // Walk each comma-separated Accept value because clients can list multiple media ranges.
		mediaType, parameters, err := mime.ParseMediaType(strings.TrimSpace(part)) // Parse media parameters so an explicit q=0 is not mistaken for client support.
		if err != nil || !strings.EqualFold(mediaType, required) {                 // Ignore malformed entries and unrelated media types while scanning the list.
			continue // Continue because a later Accept entry may validly advertise the required response representation.
		}
		if rawQuality, present := parameters["q"]; present { // Evaluate the optional HTTP quality weight when the client supplies one.
			quality, err := strconv.ParseFloat(rawQuality, 64) // Parse the quality value as the HTTP decimal range represented by Go floating point.
			if err != nil || quality <= 0 || quality > 1 {     // Reject malformed, explicitly unacceptable, and out-of-range quality values.
				continue // Keep searching in case another entry advertises the same required type with a positive valid weight.
			}
		}
		return true // Report support once the required type has no quality restriction or a positive valid quality.
	}
	return false // Report that the required media type was not explicitly advertised.
}

// hasJSONContentType reports whether a Content-Type header declares application/json with only syntactically valid optional parameters.
func hasJSONContentType(header string) bool {
	mediaType, _, err := mime.ParseMediaType(header) // Parse media-type parameters rather than relying on unsafe prefix matching.
	if err != nil {                                  // Reject absent and malformed Content-Type values before body decoding.
		return false // Report that the request does not provide a trustworthy JSON representation declaration.
	}
	return strings.EqualFold(mediaType, contentTypeJSON) // Accept application/json case-insensitively as required for HTTP media types.
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
