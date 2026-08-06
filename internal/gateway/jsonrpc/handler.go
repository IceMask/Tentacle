package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/big"
	"net/http"
	"sync"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/capabilities"
	"mcp_for_appium/internal/gateway/httpinput"
	"mcp_for_appium/internal/gateway/mcp"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/telemetry"
)

type Handler struct {
	orch             *orchestrator.Service
	mcpHandler       *mcp.MCPHandler
	capService       *capabilities.Service
	validator        *Validator
	inflightMu       sync.Mutex
	inflightRequests map[string]*inflightRequest
}

type inflightRequest struct {
	cancel context.CancelFunc
}

// NewHandler constructs a JSON-RPC protocol handler with MCP dispatch, capability discovery, schema validation, and request cancellation tracking.
func NewHandler(orch *orchestrator.Service, capSvc *capabilities.Service) *Handler {
	adbShellEnabled := true // Default to the historical tool surface when no capability service is supplied.
	if capSvc != nil {      // Read operator-shaped capability flags when discovery service wiring is available.
		adbShellEnabled = capSvc.ADBShellToolEnabled() // Reuse the capability service as the single gateway-level source of truth for adbShell exposure.
	}
	return &Handler{
		orch:             orch,                                                                                           // Preserve the orchestrator dependency for legacy direct methods and MCP tool execution.
		mcpHandler:       mcp.NewMCPHandlerWithConfig(orch, config.GatewayConfig{DisableADBShellTool: !adbShellEnabled}), // Construct the MCP handler with the gateway-shaped tool surface.
		capService:       capSvc,                                                                                         // Preserve the capability service for direct describeCapabilities requests.
		validator:        NewValidator(),                                                                                 // Create the JSON-RPC schema validator used before dispatching schema-covered methods.
		inflightRequests: make(map[string]*inflightRequest),                                                              // Initialize the cancellation registry so transports can register active request ids immediately.
	}
}

type Request struct {
	JSONRPC   string          `json:"jsonrpc"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params"`
	ID        interface{}     `json:"id"`
	IDPresent bool            `json:"-"` // IDPresent distinguishes an omitted notification id from an explicitly invalid JSON null id.
}

type Response struct {
	JSONRPC string               `json:"jsonrpc"`
	Result  interface{}          `json:"result,omitempty"`
	Error   *errors.JSONRPCError `json:"error,omitempty"`
	ID      interface{}          `json:"id"`
}

// UnmarshalJSON decodes one JSON-RPC request while retaining whether the wire envelope explicitly included the id member.
func (r *Request) UnmarshalJSON(data []byte) error {
	type requestAlias Request                         // Disable recursive UnmarshalJSON calls while preserving the request field tags.
	var decoded requestAlias                          // Hold the ordinarily decoded request fields before the id-presence marker is attached.
	decoder := json.NewDecoder(bytes.NewReader(data)) // Decode from the exact custom-unmarshal input while controlling number representation.
	decoder.UseNumber()                               // Preserve numeric request identifiers as their original JSON text instead of lossy float64 values.
	if err := decoder.Decode(&decoded); err != nil {  // Decode the request envelope with precision-preserving numeric semantics.
		return err // Preserve the syntax or field decoding failure for the transport's parse-error response.
	}
	var fields map[string]json.RawMessage                 // Decode top-level members separately so an omitted id can be distinguished from JSON null.
	if err := json.Unmarshal(data, &fields); err != nil { // Reuse JSON object decoding rather than inferring presence from a nil interface value.
		return err // Preserve the object decoding failure for the transport's parse-error response.
	}
	*r = Request(decoded)         // Copy the decoded JSON-RPC fields into the caller's request value.
	_, r.IDPresent = fields["id"] // Record exact wire-level id presence so notification classification can reject explicit null.
	return nil                    // Confirm that both request values and presence metadata were decoded successfully.
}

// IsNotification reports whether one decoded JSON-RPC message is a notification that carries a method and completely omits the id member.
func IsNotification(req *Request) bool {
	return req != nil && req.Method != "" && req.ID == nil && !req.IDPresent // Treat only method-bearing messages with no wire id member as notifications so explicit null remains invalid.
}

// IsRequest reports whether one decoded JSON-RPC message carries a method and a valid string or integer request id.
func IsRequest(req *Request) bool {
	return req != nil && req.Method != "" && validRequestID(req.ID) // Accept only MCP's string-or-integer id domain so malformed IDs cannot be dispatched as requests.
}

// validRequestID reports whether one decoded or programmatically constructed value is a JSON string or mathematically integral JSON number.
func validRequestID(id interface{}) bool {
	switch value := id.(type) { // Recognize decoder-produced numbers and common integer types used by internal tests and callers.
	case string:
		return true // Accept every JSON string, including an empty string, because MCP places no additional content restriction on string IDs.
	case json.Number:
		if !json.Valid([]byte(value.String())) { // Reject programmatically constructed json.Number values that are not valid standalone JSON numbers.
			return false // Prevent malformed numeric text from entering response serialization or cancellation keys.
		}
		var integer big.Int                            // Allocate an arbitrary-precision integer so JSON-RPC ids are not limited to int64 or uint64.
		_, ok := integer.SetString(value.String(), 10) // Require an exact base-10 integer literal with no fraction or exponent syntax.
		return ok                                      // Accept every valid arbitrary-size integer literal while preserving its exact wire representation.
	case float64:
		return !math.IsNaN(value) && !math.IsInf(value, 0) && math.Trunc(value) == value // Accept programmatic finite floats only when they represent a mathematical integer.
	case float32:
		floatValue := float64(value)                                                                         // Promote the programmatic float for standard finite and truncation checks.
		return !math.IsNaN(floatValue) && !math.IsInf(floatValue, 0) && math.Trunc(floatValue) == floatValue // Reject fractional or non-JSON float values.
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true // Accept native integer types used by programmatic callers before JSON serialization.
	default:
		return false // Reject nil, null, booleans, arrays, objects, and every non-integral or unsupported identifier type.
	}
}

// RegisterInFlightRequest records one cancellable request id and returns a cleanup function that removes only that exact registry entry.
func (h *Handler) RegisterInFlightRequest(id interface{}, cancel context.CancelFunc) func() {
	key, ok := requestIDKey(id)           // Normalize the JSON-RPC id once so requests decoded through different Go numeric types still share the same cancellation key.
	if h == nil || !ok || cancel == nil { // Skip registration when the handler, request id, or cancel hook is unavailable.
		return func() {} // Return a no-op cleanup so callers can always defer the returned function safely.
	}

	entry := &inflightRequest{cancel: cancel} // Allocate one entry pointer so cleanup can avoid deleting a newer request that reused the same id later.
	h.inflightMu.Lock()                       // Lock the in-flight registry before mutating the shared request map.
	if h.inflightRequests == nil {            // Lazily initialize the map so zero-value handlers used in tests still avoid nil-map panics.
		h.inflightRequests = make(map[string]*inflightRequest) // Allocate the registry map before storing the new request entry.
	}
	h.inflightRequests[key] = entry // Store the request cancel hook under the normalized JSON-RPC request id.
	h.inflightMu.Unlock()           // Release the registry lock before returning to request execution.

	return func() { h.unregisterInFlightRequest(key, entry) } // Return exact-entry cleanup so completed requests stop being cancellable without racing newer duplicate ids.
}

// CancelInFlightRequest cancels the request matching id and reports whether an active request was found.
func (h *Handler) CancelInFlightRequest(id interface{}) bool {
	key, ok := requestIDKey(id) // Normalize the cancellation request id using the same key function as registration.
	if h == nil || !ok {        // Treat missing handlers and malformed ids as benign unmatched cancellation notifications.
		return false // Report no match so callers can log the late or invalid cancellation without surfacing a protocol error.
	}

	h.inflightMu.Lock()                     // Lock the registry while reading and removing the matching in-flight entry.
	entry, found := h.inflightRequests[key] // Read the active request entry for the normalized id.
	if found {                              // Remove matched entries before invoking cancellation so repeated notifications stay idempotent.
		delete(h.inflightRequests, key) // Delete the matched request from the registry so later cancellations do not repeat the same cancel call.
	}
	h.inflightMu.Unlock() // Release the registry lock before calling user code through the cancel function.

	if !found { // Treat late or unknown cancellation requests as benign no-ops.
		return false // Report no match so callers can include the outcome in low-noise observability.
	}
	entry.cancel() // Cancel the in-flight request context so downstream handlers can stop work promptly.
	return true    // Report that one active request was cancelled.
}

// unregisterInFlightRequest removes one registry entry only when it still points at the same request entry.
func (h *Handler) unregisterInFlightRequest(key string, entry *inflightRequest) {
	if h == nil || entry == nil { // Ignore invalid cleanup calls because callers defer cleanup across several error paths.
		return // Nothing can be removed safely without both a handler and the exact entry pointer.
	}

	h.inflightMu.Lock()                   // Lock the registry before checking whether the entry is still current for this key.
	if h.inflightRequests[key] == entry { // Delete only the original entry so duplicate request ids cannot be removed by an older cleanup.
		delete(h.inflightRequests, key) // Remove the completed request from the cancellation registry.
	}
	h.inflightMu.Unlock() // Release the registry lock after the conditional cleanup is complete.
}

// requestIDKey converts a JSON-RPC id into a stable map key and rejects nil or JSON null identifiers.
func requestIDKey(id interface{}) (string, bool) {
	if id == nil { // Reject nil ids because notifications have no request id and therefore cannot be registered or cancelled.
		return "", false // Report that no usable cancellation key exists for the provided id.
	}

	encoded, err := json.Marshal(id) // Serialize the id through JSON so numeric ids decoded as int or float64 normalize to the same wire representation.
	if err != nil {                  // Reject ids that cannot be represented as JSON because they are not valid JSON-RPC request identifiers.
		return "", false // Report that no usable cancellation key exists for the malformed id.
	}
	key := string(encoded) // Use the compact JSON representation as the registry key so string and numeric ids remain distinct.
	if key == "null" {     // Reject explicit JSON null ids because this codebase treats nil ids as notifications.
		return "", false // Report that null is not a usable cancellation key.
	}
	return key, true // Return the normalized id key so registration and cancellation can address the same request.
}

// handleCancelledNotification decodes one MCP cancellation notification and cancels a matching in-flight request when present.
func (h *Handler) handleCancelledNotification(ctx context.Context, params json.RawMessage) (interface{}, error) {
	logger := telemetry.WithContext(ctx) // Build context-enriched logging so cancellation observations carry request trace metadata when available.
	var p struct {
		RequestID interface{} `json:"requestId"`
		Reason    string      `json:"reason"`
	}

	decoder := json.NewDecoder(bytes.NewReader(params)) // Decode cancellation metadata from its exact raw JSON representation.
	decoder.UseNumber()                                 // Preserve a large numeric requestId so it matches the original in-flight request key exactly.
	if err := decoder.Decode(&p); err != nil {          // Decode the MCP cancellation payload so the original request id can be matched against the registry.
		return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode notifications/cancelled params"} // Return invalid-params for malformed cancellation payloads while transports suppress notification responses.
	}
	if p.RequestID == nil { // Reject cancellation payloads that omit requestId because no registry lookup can be performed.
		return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "notifications/cancelled requestId is required"} // Return invalid-params so request-style misuse receives a stable protocol error.
	}

	matched := h.CancelInFlightRequest(p.RequestID)                                                                       // Cancel the matching registered request when it is still active.
	logger.Info("mcp cancelled notification received", "request_id", p.RequestID, "reason", p.Reason, "matched", matched) // Record both matched and late cancellations without surfacing protocol errors for benign races.
	return nil, nil                                                                                                       // Cancellation notifications do not produce MCP result payloads.
}

// ProcessRequest dispatches one JSON-RPC request or notification and returns the handler result for request-style calls.
func (h *Handler) ProcessRequest(ctx context.Context, req *Request) (interface{}, error) {
	if req == nil { // Reject nil messages before logging because there is no method or id to safely inspect.
		return nil, &mcp.MCPError{Code: -32600, Message: "Invalid Request"} // Return the JSON-RPC invalid-request error instead of panicking on malformed internal calls.
	}

	logger := telemetry.WithContext(ctx)                                     // Build context-enriched logger so per-request logs carry trace ids when available.
	startedAt := time.Now()                                                  // Capture dispatch start timestamp for duration reporting.
	logger.Info("jsonrpc process begin", "method", req.Method, "id", req.ID) // Log every JSON-RPC method entry for step-by-step tracing.

	var result interface{}
	var err error

	if !IsRequest(req) && !IsNotification(req) { // Reject messages that are neither requests nor notifications because the dispatcher only handles method-bearing calls.
		return nil, &mcp.MCPError{Code: -32600, Message: "Invalid Request"} // Return the JSON-RPC invalid-request error so transports expose stable protocol semantics.
	}
	modern, eraErr := validateRequestEra(req) // Classify this message from its own params._meta instead of relying on transport or connection state.
	if eraErr != nil {                        // Reject malformed metadata, unavailable versions, and methods removed from the modern protocol before dispatch.
		return nil, eraErr // Preserve the exact modern MCP error code and retry metadata for the transport response.
	}

	if h.validator != nil {
		if vErr := h.validator.Validate(req.Method, req.Params); vErr != nil {
			return nil, vErr
		}
	}

	switch req.Method {
	// ===== MCP Protocol Methods =====
	case "server/discover":
		result, err = h.mcpHandler.Discover(req.Params) // Return mandatory modern discovery metadata through the same MCP handler used by other server features.
	case "initialize":
		result, err = h.mcpHandler.Initialize(ctx, req.Params)
	case "notifications/initialized":
		logger.Info("mcp initialized notification received", "method", req.Method) // Record the lifecycle notification at low noise because clients are expected to send it after initialize.
	case "notifications/cancelled":
		result, err = h.handleCancelledNotification(ctx, req.Params) // Resolve the cancellation notification through the in-flight registry shared by all transports.
	case "ping":
		result = map[string]interface{}{} // Return the minimal MCP ping payload so clients can verify liveness without invoking business logic.
	case "tools/list":
		result, err = h.mcpHandler.ToolsList(ctx, req.Params)
	case "tools/call":
		result, err = h.mcpHandler.ToolsCall(ctx, req.Params)
	case "resources/list":
		if modern { // Separate concrete modern resources from the URI templates historically returned to legacy clients.
			result, err = h.mcpHandler.ModernResourcesList(ctx, req.Params) // Return the current caller's concrete resource set using the modern protocol shape.
		} else {
			result, err = h.mcpHandler.ResourcesList(ctx, req.Params) // Preserve the repository's historical template-like resources/list response for legacy clients.
		}
	case "resources/templates/list":
		result, err = h.mcpHandler.ResourcesTemplatesList(ctx, req.Params) // Return parameterized Appium resources through the standard resource-template method.
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
	// Interactive element operations
	case "findElement":
		var params struct {
			SessionID string `json:"sessionId"`
			Strategy  string `json:"strategy"`
			Selector  string `json:"selector"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode findElement params"}
		}
		var elementID string
		elementID, err = h.orch.FindElement(ctx, params.SessionID, params.Strategy, params.Selector)
		if err == nil {
			result = map[string]string{"elementId": elementID}
		}
	case "clickElement":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode clickElement params"}
		}
		err = h.orch.ClickElement(ctx, params.SessionID, params.ElementID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "sendKeysToElement":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
			Text      string `json:"text"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode sendKeysToElement params"}
		}
		err = h.orch.SendKeysToElement(ctx, params.SessionID, params.ElementID, params.Text)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "clearElement":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode clearElement params"}
		}
		err = h.orch.ClearElement(ctx, params.SessionID, params.ElementID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "getElementText":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode getElementText params"}
		}
		var text string
		text, err = h.orch.GetElementText(ctx, params.SessionID, params.ElementID)
		if err == nil {
			result = map[string]string{"text": text}
		}
	case "getElementAttribute":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
			Attribute string `json:"attribute"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode getElementAttribute params"}
		}
		var value string
		value, err = h.orch.GetElementAttribute(ctx, params.SessionID, params.ElementID, params.Attribute)
		if err == nil {
			result = map[string]interface{}{"attribute": params.Attribute, "value": value}
		}
	case "isElementDisplayed":
		var params struct {
			SessionID string `json:"sessionId"`
			ElementID string `json:"elementId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode isElementDisplayed params"}
		}
		var displayed bool
		displayed, err = h.orch.IsElementDisplayed(ctx, params.SessionID, params.ElementID)
		if err == nil {
			result = map[string]bool{"displayed": displayed}
		}
	case "tap":
		var params struct {
			SessionID string `json:"sessionId"`
			X         int    `json:"x"`
			Y         int    `json:"y"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode tap params"}
		}
		err = h.orch.Tap(ctx, params.SessionID, params.X, params.Y)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "swipe":
		var params struct {
			SessionID  string `json:"sessionId"`
			StartX     int    `json:"startX"`
			StartY     int    `json:"startY"`
			EndX       int    `json:"endX"`
			EndY       int    `json:"endY"`
			DurationMs int    `json:"durationMs"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode swipe params"}
		}
		if params.DurationMs == 0 {
			params.DurationMs = 200
		}
		err = h.orch.Swipe(ctx, params.SessionID, params.StartX, params.StartY, params.EndX, params.EndY, params.DurationMs)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "longPress":
		var params struct {
			SessionID  string `json:"sessionId"`
			ElementID  string `json:"elementId"`
			DurationMs int    `json:"durationMs"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode longPress params"}
		}
		if params.DurationMs == 0 {
			params.DurationMs = 1000
		}
		err = h.orch.LongPress(ctx, params.SessionID, params.ElementID, params.DurationMs)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "pressBack":
		var params struct {
			SessionID string `json:"sessionId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode pressBack params"}
		}
		err = h.orch.PressBack(ctx, params.SessionID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "hideKeyboard":
		var params struct {
			SessionID string `json:"sessionId"`
		}
		if err = json.Unmarshal(req.Params, &params); err != nil {
			return nil, &mcp.MCPError{Code: -32602, Message: "Invalid params", Data: "failed to decode hideKeyboard params"}
		}
		err = h.orch.HideKeyboard(ctx, params.SessionID)
		if err == nil {
			result = map[string]bool{"success": true}
		}
	case "replay", "subscribe", "unsubscribe":
		return nil, errors.New(errors.CodeStepUnsupported, "method not implemented: "+req.Method)
	default:
		if IsNotification(req) { // Silently ignore unknown notifications because JSON-RPC notifications must not elicit protocol errors on the wire.
			logger.Info("jsonrpc notification ignored", "method", req.Method) // Record the ignored notification at low noise so operators can still trace unexpected client traffic.
			break
		}
		return nil, &mcp.MCPError{
			Code:    -32601,
			Message: "Method not found",
		}
	}

	if err != nil {
		if IsNotification(req) { // Downgrade notification failures to info logging because transports intentionally suppress notification responses.
			logger.Info("jsonrpc notification failed", "method", req.Method, "id", req.ID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Preserve observability for failed notifications without introducing error-level noise.
			return nil, err
		}
		logger.Error("jsonrpc process failed", "method", req.Method, "id", req.ID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Log method failures with latency and root error.
		return nil, err
	}
	if modern && IsRequest(req) { // Add current-protocol result fields only to successful modern requests, never legacy responses or notifications.
		result, err = mcp.DecorateModernResult(req.Method, result) // Normalize the method result and attach method-specific current-protocol metadata.
		if err != nil {                                            // Surface an internal protocol error when a handler produced a non-object modern result.
			logger.Error("jsonrpc modern result decoration failed", "method", req.Method, "id", req.ID, "duration_ms", time.Since(startedAt).Milliseconds(), "error", err) // Record the server-side result-shape defect with request context.
			return nil, err                                                                                                                                                // Prevent a non-compliant success payload from reaching any transport.
		}
	}

	logger.Info("jsonrpc process done", "method", req.Method, "id", req.ID, "duration_ms", time.Since(startedAt).Milliseconds()) // Log successful completion with total dispatch latency.
	return result, nil
}

// ServeHTTP implements the compatibility HTTP JSON-RPC transport while suppressing responses for notifications.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > httpinput.MaxRequestBodyBytes { // Reject known oversized compatibility requests before reading or decoding their payload.
		h.writeErrorStatus(w, http.StatusRequestEntityTooLarge, nil, -32600, "Invalid Request", "request body too large") // Return HTTP 413 with a sanitized JSON-RPC envelope.
		return                                                                                                            // Stop request processing after the declared-size rejection.
	}
	r.Body = http.MaxBytesReader(w, r.Body, httpinput.MaxRequestBodyBytes) // Enforce the compatibility transport's body ceiling even without the outer gateway middleware.
	var req Request                                                        // Allocate the JSON-RPC request envelope before strict single-document decoding.
	if err := httpinput.DecodeSingleJSON(r.Body, &req); err != nil {       // Decode exactly one bounded JSON value and reject concatenated request documents.
		if httpinput.IsBodyTooLarge(err) { // Map the typed body-limit failure to its transport-level status before ordinary parse errors.
			h.writeErrorStatus(w, http.StatusRequestEntityTooLarge, nil, -32600, "Invalid Request", "request body too large") // Return HTTP 413 with a sanitized JSON-RPC envelope.
			return                                                                                                            // Stop request processing after the size rejection.
		}
		h.writeError(w, nil, -32700, "Parse error", nil)
		return
	}

	if req.JSONRPC != "2.0" {
		if IsNotification(&req) { // Suppress invalid-version notification responses because JSON-RPC notifications must not receive reply envelopes.
			w.WriteHeader(http.StatusNoContent) // Return HTTP 204 so the compatibility endpoint closes the exchange without a JSON-RPC body.
			return
		}
		h.writeError(w, req.ID, -32600, "Invalid Request", nil)
		return
	}

	ctx := r.Context()   // Start from the HTTP request context so client disconnects and server shutdown still cancel work.
	if IsRequest(&req) { // Register only request-style messages because notifications have no response id to cancel later.
		requestCtx, cancel := context.WithCancel(ctx)        // Create a cancellable child context that notifications/cancelled can trigger through the in-flight registry.
		cleanup := h.RegisterInFlightRequest(req.ID, cancel) // Record the request id before dispatch so concurrent HTTP cancellation notifications can find it.
		defer cleanup()                                      // Remove the request from the registry once dispatch completes or errors.
		defer cancel()                                       // Release context resources after dispatch even when no cancellation notification arrives.
		ctx = requestCtx                                     // Pass the cancellable child context into downstream handlers.
	}
	result, err := h.ProcessRequest(ctx, &req) // Dispatch the request or notification through the shared JSON-RPC protocol handler.

	if IsNotification(&req) { // Suppress notification responses because JSON-RPC notifications are fire-and-forget messages.
		w.WriteHeader(http.StatusNoContent) // Return HTTP 204 so the compatibility endpoint completes cleanly without emitting a response object.
		return
	}

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

// writeError executes this operation.
func (h *Handler) writeError(w http.ResponseWriter, id interface{}, code int, msg string, data interface{}) {
	h.writeResponse(w, Response{ // Preserve the historical HTTP 200 envelope for ordinary compatibility JSON-RPC errors.
		JSONRPC: "2.0",
		Error: &errors.JSONRPCError{
			Code:    code,
			Message: msg,
			Data:    data,
		},
		ID: id,
	})
}

// writeErrorStatus writes one JSON-RPC error envelope using an explicit HTTP transport status for pre-dispatch limits such as oversized bodies.
func (h *Handler) writeErrorStatus(w http.ResponseWriter, status int, id interface{}, code int, msg string, data interface{}) {
	h.writeResponseStatus(w, status, Response{ // Serialize the supplied protocol error with the transport-level rejection status.
		JSONRPC: "2.0", // Identify the response envelope as JSON-RPC 2.0.
		Error: &errors.JSONRPCError{ // Attach the supplied stable error classification and sanitized data.
			Code:    code, // Preserve the caller-selected JSON-RPC error code.
			Message: msg,  // Preserve the caller-selected public error message.
			Data:    data, // Preserve only the caller-selected safe diagnostic payload.
		},
		ID: id, // Echo the request identifier when decoding reached it, or null for pre-envelope failures.
	})
}

// writeResponse executes this operation.
func (h *Handler) writeResponse(w http.ResponseWriter, resp Response) {
	h.writeResponseStatus(w, http.StatusOK, resp) // Preserve the compatibility transport's historical HTTP 200 response status.
}

// writeResponseStatus serializes one JSON-RPC response with an explicit HTTP status.
func (h *Handler) writeResponseStatus(w http.ResponseWriter, status int, resp Response) {
	w.Header().Set("Content-Type", "application/json") // Mark every compatibility response as JSON before writing its status.
	w.WriteHeader(status)                              // Emit the explicit transport status before serializing the response envelope.
	_ = json.NewEncoder(w).Encode(resp)                // Encode the response while ignoring late network write failures after headers are committed.
}
