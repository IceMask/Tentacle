package stdio

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/capabilities"
	"mcp_for_appium/internal/gateway/jsonrpc"
	"mcp_for_appium/internal/gateway/mcp"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/telemetry"
)

// Transport handles stdio-based JSON-RPC communication for MCP protocol
type Transport struct {
	handler *jsonrpc.Handler
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	mu      sync.Mutex
}

// NewTransport creates a new stdio transport
func NewTransport(orch *orchestrator.Service, capSvc *capabilities.Service) *Transport {
	return NewTransportWithIO(orch, capSvc, os.Stdin, os.Stdout, telemetry.StdLogWriter()) // Delegate to the injectable constructor so production and tests share one initialization path.
}

// NewTransportWithIO creates one stdio transport over caller-supplied streams so tests can assert emitted JSON-RPC lines deterministically.
func NewTransportWithIO(orch *orchestrator.Service, capSvc *capabilities.Service, stdin io.Reader, stdout io.Writer, stderr io.Writer) *Transport {
	if stderr == nil { // Fall back to the telemetry-backed stderr sink when callers do not supply one explicit diagnostics stream.
		stderr = telemetry.StdLogWriter() // Reuse the production stderr sink so stdio transport diagnostics still avoid stdout corruption.
	}
	return &Transport{
		handler: jsonrpc.NewHandler(orch, capSvc),
		stdin:   stdin,
		stdout:  stdout,
		stderr:  stderr, // Route stdio transport diagnostics to the caller-supplied sink so tests can capture stderr cleanly without polluting stdout.
	}
}

// Run reads newline-delimited JSON-RPC messages, dispatches requests asynchronously, handles notifications synchronously, and writes responses to stdout until EOF or context cancellation.
func (t *Transport) Run(ctx context.Context) error {
	// Log to stderr only (stdout is reserved for JSON-RPC responses)
	log.SetOutput(t.stderr)
	log.Println("MCP stdio transport started")

	scanner := bufio.NewScanner(t.stdin)
	// Set a larger buffer size for potentially large messages
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024) // 1MB max message size

	var requestWG sync.WaitGroup // Track asynchronous request handlers so EOF can wait for already accepted work to finish before returning.
	for {
		select {
		case <-ctx.Done():
			log.Println("MCP stdio transport shutting down")
			return ctx.Err()
		default:
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					log.Printf("Scanner error: %v", err)
					return err
				}
				// EOF reached
				log.Println("EOF on stdin, shutting down")
				requestWG.Wait() // Wait for accepted asynchronous requests to finish so EOF does not drop responses that were already in progress.
				return nil
			}

			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			// Process the JSON-RPC request
			lineCopy := append([]byte(nil), line...)                             // Copy the scanner buffer because request handling may continue asynchronously after the next Scan call.
			if err := t.dispatchMessage(ctx, lineCopy, &requestWG); err != nil { // Dispatch the decoded line while keeping the read loop available for later notifications.
				log.Printf("Error handling request: %v", err)
				// Continue processing next request even if this one failed
			}
		}
	}
}

// dispatchMessage routes notifications synchronously and request messages asynchronously so cancellation notifications can be read while requests run.
func (t *Transport) dispatchMessage(ctx context.Context, reqData []byte, requestWG *sync.WaitGroup) error {
	var req jsonrpc.Request
	if err := json.Unmarshal(reqData, &req); err != nil { // Decode the line before deciding whether it can run asynchronously.
		return t.writeError(nil, -32700, "Parse error", err.Error()) // Emit parse errors synchronously because malformed messages have no request id to track.
	}

	if !jsonrpc.IsRequest(&req) { // Handle notifications and invalid methodless messages synchronously because they either suppress responses or fail quickly.
		return t.handleDecodedRequest(ctx, &req) // Reuse the normal decoded-message path so notification logging and suppression remain identical.
	}

	requestCtx, cleanup := t.prepareRequestContext(ctx, &req) // Register the request before starting the goroutine so later cancellation notifications can find it deterministically.
	requestWG.Add(1)                                          // Track the request goroutine so EOF can wait for this accepted request to finish.
	go func() {                                               // Process the request asynchronously so stdio can continue reading cancellation notifications.
		defer requestWG.Done()                                           // Mark this asynchronous request complete once response writing and cleanup finish.
		defer cleanup()                                                  // Remove the request from the in-flight registry and release the context when processing ends.
		if err := t.handleDecodedRequest(requestCtx, &req); err != nil { // Process the request on a goroutine so stdio can keep reading notifications.
			log.Printf("Error handling request: %v", err) // Keep asynchronous request errors visible because dispatchMessage cannot return them after spawning.
		}
	}()
	return nil // Report successful dispatch because the request goroutine now owns processing and response emission.
}

// handleRequest processes one incoming JSON-RPC message and suppresses any response when the message is a notification.
func (t *Transport) handleRequest(ctx context.Context, reqData []byte) error {
	var req jsonrpc.Request
	if err := json.Unmarshal(reqData, &req); err != nil { // Decode the incoming line into one JSON-RPC message before dispatching it through the transport.
		return t.writeError(nil, -32700, "Parse error", err.Error()) // Emit the standard parse-error response when the line is not valid JSON.
	}

	requestCtx, cleanup := t.prepareRequestContext(ctx, &req) // Register request ids for cancellation while leaving notifications attached to the original context.
	defer cleanup()                                           // Remove the request from the in-flight registry and release context resources after synchronous processing.
	return t.handleDecodedRequest(requestCtx, &req)           // Process the decoded message through the shared transport path used by synchronous tests and asynchronous Run dispatch.
}

// prepareRequestContext returns a cancellable request context and cleanup hook for request messages, or the original context for notifications.
func (t *Transport) prepareRequestContext(ctx context.Context, req *jsonrpc.Request) (context.Context, func()) {
	if !jsonrpc.IsRequest(req) { // Skip in-flight registration for notifications because they have no id and must not be cancellable by request id.
		return ctx, func() {} // Return a no-op cleanup so callers can always defer cleanup safely.
	}

	requestCtx, cancel := context.WithCancel(ctx)                // Create the cancellable child context that notifications/cancelled will trigger.
	cleanup := t.handler.RegisterInFlightRequest(req.ID, cancel) // Register the request id and cancel hook before business dispatch begins.
	return requestCtx, func() {                                  // Return a cleanup closure that unregisters the request and releases the child context.
		cleanup() // Remove the request id from the shared cancellation registry once processing finishes.
		cancel()  // Release context resources and make cleanup idempotent even when no cancellation notification arrived.
	} // Return the cancellable context and cleanup closure to the caller.
}

// handleDecodedRequest processes one decoded JSON-RPC message and emits a response only for request-style messages.
func (t *Transport) handleDecodedRequest(ctx context.Context, req *jsonrpc.Request) error {
	isNotification := jsonrpc.IsNotification(req) // Classify the decoded message once so request-vs-notification response handling stays consistent below.

	// Log the request to stderr
	log.Printf("Received request: method=%s id=%v", req.Method, req.ID)

	if progressToken, ok := extractProgressToken(req); ok { // Detect one optional MCP progress token so long-running tools can emit notifications/progress over stdio.
		ctx = mcp.WithProgressToken(ctx, progressToken)                  // Attach the client-supplied progress token so downstream tool handlers can correlate notifications correctly.
		ctx = mcp.WithProgressReporter(ctx, t.writeProgressNotification) // Attach the transport-backed progress reporter so downstream tool handlers can emit progress safely.
	}

	// Call the handler
	result, err := t.processRequest(ctx, req) // Dispatch the decoded message through the shared JSON-RPC handler.

	if isNotification { // Suppress all notification responses because JSON-RPC notifications are fire-and-forget messages.
		if err != nil { // Log notification failures locally because the transport intentionally will not emit a JSON-RPC error envelope.
			log.Printf("Ignoring notification error: method=%s error=%v", req.Method, err) // Preserve observability for malformed or unsupported notifications without polluting stdout.
		}
		return nil // Report transport success because the notification was consumed without any response write.
	}

	if err != nil {
		// Check if it's an MCP error or other error
		return t.writeErrorResponse(req.ID, err)
	}

	// Write successful response
	return t.writeResult(req.ID, result)
}

// processRequest routes one decoded JSON-RPC request or notification to the shared handler after validating the protocol version.
func (t *Transport) processRequest(ctx context.Context, req *jsonrpc.Request) (interface{}, error) {
	if req.JSONRPC != "2.0" {
		return nil, fmt.Errorf("invalid jsonrpc version")
	}

	return t.handler.ProcessRequest(ctx, req)
}

// writeResult writes a successful JSON-RPC response
func (t *Transport) writeResult(id interface{}, result interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	response := jsonrpc.Response{
		JSONRPC: "2.0",
		Result:  result,
		ID:      id,
	}

	return t.writeJSONLocked(response)
}

// writeError writes a JSON-RPC error response
func (t *Transport) writeError(id interface{}, code int, message string, data interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	response := jsonrpc.Response{
		JSONRPC: "2.0",
		Error: &errors.JSONRPCError{
			Code:    code,
			Message: message,
			Data:    data,
		},
		ID: id,
	}

	return t.writeJSONLocked(response)
}

// writeErrorResponse writes an error response from an error object
func (t *Transport) writeErrorResponse(id interface{}, err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var jsonErr *errors.JSONRPCError

	// Check if it's an MCP error
	if mcpErr, ok := err.(*mcp.MCPError); ok {
		jsonErr = &errors.JSONRPCError{
			Code:    mcpErr.Code,
			Message: mcpErr.Message,
			Data:    mcpErr.Data,
		}
	} else {
		// Map other errors
		jsonErr = errors.MapToJSONRPC(err)
	}

	response := jsonrpc.Response{
		JSONRPC: "2.0",
		Error:   jsonErr,
		ID:      id,
	}

	return t.writeJSONLocked(response)
}

// writeProgressNotification writes one JSON-RPC notifications/progress message to stdout in a transport-safe critical section.
func (t *Transport) writeProgressNotification(ctx context.Context, update mcp.ProgressUpdate) error {
	_ = ctx             // Accept the request context for interface symmetry even though stdio notifications are written synchronously under the transport mutex.
	t.mu.Lock()         // Serialize notifications with normal responses so stdout remains one valid newline-delimited JSON-RPC stream.
	defer t.mu.Unlock() // Release the stdout lock promptly once the progress notification has been written.

	return t.writeJSONLocked(map[string]interface{}{ // Emit the standard MCP progress notification envelope over the same stdout stream as normal responses.
		"jsonrpc": "2.0",
		"method":  "notifications/progress",
		"params": map[string]interface{}{
			"progressToken": update.Token,
			"progress":      update.Progress,
			"total":         update.Total,
			"message":       update.Message,
		},
	})
}

// extractProgressToken extracts one optional MCP progress token from a tools/call request payload.
func extractProgressToken(req *jsonrpc.Request) (interface{}, bool) {
	if req == nil || req.Method != "tools/call" || len(req.Params) == 0 { // Ignore non-tools/call and empty requests because only tools/call may carry one MCP progress token here.
		return nil, false // Report no token so the downstream context stays unchanged for requests that do not support progress notifications here.
	}

	var params struct {
		Meta map[string]interface{} `json:"_meta"` // Decode only the MCP metadata envelope because tool arguments themselves are handled later by the MCP handler.
	}
	if err := json.Unmarshal(req.Params, &params); err != nil { // Parse the minimal params shape best-effort so malformed requests still fall through to normal request validation.
		return nil, false // Suppress progress wiring on malformed params because the main handler will return the authoritative protocol error.
	}
	token, ok := params.Meta["progressToken"] // Read the optional progress token from the MCP metadata envelope when present.
	if !ok || token == nil {                  // Ignore missing or explicit null progress tokens because clients are not asking for notifications in that case.
		return nil, false // Report no token so the downstream context stays unchanged for requests without progress tracking.
	}

	return token, true // Return the caller-supplied progress token so the transport can wire notifications into the request context.
}

// writeJSON writes a JSON object to stdout followed by newline under the transport mutex.
func (t *Transport) writeJSON(v interface{}) error {
	t.mu.Lock()         // Serialize direct JSON writes with every other response path so stdout remains one valid line-delimited JSON-RPC stream.
	defer t.mu.Unlock() // Release the stdout lock promptly once the JSON object has been written.

	return t.writeJSONLocked(v) // Delegate to the unlocked writer because the mutex is already held by this helper.
}

// writeJSONLocked writes a JSON object to stdout followed by newline while assuming the transport mutex is already held by the caller.
func (t *Transport) writeJSONLocked(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return err
	}

	// Write to stdout with newline
	if _, err := t.stdout.Write(data); err != nil {
		log.Printf("Failed to write to stdout: %v", err)
		return err
	}
	if _, err := t.stdout.Write([]byte("\n")); err != nil {
		log.Printf("Failed to write newline: %v", err)
		return err
	}

	log.Printf("Sent JSON-RPC response (%d bytes)", len(data)) // Record transport completion without copying tool results, tokens, or backend details into logs.
	return nil
}
