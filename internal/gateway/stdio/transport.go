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

// Run starts the stdio transport loop
func (t *Transport) Run(ctx context.Context) error {
	// Log to stderr only (stdout is reserved for JSON-RPC responses)
	log.SetOutput(t.stderr)
	log.Println("MCP stdio transport started")

	scanner := bufio.NewScanner(t.stdin)
	// Set a larger buffer size for potentially large messages
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024) // 1MB max message size

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
				return nil
			}

			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			// Process the JSON-RPC request
			if err := t.handleRequest(ctx, line); err != nil {
				log.Printf("Error handling request: %v", err)
				// Continue processing next request even if this one failed
			}
		}
	}
}

// handleRequest processes a single JSON-RPC request
func (t *Transport) handleRequest(ctx context.Context, reqData []byte) error {
	var req jsonrpc.Request
	if err := json.Unmarshal(reqData, &req); err != nil {
		return t.writeError(nil, -32700, "Parse error", err.Error())
	}

	// Log the request to stderr
	log.Printf("Received request: method=%s id=%v", req.Method, req.ID)

	if progressToken, ok := extractProgressToken(&req); ok { // Detect one optional MCP progress token so long-running tools can emit notifications/progress over stdio.
		ctx = mcp.WithProgressToken(ctx, progressToken)                  // Attach the client-supplied progress token so downstream tool handlers can correlate notifications correctly.
		ctx = mcp.WithProgressReporter(ctx, t.writeProgressNotification) // Attach the transport-backed progress reporter so downstream tool handlers can emit progress safely.
	}

	// Call the handler
	result, err := t.processRequest(ctx, &req)

	if err != nil {
		// Check if it's an MCP error or other error
		return t.writeErrorResponse(req.ID, err)
	}

	// Write successful response
	return t.writeResult(req.ID, result)
}

// processRequest routes the request to the appropriate handler method
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

	log.Printf("Sent response: %s", string(data))
	return nil
}
