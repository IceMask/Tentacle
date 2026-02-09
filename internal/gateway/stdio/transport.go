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
	return &Transport{
		handler: jsonrpc.NewHandler(orch, capSvc),
		stdin:   os.Stdin,
		stdout:  os.Stdout,
		stderr:  os.Stderr,
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

	return t.writeJSON(response)
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

	return t.writeJSON(response)
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

	return t.writeJSON(response)
}

// writeJSON writes a JSON object to stdout followed by newline
func (t *Transport) writeJSON(v interface{}) error {
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
