// transport_test.go verifies direct stdio transport request parsing and JSON-RPC response writing behavior.
package stdio

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"mcp_for_appium/internal/gateway/jsonrpc"
)

// TestHandleRequestWritesParseError verifies that malformed JSON input produces the standard JSON-RPC parse-error response.
func TestHandleRequestWritesParseError(t *testing.T) {
	var stdout bytes.Buffer                                                                                  // Capture the transport's response stream in memory so the emitted JSON-RPC error can be asserted directly.
	transport := &Transport{handler: jsonrpc.NewHandler(nil, nil), stdout: &stdout, stderr: &bytes.Buffer{}} // Construct one transport with an in-memory stdout sink because this test exercises only request parsing and response writing.

	if err := transport.handleRequest(context.Background(), []byte("{not-json")); err != nil { // Process one malformed JSON payload through the production request handler.
		t.Fatalf("expected parse error to be written successfully, got error: %v", err) // Surface unexpected transport write failures because the request parser should still emit a response.
	}
	if !strings.Contains(stdout.String(), `"code":-32700`) { // Fail the test when the transport does not emit the standard JSON-RPC parse-error code.
		t.Fatalf("expected parse error response, got %s", stdout.String()) // Surface the emitted output so parsing-regression diagnosis is straightforward.
	}
}

// TestProcessRequestRejectsInvalidVersion verifies that the stdio transport blocks non-2.0 requests before delegating to business dispatch.
func TestProcessRequestRejectsInvalidVersion(t *testing.T) {
	transport := &Transport{handler: jsonrpc.NewHandler(nil, nil)}                                           // Construct one transport with a production JSON-RPC handler because this test exercises only the protocol-version gate.
	request := &jsonrpc.Request{JSONRPC: "1.0", Method: "describeCapabilities", Params: []byte(`{}`), ID: 1} // Build one request with an invalid JSON-RPC version so the protocol gate can be exercised directly.

	if _, err := transport.processRequest(context.Background(), request); err == nil { // Process the invalid-version request through the production stdio transport gate.
		t.Fatal("expected invalid jsonrpc version to fail") // Surface the missing failure because the transport must reject non-2.0 requests deterministically.
	}
}
