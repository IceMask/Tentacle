// modern.go classifies dual-era JSON-RPC messages and constrains MCP 2026-07-28 requests to the modern methods implemented by this server.
package jsonrpc

import "mcp_for_appium/internal/gateway/mcp"

// validateRequestEra validates per-request metadata when present and reports whether dispatch must use modern MCP 2026-07-28 semantics.
func validateRequestEra(req *Request) (bool, error) {
	metadata, err := mcp.ParseRequestMetadata(req.Params) // Inspect only params._meta so method-specific payload decoding remains inside each handler.
	if err != nil {                                       // Reject malformed metadata before any business method sees untrusted request state.
		return false, err // Preserve the protocol-specific invalid-params diagnostic created by the metadata parser.
	}
	modern := metadata.ProtocolFieldsPresent || req.Method == "server/discover" // Treat namespaced protocol fields and mandatory discovery as modern while allowing legacy progressToken metadata.
	if !modern {                                                                // Preserve existing initialization-based and compatibility JSON-RPC behavior when metadata is absent.
		return false, nil // Report legacy semantics without imposing modern version or capability requirements.
	}
	if err := mcp.ValidateModernRequestMetadata(metadata); err != nil { // Require complete stateless context on every modern request, including discovery.
		return false, err // Return malformed or unsupported-version errors before method dispatch.
	}
	if !supportsModernMethod(req.Method) { // Reject legacy lifecycle and repository-specific direct methods from the modern protocol surface.
		return false, &mcp.MCPError{Code: -32601, Message: "Method not found"} // Use the standard unknown-method response required for unimplemented modern RPCs.
	}
	return true, nil // Confirm that dispatch and result decoration must follow modern stateless semantics.
}

// supportsModernMethod reports whether this server implements one method under MCP 2026-07-28 semantics.
func supportsModernMethod(method string) bool {
	switch method { // Enumerate only current core methods with production handlers in this repository.
	case "server/discover", "tools/list", "tools/call", "resources/list", "resources/templates/list", "resources/read", "notifications/cancelled":
		return true // Permit discovery, tools, resources, and stdio cancellation under modern semantics.
	default:
		return false // Exclude removed lifecycle methods and every unimplemented modern feature.
	}
}
