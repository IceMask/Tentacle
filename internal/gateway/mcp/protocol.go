// protocol.go defines MCP 2026-07-28 request metadata, discovery, result decoration, and dual-era version compatibility primitives.
package mcp

import (
	"encoding/json"
	"strings"
)

const (
	CurrentProtocolVersion      = "2026-07-28"                                   // CurrentProtocolVersion identifies the modern stateless MCP revision implemented by this server.
	LatestLegacyProtocolVersion = "2025-11-25"                                   // LatestLegacyProtocolVersion identifies the newest initialization-based MCP revision accepted by this server.
	LegacyProtocolVersion       = "2025-06-18"                                   // LegacyProtocolVersion preserves compatibility with the repository's previously advertised MCP revision.
	JSONSchema202012            = "https://json-schema.org/draft/2020-12/schema" // JSONSchema202012 identifies the minimum JSON Schema dialect required by current MCP.
	ProtocolVersionMetaKey      = "io.modelcontextprotocol/protocolVersion"      // ProtocolVersionMetaKey carries the protocol revision on every modern request.
	ClientCapabilitiesMetaKey   = "io.modelcontextprotocol/clientCapabilities"   // ClientCapabilitiesMetaKey carries request-relevant client capabilities on every modern request.
	ClientInfoMetaKey           = "io.modelcontextprotocol/clientInfo"           // ClientInfoMetaKey optionally identifies the client implementation on a modern request.
	ServerInfoMetaKey           = "io.modelcontextprotocol/serverInfo"           // ServerInfoMetaKey identifies this server in each modern successful result.
	ResultTypeComplete          = "complete"                                     // ResultTypeComplete identifies a successful result that needs no additional client input.
	CacheScopePublic            = "public"                                       // CacheScopePublic allows clients to reuse server-wide protocol metadata across callers.
	CacheScopePrivate           = "private"                                      // CacheScopePrivate limits cached user-specific data to the same authorization context.
	DiscoveryCacheTTLMS         = 3600000                                        // DiscoveryCacheTTLMS allows one hour of caching for stable server identity and capabilities.
	CatalogCacheTTLMS           = 300000                                         // CatalogCacheTTLMS allows five minutes of caching for static tool and resource catalogs.
	ResourceCacheTTLMS          = 30000                                          // ResourceCacheTTLMS allows thirty seconds of caching for mutable Appium resource reads.
	ErrorCodeHeaderMismatch     = -32020                                         // ErrorCodeHeaderMismatch identifies missing, malformed, or body-inconsistent modern HTTP headers.
	ErrorCodeUnsupportedVersion = -32022                                         // ErrorCodeUnsupportedVersion identifies a modern request for a protocol revision this server cannot process.
)

// RequestMetadata captures the modern MCP protocol fields decoded from one request's params._meta object.
type RequestMetadata struct {
	Present                   bool                   // Present reports whether the request included a params._meta object.
	ProtocolFieldsPresent     bool                   // ProtocolFieldsPresent reports whether _meta contains modern namespaced version, capability, or identity fields.
	ProtocolVersion           string                 // ProtocolVersion is the exact MCP revision requested for this operation.
	ClientCapabilities        map[string]interface{} // ClientCapabilities contains only capabilities explicitly declared for this request.
	ClientCapabilitiesPresent bool                   // ClientCapabilitiesPresent distinguishes an omitted required field from a present empty object.
	ClientInfo                map[string]interface{} // ClientInfo optionally identifies the requesting client implementation.
}

// SupportedProtocolVersions returns a new preference-ordered slice containing every modern and legacy MCP revision this server implements.
func SupportedProtocolVersions() []string {
	return []string{CurrentProtocolVersion, LatestLegacyProtocolVersion, LegacyProtocolVersion} // Return a fresh slice so callers cannot mutate the server's advertised version set.
}

// NegotiateLegacyProtocolVersion preserves a supported legacy request version and otherwise falls back to the repository's previous 2025-06-18 behavior.
func NegotiateLegacyProtocolVersion(requested string) string {
	switch requested { // Select only initialization-based revisions because modern MCP removed the initialize handshake.
	case LatestLegacyProtocolVersion, LegacyProtocolVersion:
		return requested // Echo a mutually supported legacy revision so the client can continue with its requested semantics.
	default:
		return LegacyProtocolVersion // Preserve the historical fallback for older or unknown initialization-based clients.
	}
}

// ServerInfo returns a new MCP implementation descriptor for inclusion in legacy handshakes and modern result metadata.
func ServerInfo() map[string]interface{} {
	return map[string]interface{}{ // Allocate a fresh map so result decoration never shares mutable metadata across requests.
		"name":    "MCP Mobile Worker", // Identify the server product consistently across protocol eras.
		"version": "1.0.0",             // Identify the current server implementation release for diagnostics.
	}
}

// ServerCapabilities returns the MCP tool and resource capabilities implemented by this server without advertising unsupported subscriptions.
func ServerCapabilities() map[string]interface{} {
	return map[string]interface{}{ // Allocate one request-local capabilities object because clients may retain or transform the response.
		"tools": map[string]interface{}{ // Advertise the tool server feature implemented by tools/list and tools/call.
			"listChanged": false, // State that this process does not emit tool catalog change notifications.
		},
		"resources": map[string]interface{}{ // Advertise the resource server feature implemented by resource list, template list, and read methods.
			"subscribe":   false, // State that this server does not implement resource subscriptions.
			"listChanged": false, // State that this process does not emit resource catalog change notifications.
		},
	}
}

// ParseRequestMetadata decodes params._meta without interpreting a missing metadata object as a modern request.
func ParseRequestMetadata(params json.RawMessage) (RequestMetadata, error) {
	metadata := RequestMetadata{}                     // Start with legacy-compatible absence until an explicit _meta member is found.
	if len(params) == 0 || string(params) == "null" { // Accept omitted or null params as metadata-free input for legacy methods that allow them.
		return metadata, nil // Return the absent metadata marker without changing legacy dispatch behavior.
	}

	var paramsObject map[string]json.RawMessage                   // Decode only the top-level params members needed to locate _meta.
	if err := json.Unmarshal(params, &paramsObject); err != nil { // Reject non-object or malformed params before protocol metadata can be trusted.
		return metadata, &MCPError{Code: -32602, Message: "Invalid params", Data: "params must be a JSON object"} // Return the modern required invalid-params code with a stable diagnostic.
	}
	rawMetadata, present := paramsObject["_meta"] // Read the standard metadata member while leaving method-specific parameters untouched.
	if !present {                                 // Preserve legacy semantics when the request carries no modern metadata marker.
		return metadata, nil // Return a non-modern classification so dual-era dispatch can use initialization-based behavior.
	}
	metadata.Present = true // Record the metadata object without assuming legacy progress metadata selects modern protocol semantics.

	var metadataObject map[string]json.RawMessage                                                 // Decode namespaced metadata keys while preserving unknown extension metadata.
	if err := json.Unmarshal(rawMetadata, &metadataObject); err != nil || metadataObject == nil { // Require _meta to be a concrete JSON object rather than null, array, or scalar.
		return metadata, &MCPError{Code: -32602, Message: "Invalid params", Data: "params._meta must be a JSON object"} // Reject malformed request metadata with the protocol-required invalid-params code.
	}

	if rawVersion, exists := metadataObject[ProtocolVersionMetaKey]; exists { // Decode the required protocol version only when the namespaced key is present.
		metadata.ProtocolFieldsPresent = true                                         // Distinguish modern protocol metadata from legacy _meta members such as progressToken.
		if err := json.Unmarshal(rawVersion, &metadata.ProtocolVersion); err != nil { // Require the protocol version value to be a JSON string.
			return metadata, &MCPError{Code: -32602, Message: "Invalid params", Data: ProtocolVersionMetaKey + " must be a string"} // Return an actionable type diagnostic for malformed metadata.
		}
	}
	if rawCapabilities, exists := metadataObject[ClientCapabilitiesMetaKey]; exists { // Decode the required capability object while preserving an explicitly empty declaration.
		metadata.ProtocolFieldsPresent = true                                                 // Mark an empty capability declaration as a modern protocol-era signal.
		metadata.ClientCapabilitiesPresent = true                                             // Record field presence independently from decoded map contents so null remains invalid.
		if err := json.Unmarshal(rawCapabilities, &metadata.ClientCapabilities); err != nil { // Require capabilities to use the MCP object shape.
			return metadata, &MCPError{Code: -32602, Message: "Invalid params", Data: ClientCapabilitiesMetaKey + " must be an object"} // Return an actionable type diagnostic for malformed capabilities.
		}
	}
	if rawClientInfo, exists := metadataObject[ClientInfoMetaKey]; exists { // Decode optional implementation metadata only when the client supplies it.
		metadata.ProtocolFieldsPresent = true                                                                     // Treat optional modern identity alone as a modern signal that still requires both mandatory fields.
		if err := json.Unmarshal(rawClientInfo, &metadata.ClientInfo); err != nil || metadata.ClientInfo == nil { // Require optional clientInfo to be a non-null object when present.
			return metadata, &MCPError{Code: -32602, Message: "Invalid params", Data: ClientInfoMetaKey + " must be an object"} // Reject malformed optional identity metadata consistently.
		}
	}

	return metadata, nil // Return all recognized modern metadata for stateless validation and dispatch.
}

// ValidateModernRequestMetadata enforces the version and capability fields required on every MCP 2026-07-28 request.
func ValidateModernRequestMetadata(metadata RequestMetadata) error {
	if !metadata.Present { // Reject modern methods that omit the per-request metadata object entirely.
		return &MCPError{Code: -32602, Message: "Invalid params", Data: "params._meta is required for modern MCP requests"} // Use the protocol-required malformed-request error.
	}
	if strings.TrimSpace(metadata.ProtocolVersion) == "" { // Reject missing and empty protocol version values before attempting version selection.
		return &MCPError{Code: -32602, Message: "Invalid params", Data: ProtocolVersionMetaKey + " is required"} // Name the exact missing metadata key for client remediation.
	}
	if metadata.ProtocolVersion != CurrentProtocolVersion { // Process only the modern revision through per-request metadata semantics.
		return NewUnsupportedProtocolVersionError(metadata.ProtocolVersion) // Return the protocol-defined version error with retryable supported versions.
	}
	if !metadata.ClientCapabilitiesPresent || metadata.ClientCapabilities == nil { // Require a concrete object even when the request needs no optional client capability.
		return &MCPError{Code: -32602, Message: "Invalid params", Data: ClientCapabilitiesMetaKey + " is required and must be an object"} // Reject absent or null capabilities with the required malformed-request error.
	}
	return nil // Confirm that this request contains all state needed for independent modern processing.
}

// NewUnsupportedProtocolVersionError creates the protocol-defined retryable error for a modern request using an unavailable revision.
func NewUnsupportedProtocolVersionError(requested string) *MCPError {
	return &MCPError{ // Build the exact modern MCP error shape clients use for version retry selection.
		Code:    ErrorCodeUnsupportedVersion,    // Use the specification-reserved UnsupportedProtocolVersion code.
		Message: "Unsupported protocol version", // Use the canonical protocol error message.
		Data: map[string]interface{}{ // Include both values required for a deterministic client retry decision.
			"supported": SupportedProtocolVersions(), // Advertise modern and legacy revisions implemented by this dual-era server.
			"requested": requested,                   // Echo the unavailable request revision for diagnostics.
		},
	}
}

// DecorateModernResult converts one successful method result to an object and adds current-protocol completion, caching, and server identity metadata.
func DecorateModernResult(method string, result interface{}) (interface{}, error) {
	encodedResult, err := json.Marshal(result) // Normalize typed maps and structs into the JSON object representation required by MCP results.
	if err != nil {                            // Reject server values that cannot be represented on the JSON-RPC wire.
		return nil, &MCPError{Code: -32603, Message: "Internal error", Data: "failed to encode MCP result"} // Surface a standard internal error without leaking serialization internals.
	}
	var resultObject map[string]interface{}                                                     // Decode the normalized result into a mutable object for protocol field insertion.
	if err := json.Unmarshal(encodedResult, &resultObject); err != nil || resultObject == nil { // Require all modern MCP success payloads to be JSON objects.
		return nil, &MCPError{Code: -32603, Message: "Internal error", Data: "MCP result must be a JSON object"} // Reject invalid server result shapes before sending a non-compliant response.
	}
	if _, exists := resultObject["resultType"]; !exists { // Preserve extension or input-required result types already selected by a method.
		resultObject["resultType"] = ResultTypeComplete // Mark ordinary successful method results as final and complete.
	}
	applyModernCacheMetadata(method, resultObject)                       // Add only the cache policy defined for this modern method while leaving legacy method results unchanged.
	resultMetadata, ok := resultObject["_meta"].(map[string]interface{}) // Preserve existing response metadata when the method already supplied an object.
	if !ok {                                                             // Replace absent or malformed metadata with a valid request-local object.
		resultMetadata = make(map[string]interface{}) // Allocate the result metadata container required for server identity insertion.
	}
	resultMetadata[ServerInfoMetaKey] = ServerInfo() // Identify the server on this response without relying on prior connection state.
	resultObject["_meta"] = resultMetadata           // Attach the completed metadata object to the modern result.
	return resultObject, nil                         // Return the compliant modern MCP success object to the shared transport.
}

// applyModernCacheMetadata adds the current protocol's method-specific freshness and authorization-scope controls to one successful result object.
func applyModernCacheMetadata(method string, resultObject map[string]interface{}) {
	var ttlMS int    // Hold the method-specific freshness window in milliseconds until a cacheable method is selected.
	var scope string // Hold the method-specific cache sharing boundary until a cacheable method is selected.
	switch method {  // Select cache policy from the operation because result payloads do not otherwise identify their source method.
	case "server/discover":
		ttlMS = DiscoveryCacheTTLMS // Cache stable server identity and capability discovery for one hour.
		scope = CacheScopePublic    // Share discovery metadata because it does not vary by caller or authorization.
	case "tools/list", "resources/list", "resources/templates/list":
		ttlMS = CatalogCacheTTLMS // Cache static tool and resource catalogs for five minutes.
		scope = CacheScopePublic  // Share catalogs because this server does not filter them by caller.
	case "resources/read":
		ttlMS = ResourceCacheTTLMS // Limit mutable Appium resource reuse to thirty seconds.
		scope = CacheScopePrivate  // Restrict trace and session data to the same authorization context.
	default:
		return // Leave non-cacheable operations without cache metadata so clients do not retain dynamic command results.
	}
	resultObject["ttlMs"] = ttlMS      // Advertise the selected freshness duration on the modern successful result.
	resultObject["cacheScope"] = scope // Advertise whether the cached result may be shared across authorization contexts.
}

// Discover returns current server versions, capabilities, and usage guidance for the mandatory modern discovery method.
func (h *MCPHandler) Discover(_ json.RawMessage) (interface{}, error) {
	return map[string]interface{}{ // Build the stable discovery payload before shared result decoration adds current-protocol metadata.
		"supportedVersions": SupportedProtocolVersions(),                                                                                 // Advertise every revision implemented by this dual-era endpoint in preference order.
		"capabilities":      ServerCapabilities(),                                                                                        // Advertise only the server features currently implemented by this repository.
		"instructions":      "Use tools/list to discover Appium operations and resources/templates/list to discover trace URI patterns.", // Give clients concise, stable guidance for selecting the next operation.
	}, nil
}
