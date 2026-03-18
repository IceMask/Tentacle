package capabilities

import (
	"context"
	"mcp_for_appium/internal/config"
)

type Service struct {
	cfg config.GatewayConfig
}

// NewService builds a capability service that returns the gateway's current externally advertised feature set.
func NewService(cfg config.GatewayConfig) *Service {
	return &Service{cfg: cfg} // Preserve the gateway config so future capability shaping can remain request-local and side-effect free.
}

// List returns the current discovery snapshot for clients, including supported MCP tools, recommended execution mode, and known experimental gaps.
func (s *Service) List(ctx context.Context) map[string]interface{} {
	_ = ctx                        // Keep the context parameter for future request-scoped capability shaping without changing the public method signature.
	return map[string]interface{}{ // Return a static capability snapshot that mirrors the repository's current supported surface.
		"apiVersion": "4.3.0",
		"capabilities": map[string]interface{}{ // Group detailed discovery metadata under a stable top-level capabilities envelope.
			"toolCount":                26,         // Advertise the actual number of embedded MCP tool definitions in internal/gateway/mcp/tools.
			"recommendedExecutionMode": "monolith", // Mark monolith as the currently recommended and formally supported execution path.
			"experimentalExecutionModes": []string{ // Enumerate execution modes that are intentionally exposed as non-production.
				"distributed", // Surface distributed mode as experimental because its execution closure is not yet production-ready.
			},
			"toolNames": []string{ // Enumerate the full embedded MCP tool surface for client discovery and documentation alignment.
				"startSession",                // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"executePlan",                 // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"endSession",                  // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"getSemanticSnapshot",         // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"takeScreenshot",              // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"cancelPlan",                  // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"getTrace",                    // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"healthCheck",                 // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"findElement",                 // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"clickElement",                // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"sendKeysToElement",           // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"clearElement",                // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"getElementText",              // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"getElementAttribute",         // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"isElementDisplayed",          // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"tap",                         // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"swipe",                       // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"longPress",                   // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"pressBack",                   // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"hideKeyboard",                // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"createDeviceFarmUpload",      // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"getDeviceFarmUpload",         // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"getDeviceFarmRuntimeContext", // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"scheduleDeviceFarmRun",       // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"getDeviceFarmRun",            // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
				"adbShell",                    // Advertise the current discoverable MCP tool set exactly as embedded in the registry.
			},
			"unimplementedJSONRPCMethods": []string{ // Enumerate reserved JSON-RPC methods that still return not implemented today.
				"replay",      // Expose reserved JSON-RPC methods that are declared but not yet wired to business logic.
				"subscribe",   // Expose reserved JSON-RPC methods that are declared but not yet wired to business logic.
				"unsubscribe", // Expose reserved JSON-RPC methods that are declared but not yet wired to business logic.
			},
			"features": map[string]string{ // Publish high-level stability labels for each externally visible interface family.
				"jsonrpc":     "stable",       // Keep JSON-RPC marked stable for the implemented core request surface.
				"a2a":         "beta",         // Keep REST/A2A marked beta while governance and security wiring are still incomplete.
				"mcp":         "stable",       // Mark MCP stable because the embedded tool discovery and call flow are in active use.
				"distributed": "experimental", // Advertise distributed mode conservatively until execution closure is complete.
			},
		},
	}
}
