package capabilities

import (
	"context"
	"mcp_for_appium/internal/config"
)

var defaultDiscoveryToolNames = []string{ // defaultDiscoveryToolNames defines the full MCP tool catalog advertised when no operator-level tool disablement is active.
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
	"adbShell",                    // Advertise the current discoverable MCP tool set exactly as embedded in the registry when the operator has not disabled it.
}

type Service struct {
	cfg config.GatewayConfig
}

// NewService builds a capability service that returns the gateway's current externally advertised feature set.
func NewService(cfg config.GatewayConfig) *Service {
	return &Service{cfg: cfg} // Preserve the gateway config so future capability shaping can remain request-local and side-effect free.
}

// ADBShellToolEnabled reports whether the operator left the adbShell MCP tool enabled for external discovery and invocation.
func (s *Service) ADBShellToolEnabled() bool {
	return !s.cfg.DisableADBShellTool // Treat the operator-level disable flag as the single discovery and handler source of truth for adbShell exposure.
}

// List returns the current discovery snapshot for clients, including supported MCP tools, recommended execution mode, and known experimental gaps.
func (s *Service) List(ctx context.Context) map[string]interface{} {
	_ = ctx                             // Keep the context parameter for future request-scoped capability shaping without changing the public method signature.
	toolNames := s.discoveryToolNames() // Derive the externally visible tool catalog after applying operator-level feature disablement.
	return map[string]interface{}{      // Return a static capability snapshot that mirrors the repository's current supported surface.
		"apiVersion": "4.3.0",
		"capabilities": map[string]interface{}{ // Group detailed discovery metadata under a stable top-level capabilities envelope.
			"toolCount":                len(toolNames), // Advertise the actual number of externally exposed MCP tools after feature gating is applied.
			"recommendedExecutionMode": "monolith",     // Mark monolith as the currently recommended and formally supported execution path.
			"experimentalExecutionModes": []string{ // Enumerate execution modes that are intentionally exposed as non-production.
				"distributed", // Surface distributed mode as experimental because its execution closure is not yet production-ready.
			},
			"toolNames": toolNames, // Enumerate the full externally visible MCP tool surface for client discovery and documentation alignment.
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

// discoveryToolNames returns the externally visible tool catalog after applying operator-level disablement flags.
func (s *Service) discoveryToolNames() []string {
	toolNames := make([]string, 0, len(defaultDiscoveryToolNames)) // Allocate one independent slice so callers cannot mutate the package-level default catalog.
	for _, toolName := range defaultDiscoveryToolNames {           // Walk the full built-in discovery catalog once so disabled tools can be filtered deterministically.
		if toolName == "adbShell" && !s.ADBShellToolEnabled() { // Hide adbShell from discovery entirely when the operator-level disable flag is active.
			continue // Skip the disabled tool so discovery output and MCP registry exposure stay aligned.
		}
		toolNames = append(toolNames, toolName) // Preserve each still-enabled tool in discovery order for stable client-facing output.
	}
	return toolNames // Return the filtered discovery catalog so capability output and tests can assert the exact exposed tool set.
}
