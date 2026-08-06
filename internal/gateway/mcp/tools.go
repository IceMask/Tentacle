package mcp

import (
	"embed"
	"encoding/json"
	"path/filepath"
	"sort"

	"mcp_for_appium/internal/errors"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed tools/*.json
var toolsFS embed.FS

// Tool represents an MCP tool definition
type Tool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

// ToolRegistry holds all available MCP tools
type ToolRegistry struct {
	tools   map[string]Tool               // tools stores each advertised MCP definition by its unique tool name.
	schemas map[string]*jsonschema.Schema // schemas stores one precompiled JSON Schema 2020-12 validator per tool name.
}

// NewToolRegistry loads every embedded tool definition, compiles its input schema as JSON Schema 2020-12, and panics when the built-in catalog is invalid.
func NewToolRegistry() *ToolRegistry {
	registry := &ToolRegistry{ // Allocate both catalog maps before reading the embedded tool definitions.
		tools:   make(map[string]Tool),               // Store tool metadata used by discovery and name lookup.
		schemas: make(map[string]*jsonschema.Schema), // Store compiled validators so calls do not recompile schemas per request.
	}
	if err := registry.registerBuiltinTools(); err != nil { // Treat an invalid embedded catalog as a build or deployment defect rather than serving partial discovery data.
		panic("failed to load MCP tools: " + err.Error()) // Stop startup so tools/list and tools/call can never disagree about available schemas.
	}
	return registry // Return the complete immutable-after-startup registry to the MCP handler.
}

// registerBuiltinTools decodes every embedded JSON tool, declares the MCP-default 2020-12 dialect, and stores its precompiled validator.
func (r *ToolRegistry) registerBuiltinTools() error {
	entries, err := toolsFS.ReadDir("tools") // Enumerate the embedded tool directory bundled into the gateway binary.
	if err != nil {                          // Propagate embed filesystem failures because no trustworthy catalog can be constructed.
		return err // Return the original read failure to the startup panic with full context.
	}

	for _, entry := range entries { // Process each embedded directory entry exactly once during registry construction.
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" { // Ignore nested directories and non-JSON support files.
			continue // Skip entries that cannot define one MCP tool document.
		}

		raw, err := toolsFS.ReadFile("tools/" + entry.Name()) // Read the complete embedded tool definition by its relative path.
		if err != nil {                                       // Abort registry construction when one advertised tool cannot be read.
			return err // Return the embed read failure so startup cannot expose a partial catalog.
		}

		var tool Tool                                      // Allocate the strongly typed MCP tool definition for this file.
		if err := json.Unmarshal(raw, &tool); err != nil { // Decode metadata and inputSchema from the embedded JSON document.
			return err // Reject malformed built-in JSON before the server starts accepting requests.
		}
		if tool.InputSchema == nil { // Require every advertised tool to provide the MCP-mandated input schema object.
			return errors.New(errors.CodeSchemaInvalid, "tool "+tool.Name+" has no inputSchema") // Return a structured startup error naming the invalid tool definition.
		}
		if _, declared := tool.InputSchema["$schema"]; !declared { // Make the current MCP default dialect explicit in tools/list output and compilation.
			tool.InputSchema["$schema"] = JSONSchema202012 // Declare JSON Schema 2020-12 for every existing schema that omitted an explicit dialect.
		}
		compiledSchema, err := compileToolInputSchema(tool) // Compile the schema once so unsupported 2020-12 keywords fail during startup rather than invocation.
		if err != nil {                                     // Abort registry construction when one input schema is invalid under the required dialect.
			return err // Return the compiler diagnostic so the malformed tool can be corrected before deployment.
		}

		r.tools[tool.Name] = tool             // Store the dialect-annotated definition used by deterministic tools/list responses.
		r.schemas[tool.Name] = compiledSchema // Store the matching validator used by tools/call argument validation.
	}

	return nil // Report that every embedded definition and validator loaded successfully.
}

// compileToolInputSchema compiles one tool input schema with JSON Schema 2020-12 as the explicit default dialect and performs no network loading.
func compileToolInputSchema(tool Tool) (*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()                                                    // Create an isolated compiler so one tool cannot resolve references into another tool implicitly.
	compiler.DefaultDraft(jsonschema.Draft2020)                                             // Pin schemas without $schema to the minimum dialect required by current MCP.
	schemaURL := "https://mcp-for-appium.invalid/tools/" + tool.Name + "/input-schema.json" // Assign a deterministic non-routable base URI for local reference resolution.
	if err := compiler.AddResource(schemaURL, tool.InputSchema); err != nil {               // Register the in-memory schema document without performing any network request.
		return nil, errors.Wrap(errors.CodeSchemaInvalid, "failed to register input schema for "+tool.Name, err) // Add the tool name while preserving the compiler failure.
	}
	compiledSchema, err := compiler.Compile(schemaURL) // Compile and meta-validate the tool schema against its declared 2020-12 dialect.
	if err != nil {                                    // Return a structured startup failure when the schema itself is invalid.
		return nil, errors.Wrap(errors.CodeSchemaInvalid, "failed to compile input schema for "+tool.Name, err) // Add the tool name so catalog defects are immediately actionable.
	}
	return compiledSchema, nil // Return the immutable validator reused by every invocation of this tool.
}

// List returns all registered tools sorted lexicographically by name for deterministic client and prompt caching.
func (r *ToolRegistry) List() []Tool {
	tools := make([]Tool, 0, len(r.tools)) // Preallocate the exact catalog capacity while retaining a zero-length append target.
	for _, tool := range r.tools {         // Copy each map value because Go map iteration order is intentionally unstable.
		tools = append(tools, tool) // Append the complete tool definition to the request-local result slice.
	}
	sort.Slice(tools, func(i int, j int) bool { return tools[i].Name < tools[j].Name }) // Sort by the protocol-unique name so unchanged catalogs serialize in a stable order.
	return tools                                                                        // Return the deterministic catalog snapshot without exposing the registry map itself.
}

// Get retrieves a tool by name
func (r *ToolRegistry) Get(name string) (Tool, bool) {
	tool, exists := r.tools[name]
	return tool, exists
}

// Validate rejects unknown tools, malformed argument JSON, and instances that fail the tool's precompiled JSON Schema 2020-12 validator.
func (r *ToolRegistry) Validate(toolName string, arguments json.RawMessage) error {
	_, exists := r.Get(toolName) // Confirm that discovery and invocation refer to the same enabled tool catalog.
	if !exists {                 // Reject names omitted from the live registry, including operator-disabled tools.
		return &MCPError{ // Return an MCP method-level not-found error before decoding untrusted arguments.
			Code:    -32601,                        // Preserve the repository's established tool-not-found code.
			Message: "Tool not found: " + toolName, // Name the unavailable tool for client correction.
		}
	}

	var args interface{}                                     // Decode arguments into the generic JSON value model expected by the schema validator.
	if err := json.Unmarshal(arguments, &args); err != nil { // Reject malformed JSON before schema evaluation.
		return &MCPError{ // Return the standard invalid-params family used by existing MCP clients.
			Code:    -32602,                   // Identify malformed tool arguments as invalid parameters.
			Message: "Invalid arguments JSON", // Distinguish JSON decoding failure from schema constraint failure.
			Data:    err.Error(),              // Preserve the decoder location and syntax diagnostic.
		}
	}
	schema, exists := r.schemas[toolName] // Read the validator compiled from the same definition returned by tools/list.
	if !exists || schema == nil {         // Treat registry divergence as an internal server defect rather than accepting unchecked input.
		return &MCPError{ // Return a standard internal error because clients cannot repair a missing server validator.
			Code:    -32603,                    // Identify catalog-validator divergence as a JSON-RPC internal error.
			Message: "Internal error",          // Avoid misclassifying a server setup defect as client input failure.
			Data:    "tool schema unavailable", // Provide a concise operator-facing diagnostic without exposing implementation details.
		}
	}
	if err := schema.Validate(args); err != nil { // Evaluate the complete argument value with JSON Schema 2020-12 semantics.
		return &MCPError{ // Preserve the stable MCP error shape expected by existing invalid-argument tests and clients.
			Code:    -32602,                                                    // Identify schema constraint violations as invalid parameters.
			Message: "Schema validation failed",                                // Keep the established client-facing classification unchanged.
			Data:    errors.New(errors.CodeSchemaInvalid, err.Error()).Error(), // Wrap the detailed validator path in the repository's schema error code.
		}
	}
	return nil // Confirm that the selected tool exists and all supplied arguments satisfy its advertised schema.
}

// MCPError represents an MCP protocol error
type MCPError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Error executes this operation.
func (e *MCPError) Error() string {
	return e.Message
}
