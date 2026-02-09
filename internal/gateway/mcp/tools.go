package mcp

import (
	"embed"
	"encoding/json"
	"path/filepath"

	"mcp_for_appium/internal/errors"

	"github.com/xeipuuv/gojsonschema"
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
	tools map[string]Tool
}

// NewToolRegistry creates and initializes the tool registry
func NewToolRegistry() *ToolRegistry {
	registry := &ToolRegistry{
		tools: make(map[string]Tool),
	}
	if err := registry.registerBuiltinTools(); err != nil {
		// Log error but don't fail initialization — return empty registry
		// In production, consider panic or return error from constructor
		panic("failed to load MCP tools: " + err.Error())
	}
	return registry
}

// registerBuiltinTools loads all tool definitions from embedded JSON files
func (r *ToolRegistry) registerBuiltinTools() error {
	entries, err := toolsFS.ReadDir("tools")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		raw, err := toolsFS.ReadFile("tools/" + entry.Name())
		if err != nil {
			return err
		}

		var tool Tool
		if err := json.Unmarshal(raw, &tool); err != nil {
			return err
		}

		r.tools[tool.Name] = tool
	}

	return nil
}

// List returns all registered tools
func (r *ToolRegistry) List() []Tool {
	tools := make([]Tool, 0, len(r.tools))
	for _, tool := range r.tools {
		tools = append(tools, tool)
	}
	return tools
}

// Get retrieves a tool by name
func (r *ToolRegistry) Get(name string) (Tool, bool) {
	tool, exists := r.tools[name]
	return tool, exists
}

// Validate validates tool arguments against the schema
func (r *ToolRegistry) Validate(toolName string, arguments json.RawMessage) error {
	tool, exists := r.Get(toolName)
	if !exists {
		return &MCPError{
			Code:    -32601,
			Message: "Tool not found: " + toolName,
		}
	}

	// Validate JSON structure
	var args interface{}
	if err := json.Unmarshal(arguments, &args); err != nil {
		return &MCPError{
			Code:    -32602,
			Message: "Invalid arguments JSON",
			Data:    err.Error(),
		}
	}
	schemaLoader := gojsonschema.NewGoLoader(tool.InputSchema)
	docLoader := gojsonschema.NewBytesLoader(arguments)
	schema, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		return &MCPError{
			Code:    -32602,
			Message: "Invalid tool schema",
			Data:    err.Error(),
		}
	}
	res, err := schema.Validate(docLoader)
	if err != nil {
		return &MCPError{
			Code:    -32602,
			Message: "Schema validation error",
			Data:    err.Error(),
		}
	}
	if !res.Valid() {
		return &MCPError{
			Code:    -32602,
			Message: "Schema validation failed",
			Data:    errors.New(errors.CodeSchemaInvalid, res.Errors()[0].String()).Error(),
		}
	}
	return nil
}

// MCPError represents an MCP protocol error
type MCPError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func (e *MCPError) Error() string {
	return e.Message
}
