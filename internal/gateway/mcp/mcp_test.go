package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"mcp_for_appium/internal/orchestrator"
)

// TestMCPIntegration tests the complete MCP protocol flow
func TestMCPIntegration(t *testing.T) {
	// Skip if no real orchestrator (needs dependencies)
	// This is a structure test showing how MCP should work

	t.Run("Initialize", func(t *testing.T) {
		// This is a structure test documenting the MCP initialize flow
		// Expected response structure should contain: protocolVersion, capabilities, serverInfo
		t.Log("MCP initialize should return protocol version, capabilities, and server info")
	})

	t.Run("ToolsList", func(t *testing.T) {
		req := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "tools/list",
			"params":  map[string]interface{}{},
			"id":      2,
		}

		reqJSON, _ := json.Marshal(req)
		t.Logf("tools/list request: %s", reqJSON)

		// Expected: list of tools with name, description, inputSchema
		expectedTools := []string{
			"startSession",
			"executePlan",
			"endSession",
			"getSemanticSnapshot",
			"takeScreenshot",
			"cancelPlan",
			"getTrace",
			"healthCheck",
		}
		t.Logf("Expected tools: %v", expectedTools)
	})

	t.Run("ToolsCall_StartSession", func(t *testing.T) {
		req := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "tools/call",
			"params": map[string]interface{}{
				"name": "startSession",
				"arguments": map[string]interface{}{
					"projectId": "test-project",
					"w3cCapsJson": map[string]interface{}{
						"platformName": "iOS",
						"deviceName":   "iPhone 13",
					},
				},
			},
			"id": 3,
		}

		reqJSON, _ := json.Marshal(req)
		t.Logf("tools/call (startSession) request: %s", reqJSON)

		// Expected response format
		expectedResponse := map[string]interface{}{
			"content": []map[string]interface{}{
				{
					"type": "text",
					"text": "{...sessionId...}",
				},
			},
			"isError": false,
		}
		respJSON, _ := json.Marshal(expectedResponse)
		t.Logf("Expected response format: %s", respJSON)
	})

	t.Run("ToolsCall_ExecutePlan", func(t *testing.T) {
		req := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "tools/call",
			"params": map[string]interface{}{
				"name": "executePlan",
				"arguments": map[string]interface{}{
					"sessionId": "sess_123",
					"plan": map[string]interface{}{
						"steps": []map[string]interface{}{
							{
								"type":     "click",
								"selector": "//button[@id='submit']",
							},
							{
								"type": "wait",
							},
						},
					},
				},
			},
			"id": 4,
		}

		reqJSON, _ := json.Marshal(req)
		t.Logf("tools/call (executePlan) request: %s", reqJSON)
	})

	t.Run("ResourcesList", func(t *testing.T) {
		req := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "resources/list",
			"params":  map[string]interface{}{},
			"id":      5,
		}

		reqJSON, _ := json.Marshal(req)
		t.Logf("resources/list request: %s", reqJSON)

		expectedResources := []string{
			"mcp://appium/artifacts/{traceId}",
			"mcp://appium/traces/{traceId}",
			"mcp://appium/sessions/{sessionId}",
		}
		t.Logf("Expected resource URIs: %v", expectedResources)
	})

	t.Run("ResourcesRead", func(t *testing.T) {
		req := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "resources/read",
			"params": map[string]interface{}{
				"uri": "mcp://appium/artifacts/trace_123",
			},
			"id": 6,
		}

		reqJSON, _ := json.Marshal(req)
		t.Logf("resources/read request: %s", reqJSON)
	})
}

// TestToolRegistry tests the tool registry functionality
func TestToolRegistry(t *testing.T) {
	registry := NewToolRegistry()

	t.Run("ListTools", func(t *testing.T) {
		tools := registry.List()
		if len(tools) == 0 {
			t.Fatal("Expected at least one tool to be registered")
		}

		t.Logf("Registered %d tools", len(tools))
		for _, tool := range tools {
			t.Logf("  - %s: %s", tool.Name, tool.Description)
		}
	})

	t.Run("GetTool", func(t *testing.T) {
		tool, exists := registry.Get("startSession")
		if !exists {
			t.Fatal("Expected startSession tool to exist")
		}

		if tool.Name != "startSession" {
			t.Errorf("Expected tool name 'startSession', got '%s'", tool.Name)
		}

		if tool.InputSchema == nil {
			t.Error("Expected tool to have inputSchema")
		}

		schemaJSON, _ := json.MarshalIndent(tool.InputSchema, "", "  ")
		t.Logf("startSession schema:\n%s", schemaJSON)
	})

	t.Run("ValidateArguments", func(t *testing.T) {
		// Valid arguments
		validArgs := json.RawMessage(`{
			"projectId": "test",
			"w3cCapsJson": {
				"platformName": "iOS"
			}
		}`)

		err := registry.Validate("startSession", validArgs)
		if err != nil {
			t.Errorf("Expected valid arguments to pass, got error: %v", err)
		}

		// Missing required field
		invalidArgs := json.RawMessage(`{
			"w3cCapsJson": {
				"platformName": "iOS"
			}
		}`)

		err = registry.Validate("startSession", invalidArgs)
		if err == nil {
			t.Error("Expected validation to fail for missing required field")
		} else {
			t.Logf("Validation correctly failed: %v", err)
		}
	})

	t.Run("GetNonExistentTool", func(t *testing.T) {
		_, exists := registry.Get("nonExistentTool")
		if exists {
			t.Error("Expected nonExistentTool to not exist")
		}
	})
}

// TestMCPHandler tests the MCP handler methods
func TestMCPHandler(t *testing.T) {
	// Create a mock orchestrator (nil for now, as we're testing structure)
	var mockOrch *orchestrator.Service

	handler := NewMCPHandler(mockOrch)

	t.Run("Initialize", func(t *testing.T) {
		params := json.RawMessage(`{
			"protocolVersion": "2024-11-05",
			"capabilities": {},
			"clientInfo": {
				"name": "test-client",
				"version": "1.0.0"
			}
		}`)

		result, err := handler.Initialize(context.Background(), params)
		if err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Fatal("Expected result to be a map")
		}

		if resultMap["protocolVersion"] == nil {
			t.Error("Expected protocolVersion in response")
		}

		if resultMap["capabilities"] == nil {
			t.Error("Expected capabilities in response")
		}

		if resultMap["serverInfo"] == nil {
			t.Error("Expected serverInfo in response")
		}

		resultJSON, _ := json.MarshalIndent(result, "", "  ")
		t.Logf("Initialize response:\n%s", resultJSON)
	})

	t.Run("ToolsList", func(t *testing.T) {
		params := json.RawMessage(`{}`)

		result, err := handler.ToolsList(context.Background(), params)
		if err != nil {
			t.Fatalf("ToolsList failed: %v", err)
		}

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Fatal("Expected result to be a map")
		}

		tools, ok := resultMap["tools"]
		if !ok {
			t.Fatal("Expected 'tools' field in response")
		}

		toolsSlice, ok := tools.([]Tool)
		if !ok {
			t.Fatal("Expected tools to be a slice of Tool")
		}

		if len(toolsSlice) == 0 {
			t.Error("Expected at least one tool")
		}

		t.Logf("Found %d tools:", len(toolsSlice))
		for _, tool := range toolsSlice {
			t.Logf("  - %s", tool.Name)
		}
	})

	t.Run("ResourcesList", func(t *testing.T) {
		params := json.RawMessage(`{}`)

		result, err := handler.ResourcesList(context.Background(), params)
		if err != nil {
			t.Fatalf("ResourcesList failed: %v", err)
		}

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Fatal("Expected result to be a map")
		}

		resources, ok := resultMap["resources"]
		if !ok {
			t.Fatal("Expected 'resources' field in response")
		}

		resourcesSlice, ok := resources.([]map[string]interface{})
		if !ok {
			t.Fatal("Expected resources to be a slice")
		}

		if len(resourcesSlice) == 0 {
			t.Error("Expected at least one resource")
		}

		t.Logf("Found %d resources:", len(resourcesSlice))
		for _, res := range resourcesSlice {
			t.Logf("  - %s", res["uri"])
		}
	})
}

// TestParseResourceURI tests URI parsing for resources/read
func TestParseResourceURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		wantType    string
		wantID      string
		expectError bool
	}{
		{
			name:     "artifacts URI",
			uri:      "mcp://appium/artifacts/trace_abc123",
			wantType: "artifacts",
			wantID:   "trace_abc123",
		},
		{
			name:     "traces URI",
			uri:      "mcp://appium/traces/trace_xyz789",
			wantType: "traces",
			wantID:   "trace_xyz789",
		},
		{
			name:     "sessions URI",
			uri:      "mcp://appium/sessions/sess_def456",
			wantType: "sessions",
			wantID:   "sess_def456",
		},
		{
			name:        "missing prefix",
			uri:         "http://appium/artifacts/trace_123",
			expectError: true,
		},
		{
			name:        "missing id",
			uri:         "mcp://appium/artifacts/",
			expectError: true,
		},
		{
			name:        "missing resource type",
			uri:         "mcp://appium/",
			expectError: true,
		},
		{
			name:        "empty URI",
			uri:         "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resType, id, err := parseResourceURI(tt.uri)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if resType != tt.wantType {
				t.Errorf("resource type: got %q, want %q", resType, tt.wantType)
			}
			if id != tt.wantID {
				t.Errorf("id: got %q, want %q", id, tt.wantID)
			}
		})
	}
}
