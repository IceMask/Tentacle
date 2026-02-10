package main

import (
	"encoding/json"
	"fmt"
	"mcp_for_appium/internal/gateway/mcp"
)

func main() {
	// Create tool registry
	registry := mcp.NewToolRegistry()

	// List all tools
	tools := registry.List()

	fmt.Printf("Loaded %d MCP tools:\n\n", len(tools))

	for i, tool := range tools {
		fmt.Printf("%d. %s\n", i+1, tool.Name)

		// Show description (truncated if too long)
		desc := tool.Description
		if len(desc) > 150 {
			desc = desc[:150] + "..."
		}
		fmt.Printf("   Description: %s\n", desc)

		// Show required parameters
		if schema, ok := tool.InputSchema["properties"].(map[string]interface{}); ok {
			if required, ok := tool.InputSchema["required"].([]interface{}); ok && len(required) > 0 {
				fmt.Printf("   Required params: ")
				for j, r := range required {
					if j > 0 {
						fmt.Printf(", ")
					}
					fmt.Printf("%v", r)
				}
				fmt.Printf("\n")
			}

			// Show parameter count
			fmt.Printf("   Total parameters: %d\n", len(schema))
		}

		fmt.Println()
	}

	// Test a sample tool validation
	fmt.Println("Testing tool validation...")
	testArgs := json.RawMessage(`{"sessionId": "test123", "strategy": "id", "selector": "button"}`)
	err := registry.Validate("findElement", testArgs)
	if err != nil {
		fmt.Printf("❌ Validation failed: %v\n", err)
	} else {
		fmt.Printf("✓ findElement validation passed\n")
	}

	// Test invalid args
	invalidArgs := json.RawMessage(`{"sessionId": "test123"}`) // missing required fields
	err = registry.Validate("findElement", invalidArgs)
	if err != nil {
		fmt.Printf("✓ Invalid args correctly rejected: %v\n", err)
	} else {
		fmt.Printf("❌ Should have rejected invalid args\n")
	}
}
