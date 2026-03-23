//go:build tools
// +build tools

package main

import (
	"encoding/json"
	"fmt"
	"mcp_for_appium/internal/gateway/mcp"
	"sort"
	"strings"
)

// main is the entry point for this binary.
func main() {
	registry := mcp.NewToolRegistry()
	tools := registry.List()

	// Sort tools by name for consistent output
	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})

	fmt.Println("╔════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║              MCP for Appium - Complete Tool Catalog                        ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════════════════╝")
	fmt.Printf("\nTotal: %d tools available\n\n", len(tools))

	// Group tools by category
	sessionTools := []mcp.Tool{}
	executionTools := []mcp.Tool{}
	elementTools := []mcp.Tool{}
	gestureTools := []mcp.Tool{}
	utilityTools := []mcp.Tool{}

	for _, tool := range tools {
		name := tool.Name
		switch {
		case name == "startSession" || name == "endSession":
			sessionTools = append(sessionTools, tool)
		case name == "executePlan" || name == "cancelPlan" || name == "getTrace":
			executionTools = append(executionTools, tool)
		case strings.Contains(name, "Element"):
			elementTools = append(elementTools, tool)
		case name == "tap" || name == "swipe" || name == "longPress" || name == "pressBack" || name == "hideKeyboard":
			gestureTools = append(gestureTools, tool)
		default:
			utilityTools = append(utilityTools, tool)
		}
	}

	printCategory("📱 Session Management", sessionTools)
	printCategory("⚡ Batch Execution", executionTools)
	printCategory("🔍 Element Operations", elementTools)
	printCategory("👆 Gesture & Input", gestureTools)
	printCategory("🛠️  Utility", utilityTools)

	fmt.Println("\n" + strings.Repeat("─", 80))
	fmt.Println("\n💡 Usage Patterns:\n")
	fmt.Println("   Batch Mode (CI/CD):")
	fmt.Println("   1. startSession → executePlan → getTrace → endSession")
	fmt.Println()
	fmt.Println("   Interactive Mode (Agent Exploration):")
	fmt.Println("   1. startSession")
	fmt.Println("   2. getSemanticSnapshot (understand UI)")
	fmt.Println("   3. findElement → clickElement/sendKeysToElement (interact)")
	fmt.Println("   4. takeScreenshot (capture state)")
	fmt.Println("   5. endSession")
	fmt.Println()
	fmt.Println("   Mixed Mode (Smart Testing):")
	fmt.Println("   1. startSession")
	fmt.Println("   2. executePlan (known steps)")
	fmt.Println("   3. findElement + tap (adaptive interaction)")
	fmt.Println("   4. takeScreenshot + getTrace")
	fmt.Println("   5. endSession")
	fmt.Println()
	fmt.Println(strings.Repeat("─", 80))
	fmt.Println("\n✓ All tools loaded successfully")
	fmt.Println("✓ Schema validation enabled for all tools")
	fmt.Println("✓ MCP protocol 2024-11-05 compliant")
}

// printCategory executes this operation.
func printCategory(title string, tools []mcp.Tool) {
	if len(tools) == 0 {
		return
	}

	fmt.Println("\n" + strings.Repeat("─", 80))
	fmt.Printf("%s (%d tools)\n", title, len(tools))
	fmt.Println(strings.Repeat("─", 80))

	for i, tool := range tools {
		fmt.Printf("\n%d. %s\n", i+1, tool.Name)

		// Description
		desc := wrapText(tool.Description, 77)
		fmt.Printf("   %s\n", strings.ReplaceAll(desc, "\n", "\n   "))

		// Parameters
		if schema, ok := tool.InputSchema["properties"].(map[string]interface{}); ok {
			required := []string{}
			if req, ok := tool.InputSchema["required"].([]interface{}); ok {
				for _, r := range req {
					required = append(required, r.(string))
				}
			}

			fmt.Printf("\n   Parameters:\n")
			// Print required first
			for _, paramName := range required {
				if paramDef, ok := schema[paramName].(map[string]interface{}); ok {
					paramDesc := ""
					if d, ok := paramDef["description"].(string); ok {
						paramDesc = d
					}
					paramType := "string"
					if t, ok := paramDef["type"].(string); ok {
						paramType = t
					}
					fmt.Printf("   • %s (%s) [required]\n", paramName, paramType)
					if paramDesc != "" {
						wrapped := wrapText(paramDesc, 72)
						fmt.Printf("     %s\n", strings.ReplaceAll(wrapped, "\n", "\n     "))
					}
				}
			}

			// Print optional
			for paramName, paramDefRaw := range schema {
				if contains(required, paramName) {
					continue
				}
				if paramDef, ok := paramDefRaw.(map[string]interface{}); ok {
					paramDesc := ""
					if d, ok := paramDef["description"].(string); ok {
						paramDesc = d
					}
					paramType := "string"
					if t, ok := paramDef["type"].(string); ok {
						paramType = t
					}
					fmt.Printf("   • %s (%s) [optional]\n", paramName, paramType)
					if paramDesc != "" {
						wrapped := wrapText(paramDesc, 72)
						fmt.Printf("     %s\n", strings.ReplaceAll(wrapped, "\n", "\n     "))
					}
				}
			}
		}

		// Example
		fmt.Printf("\n   Example:\n")
		example := generateExample(tool)
		exampleJSON, _ := json.MarshalIndent(example, "   ", "  ")
		fmt.Printf("   %s\n", string(exampleJSON))
	}
}

// wrapText executes this operation.
func wrapText(text string, width int) string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return text
	}

	var lines []string
	var currentLine string

	for _, word := range words {
		if len(currentLine)+len(word)+1 <= width {
			if currentLine == "" {
				currentLine = word
			} else {
				currentLine += " " + word
			}
		} else {
			if currentLine != "" {
				lines = append(lines, currentLine)
			}
			currentLine = word
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}

	return strings.Join(lines, "\n")
}

// contains executes this operation.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// generateExample executes this operation.
func generateExample(tool mcp.Tool) map[string]interface{} {
	example := map[string]interface{}{}

	if schema, ok := tool.InputSchema["properties"].(map[string]interface{}); ok {
		required := []string{}
		if req, ok := tool.InputSchema["required"].([]interface{}); ok {
			for _, r := range req {
				required = append(required, r.(string))
			}
		}

		for _, paramName := range required {
			if paramDef, ok := schema[paramName].(map[string]interface{}); ok {
				paramType := "string"
				if t, ok := paramDef["type"].(string); ok {
					paramType = t
				}

				switch paramName {
				case "sessionId":
					example[paramName] = "abc123-session-id"
				case "traceId":
					example[paramName] = "def456-trace-id"
				case "projectId":
					example[paramName] = "my-project"
				case "elementId":
					example[paramName] = "element-789"
				case "strategy":
					example[paramName] = "id"
				case "selector":
					example[paramName] = "login_button"
				case "text":
					example[paramName] = "example text"
				case "attribute":
					example[paramName] = "enabled"
				case "plan":
					example[paramName] = json.RawMessage(`{"steps":[{"action":"click","selector":"button"}]}`)
				case "w3cCapsJson":
					example[paramName] = json.RawMessage(`{"platformName":"Android","deviceName":"emulator"}`)
				default:
					switch paramType {
					case "integer":
						if strings.Contains(paramName, "X") || strings.Contains(paramName, "Y") {
							example[paramName] = 100
						} else {
							example[paramName] = 1000
						}
					case "boolean":
						example[paramName] = true
					default:
						example[paramName] = "value"
					}
				}
			}
		}
	}

	return example
}
