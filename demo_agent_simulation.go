//go:build tools
// +build tools

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"mcp_for_appium/internal/gateway/mcp"
)

// AgentSimulator simulates an AI agent using MCP tools
type AgentSimulator struct {
	handler *mcp.MCPHandler
	ctx     context.Context
}

// NewAgentSimulator executes this operation.
func NewAgentSimulator() *AgentSimulator {
	return &AgentSimulator{
		handler: mcp.NewMCPHandler(nil), // nil orchestrator for protocol testing
		ctx:     context.Background(),
	}
}

// callTool simulates calling an MCP tool
func (a *AgentSimulator) callTool(name string, args map[string]interface{}) (interface{}, error) {
	params := map[string]interface{}{
		"name":      name,
		"arguments": args,
	}
	paramsJSON, _ := json.Marshal(params)
	return a.handler.ToolsCall(a.ctx, paramsJSON)
}

// listTools discovers available tools
func (a *AgentSimulator) listTools() ([]mcp.Tool, error) {
	resp, err := a.handler.ToolsList(a.ctx, json.RawMessage(`{}`))
	if err != nil {
		return nil, err
	}
	respMap := resp.(map[string]interface{})
	tools := respMap["tools"].([]mcp.Tool)
	return tools, nil
}

// thinkAndExplain simulates agent's reasoning process
func (a *AgentSimulator) thinkAndExplain(thought string) {
	fmt.Printf("\n💭 Agent思考: %s\n", thought)
	time.Sleep(300 * time.Millisecond) // Simulate thinking
}

// executeStep simulates agent executing a tool with explanation
func (a *AgentSimulator) executeStep(action, toolName string, args map[string]interface{}) {
	fmt.Printf("\n🔧 Agent行动: %s\n", action)
	fmt.Printf("   调用工具: %s\n", toolName)

	// Show arguments
	argsJSON, _ := json.MarshalIndent(args, "   ", "  ")
	fmt.Printf("   参数:\n   %s\n", string(argsJSON))

	result, err := a.callTool(toolName, args)

	if err != nil {
		mcpErr, ok := err.(*mcp.MCPError)
		if ok {
			fmt.Printf("   ❌ 错误: %s (code: %d)\n", mcpErr.Message, mcpErr.Code)
			if mcpErr.Data != nil {
				fmt.Printf("   详情: %v\n", mcpErr.Data)
			}
		} else {
			fmt.Printf("   ❌ 错误: %v\n", err)
		}
	} else {
		fmt.Printf("   ✅ 成功")
		// Show abbreviated result
		resultJSON, _ := json.Marshal(result)
		resultStr := string(resultJSON)
		if len(resultStr) > 200 {
			resultStr = resultStr[:200] + "..."
		}
		fmt.Printf(": %s\n", resultStr)
	}

	time.Sleep(200 * time.Millisecond)
}

// main is the entry point for this binary.
func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║          AI Agent 模拟器 - 使用 MCP for Appium                            ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════════════════╝")

	agent := NewAgentSimulator()

	// Phase 1: Discovery
	fmt.Println("\n" + strings.Repeat("═", 80))
	fmt.Println("阶段 1: 工具发现 (Tool Discovery)")
	fmt.Println(strings.Repeat("═", 80))

	agent.thinkAndExplain("我需要先了解有哪些可用的工具来完成移动应用测试任务")

	tools, err := agent.listTools()
	if err != nil {
		fmt.Printf("❌ 无法列出工具: %v\n", err)
		return
	}

	fmt.Printf("\n✓ 发现了 %d 个可用工具\n", len(tools))
	fmt.Println("\n工具清单:")

	// Group by category for better understanding
	categories := map[string][]string{
		"会话管理": {},
		"批量执行": {},
		"元素操作": {},
		"手势输入": {},
		"实用工具": {},
	}

	for _, tool := range tools {
		switch {
		case tool.Name == "startSession" || tool.Name == "endSession":
			categories["会话管理"] = append(categories["会话管理"], tool.Name)
		case tool.Name == "executePlan" || tool.Name == "cancelPlan" || tool.Name == "getTrace":
			categories["批量执行"] = append(categories["批量执行"], tool.Name)
		case strings.Contains(tool.Name, "Element"):
			categories["元素操作"] = append(categories["元素操作"], tool.Name)
		case tool.Name == "tap" || tool.Name == "swipe" || tool.Name == "longPress" ||
			tool.Name == "pressBack" || tool.Name == "hideKeyboard":
			categories["手势输入"] = append(categories["手势输入"], tool.Name)
		default:
			categories["实用工具"] = append(categories["实用工具"], tool.Name)
		}
	}

	for category, toolNames := range categories {
		if len(toolNames) > 0 {
			fmt.Printf("  • %s: %s\n", category, strings.Join(toolNames, ", "))
		}
	}

	// Phase 2: Interactive Testing Scenario
	fmt.Println("\n" + strings.Repeat("═", 80))
	fmt.Println("阶段 2: 交互式测试场景 (Interactive Testing)")
	fmt.Println(strings.Repeat("═", 80))

	agent.thinkAndExplain("假设我要测试一个登录功能，我会按照以下步骤进行：\n" +
		"   1. 启动测试会话\n" +
		"   2. 获取UI快照理解界面结构\n" +
		"   3. 查找用户名输入框\n" +
		"   4. 输入用户名\n" +
		"   5. 查找密码输入框\n" +
		"   6. 输入密码\n" +
		"   7. 查找并点击登录按钮\n" +
		"   8. 截图保存结果\n" +
		"   9. 结束会话")

	// Step 1: Start Session
	agent.executeStep(
		"启动Android模拟器测试会话",
		"startSession",
		map[string]interface{}{
			"projectId": "login-test-project",
			"w3cCapsJson": json.RawMessage(`{
				"platformName": "Android",
				"deviceName": "emulator-5554",
				"appPackage": "com.example.app",
				"appActivity": ".MainActivity",
				"automationName": "UiAutomator2"
			}`),
			"tags": map[string]string{
				"feature": "login",
				"env":     "staging",
			},
		},
	)

	agent.thinkAndExplain("由于没有真实的Appium服务，这个调用会失败。\n" +
		"   但在真实场景中，这会返回一个sessionId供后续使用。")

	// Step 2: Get Semantic Snapshot (would fail without orchestrator)
	agent.executeStep(
		"获取当前界面的语义快照，理解UI结构",
		"getSemanticSnapshot",
		map[string]interface{}{
			"sessionId": "mock-session-123",
		},
	)

	// Step 3: Find username field
	agent.executeStep(
		"查找用户名输入框（使用accessibility id策略）",
		"findElement",
		map[string]interface{}{
			"sessionId": "mock-session-123",
			"strategy":  "accessibility id",
			"selector":  "username_field",
		},
	)

	// Step 4: Type username
	agent.executeStep(
		"在用户名输入框中输入文本",
		"sendKeysToElement",
		map[string]interface{}{
			"sessionId": "mock-session-123",
			"elementId": "element-001",
			"text":      "testuser@example.com",
		},
	)

	// Step 5: Find password field
	agent.executeStep(
		"查找密码输入框（使用xpath策略）",
		"findElement",
		map[string]interface{}{
			"sessionId": "mock-session-123",
			"strategy":  "xpath",
			"selector":  "//android.widget.EditText[@resource-id='password']",
		},
	)

	// Step 6: Type password
	agent.executeStep(
		"在密码输入框中输入密码",
		"sendKeysToElement",
		map[string]interface{}{
			"sessionId": "mock-session-123",
			"elementId": "element-002",
			"text":      "SecureP@ssw0rd",
		},
	)

	// Step 7: Hide keyboard
	agent.executeStep(
		"隐藏软键盘，确保登录按钮可见",
		"hideKeyboard",
		map[string]interface{}{
			"sessionId": "mock-session-123",
		},
	)

	// Step 8: Find and verify login button
	agent.executeStep(
		"查找登录按钮并验证其是否可点击",
		"findElement",
		map[string]interface{}{
			"sessionId": "mock-session-123",
			"strategy":  "id",
			"selector":  "login_button",
		},
	)

	agent.executeStep(
		"检查登录按钮是否显示",
		"isElementDisplayed",
		map[string]interface{}{
			"sessionId": "mock-session-123",
			"elementId": "element-003",
		},
	)

	// Step 9: Click login button
	agent.executeStep(
		"点击登录按钮",
		"clickElement",
		map[string]interface{}{
			"sessionId": "mock-session-123",
			"elementId": "element-003",
		},
	)

	// Step 10: Take screenshot
	agent.executeStep(
		"截图保存登录后的界面状态",
		"takeScreenshot",
		map[string]interface{}{
			"sessionId":    "mock-session-123",
			"traceId":      "trace-login-test-001",
			"includeThumb": true,
		},
	)

	// Step 11: End session
	agent.executeStep(
		"结束测试会话，释放资源",
		"endSession",
		map[string]interface{}{
			"sessionId": "mock-session-123",
		},
	)

	// Phase 3: Batch Testing Scenario
	fmt.Println("\n" + strings.Repeat("═", 80))
	fmt.Println("阶段 3: 批量测试场景 (Batch Testing)")
	fmt.Println(strings.Repeat("═", 80))

	agent.thinkAndExplain("对于已知的测试流程，我可以使用executePlan一次性执行多个步骤，\n" +
		"   这更适合CI/CD场景中的回归测试。")

	testPlan := map[string]interface{}{
		"steps": []map[string]interface{}{
			{
				"action":   "click",
				"strategy": "id",
				"selector": "username_field",
			},
			{
				"action": "sendKeys",
				"text":   "testuser@example.com",
			},
			{
				"action":   "click",
				"strategy": "id",
				"selector": "password_field",
			},
			{
				"action": "sendKeys",
				"text":   "SecureP@ssw0rd",
			},
			{
				"action":   "click",
				"strategy": "id",
				"selector": "login_button",
			},
			{
				"action": "wait",
				"condition": map[string]interface{}{
					"type":    "element",
					"locator": "home_screen_title",
				},
			},
			{
				"action": "screenshot",
			},
		},
	}

	planJSON, _ := json.Marshal(testPlan)

	agent.executeStep(
		"执行完整的登录测试计划（7个步骤）",
		"executePlan",
		map[string]interface{}{
			"sessionId": "mock-session-456",
			"plan":      json.RawMessage(planJSON),
			"traceId":   "trace-batch-login-001",
		},
	)

	// Phase 4: Advanced Gestures
	fmt.Println("\n" + strings.Repeat("═", 80))
	fmt.Println("阶段 4: 高级手势操作 (Advanced Gestures)")
	fmt.Println(strings.Repeat("═", 80))

	agent.thinkAndExplain("有些场景需要更复杂的手势操作，比如滑动、长按等。")

	agent.executeStep(
		"向上滑动页面（从屏幕底部滑到顶部）",
		"swipe",
		map[string]interface{}{
			"sessionId":  "mock-session-789",
			"startX":     500,
			"startY":     1500,
			"endX":       500,
			"endY":       300,
			"durationMs": 800,
		},
	)

	agent.executeStep(
		"长按某个元素以显示上下文菜单",
		"longPress",
		map[string]interface{}{
			"sessionId":  "mock-session-789",
			"elementId":  "element-005",
			"durationMs": 1000,
		},
	)

	agent.executeStep(
		"在特定坐标点击（例如点击广告关闭按钮）",
		"tap",
		map[string]interface{}{
			"sessionId": "mock-session-789",
			"x":         50,
			"y":         50,
		},
	)

	agent.executeStep(
		"按返回键退出当前页面",
		"pressBack",
		map[string]interface{}{
			"sessionId": "mock-session-789",
		},
	)

	// Phase 5: Error Handling
	fmt.Println("\n" + strings.Repeat("═", 80))
	fmt.Println("阶段 5: 错误处理演示 (Error Handling)")
	fmt.Println(strings.Repeat("═", 80))

	agent.thinkAndExplain("作为智能Agent，我需要能够处理各种错误情况。")

	agent.executeStep(
		"尝试调用不存在的工具（预期失败）",
		"nonExistentTool",
		map[string]interface{}{
			"param": "value",
		},
	)

	agent.executeStep(
		"使用无效参数调用findElement（缺少必需字段）",
		"findElement",
		map[string]interface{}{
			"sessionId": "test",
			// Missing: strategy, selector
		},
	)

	agent.executeStep(
		"使用无效的strategy值",
		"findElement",
		map[string]interface{}{
			"sessionId": "test",
			"strategy":  "invalid-strategy",
			"selector":  "button",
		},
	)

	// Phase 6: Resource Exploration
	fmt.Println("\n" + strings.Repeat("═", 80))
	fmt.Println("阶段 6: 资源探索 (Resource Exploration)")
	fmt.Println(strings.Repeat("═", 80))

	agent.thinkAndExplain("MCP协议还支持资源(Resources)，让我可以访问测试相关的数据。")

	resp, _ := agent.handler.ResourcesList(agent.ctx, json.RawMessage(`{}`))
	jsonData, _ := json.Marshal(resp)
	var respMap map[string]interface{}
	json.Unmarshal(jsonData, &respMap)
	resources := respMap["resources"].([]interface{})

	fmt.Printf("\n📦 可用资源类型 (%d个):\n", len(resources))
	for _, res := range resources {
		resMap := res.(map[string]interface{})
		fmt.Printf("  • %s\n", resMap["uri"])
		fmt.Printf("    名称: %s\n", resMap["name"])
		fmt.Printf("    描述: %s\n", resMap["description"])
		fmt.Println()
	}

	// Summary
	fmt.Println(strings.Repeat("═", 80))
	fmt.Println("🎯 模拟总结")
	fmt.Println(strings.Repeat("═", 80))

	fmt.Println("\n本次模拟演示了AI Agent如何使用MCP for Appium进行移动应用测试：\n")

	fmt.Println("✅ 完成的操作:")
	fmt.Println("  1. 工具发现 - 列出所有20个可用工具")
	fmt.Println("  2. 交互式测试 - 模拟完整登录流程（11个步骤）")
	fmt.Println("  3. 批量测试 - 使用executePlan执行预定义测试计划")
	fmt.Println("  4. 高级手势 - 演示滑动、长按、点击等操作")
	fmt.Println("  5. 错误处理 - 展示如何处理工具调用失败")
	fmt.Println("  6. 资源探索 - 发现可用的测试资源类型")

	fmt.Println("\n💡 Agent能力展示:")
	fmt.Println("  • 自主发现可用工具并理解其功能")
	fmt.Println("  • 将高层任务（\"测试登录\"）分解为具体操作")
	fmt.Println("  • 灵活选择交互式或批量模式")
	fmt.Println("  • 处理错误并理解错误原因")
	fmt.Println("  • 访问测试资源获取更多信息")

	fmt.Println("\n⚠️  实际使用限制:")
	fmt.Println("  • 当前模拟仅测试MCP协议层，未连接真实服务")
	fmt.Println("  • 真实场景需要: Postgres + Redis + S3 + Appium Server")
	fmt.Println("  • 工具调用会失败，但展示了完整的交互流程")

	fmt.Println("\n🚀 下一步:")
	fmt.Println("  • 集成到Claude Desktop作为MCP Server")
	fmt.Println("  • 部署完整后端服务栈")
	fmt.Println("  • 连接真实的Appium服务器和移动设备")
	fmt.Println("  • 让AI Agent进行真实的移动应用测试")

	fmt.Println("\n" + strings.Repeat("═", 80))
}
