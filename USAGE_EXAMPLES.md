# MCP Mobile Worker - 使用示例

## 目录

1. [作为 MCP Server 使用（Claude Desktop）](#1-作为-mcp-server-使用claude-desktop)
2. [通过 HTTP JSON-RPC 调用](#2-通过-http-json-rpc-调用)
3. [使用 stdio 模式测试](#3-使用-stdio-模式测试)

---

## 1. 作为 MCP Server 使用（Claude Desktop）

### 配置 Claude Desktop

编辑 Claude Desktop 配置文件：

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

添加配置：

```json
{
  "mcpServers": {
    "appium-mobile-testing": {
      "command": "/absolute/path/to/gateway",
      "args": ["--stdio", "--config", "/absolute/path/to/config.yaml"]
    }
  }
}
```

### 重启 Claude Desktop

重启后，Claude 会自动连接到 MCP Server。

### 在 Claude 中使用

你可以直接向 Claude 请求移动测试任务：

```
请帮我在 iPhone 13 上运行一个测试：
1. 启动会话（iOS 15, iPhone 13）
2. 点击屏幕上的登录按钮
3. 截图
```

Claude 会自动调用相应的 MCP Tools 来完成任务。

---

## 2. 通过 HTTP JSON-RPC 调用

### 启动 HTTP 服务器

```bash
./gateway --config config.yaml
```

### 示例 1: 列出可用工具

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/list",
    "params": {},
    "id": 1
  }'
```

**响应**:
```json
{
  "jsonrpc": "2.0",
  "result": {
    "tools": [
      {
        "name": "startSession",
        "description": "创建一个新的移动设备测试会话，用于连接到 Appium 服务器并初始化设备",
        "inputSchema": {
          "type": "object",
          "properties": {
            "projectId": {
              "type": "string",
              "description": "项目ID，用于标识测试项目"
            },
            "w3cCapsJson": {
              "type": "object",
              "description": "W3C WebDriver 标准的设备能力配置"
            }
          },
          "required": ["projectId", "w3cCapsJson"]
        }
      }
      // ... 其他 7 个工具
    ]
  },
  "id": 1
}
```

### 示例 2: 创建测试会话

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "startSession",
      "arguments": {
        "projectId": "my-test-project",
        "w3cCapsJson": {
          "platformName": "iOS",
          "deviceName": "iPhone 13",
          "platformVersion": "15.0",
          "automationName": "XCUITest",
          "app": "/path/to/your/app.app"
        }
      }
    },
    "id": 2
  }'
```

**响应**:
```json
{
  "jsonrpc": "2.0",
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"sessionId\":\"sess_abc123\",\"capabilities\":{...},\"status\":\"created\"}"
      }
    ],
    "isError": false
  },
  "id": 2
}
```

### 示例 3: 执行测试计划

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "executePlan",
      "arguments": {
        "sessionId": "sess_abc123",
        "plan": {
          "steps": [
            {
              "type": "click",
              "selector": "//XCUIElementTypeButton[@name=\"登录\"]"
            },
            {
              "type": "sendKeys",
              "selector": "//XCUIElementTypeTextField[@name=\"用户名\"]",
              "params": {
                "text": "testuser"
              }
            },
            {
              "type": "screenshot"
            }
          ]
        }
      }
    },
    "id": 3
  }'
```

**响应**:
```json
{
  "jsonrpc": "2.0",
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"traceId\":\"trace_xyz789\",\"status\":\"running\"}"
      }
    ],
    "isError": false
  },
  "id": 3
}
```

### 示例 4: 获取执行记录

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "getTrace",
      "arguments": {
        "traceId": "trace_xyz789"
      }
    },
    "id": 4
  }'
```

### 示例 5: 健康检查

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "healthCheck",
      "arguments": {}
    },
    "id": 5
  }'
```

**响应**:
```json
{
  "jsonrpc": "2.0",
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"status\":\"healthy\",\"checks\":{\"database\":\"ok\",\"redis\":\"ok\",\"appium\":\"ok\"}}"
      }
    ],
    "isError": false
  },
  "id": 5
}
```

---

## 3. 使用 stdio 模式测试

### 启动 stdio 模式

```bash
./gateway --stdio --config config.yaml
```

### 发送 JSON-RPC 请求

在 stdin 中输入（一行）：

```json
{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test-client","version":"1.0.0"}},"id":1}
```

按回车发送。

### 接收响应

从 stdout 读取响应：

```json
{"jsonrpc":"2.0","result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":false},"resources":{"subscribe":false,"listChanged":false}},"serverInfo":{"name":"MCP Mobile Worker","version":"1.0.0"}},"id":1}
```

### 完整测试脚本

创建文件 `test_stdio.sh`:

```bash
#!/bin/bash

# 启动 gateway（后台）
./gateway --stdio --config config.yaml &
GATEWAY_PID=$!

# 等待启动
sleep 2

# 发送 initialize 请求
echo '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}},"id":1}' | \
  ./gateway --stdio --config config.yaml

# 清理
kill $GATEWAY_PID
```

---

## 4. 传统直接方法调用（向后兼容）

### 直接调用 startSession（不通过 tools/call）

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "startSession",
    "params": {
      "projectId": "my-project",
      "caps": {
        "platformName": "iOS",
        "deviceName": "iPhone 13"
      }
    },
    "id": 1
  }'
```

**响应**:
```json
{
  "jsonrpc": "2.0",
  "result": {
    "sessionId": "sess_abc123"
  },
  "id": 1
}
```

---

## 5. 错误处理

### 示例：缺少必填参数

**请求**:
```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "startSession",
      "arguments": {
        "w3cCapsJson": {
          "platformName": "iOS"
        }
      }
    },
    "id": 1
  }'
```

**响应**:
```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32602,
    "message": "Missing required field: projectId"
  },
  "id": 1
}
```

### 示例：工具不存在

**请求**:
```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "nonExistentTool",
      "arguments": {}
    },
    "id": 1
  }'
```

**响应**:
```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32601,
    "message": "Tool not found: nonExistentTool"
  },
  "id": 1
}
```

---

## 6. 完整工作流示例

### iOS 登录测试完整流程

```bash
#!/bin/bash

BASE_URL="http://localhost:8080/jsonrpc"

# 1. 创建会话
SESSION_RESPONSE=$(curl -s -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "startSession",
      "arguments": {
        "projectId": "login-test",
        "w3cCapsJson": {
          "platformName": "iOS",
          "deviceName": "iPhone 13",
          "platformVersion": "15.0",
          "automationName": "XCUITest",
          "app": "/path/to/app.app"
        }
      }
    },
    "id": 1
  }')

# 提取 sessionId（需要 jq 工具）
SESSION_ID=$(echo $SESSION_RESPONSE | jq -r '.result.content[0].text' | jq -r '.sessionId')
echo "Session ID: $SESSION_ID"

# 2. 执行测试计划
TRACE_RESPONSE=$(curl -s -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d "{
    \"jsonrpc\": \"2.0\",
    \"method\": \"tools/call\",
    \"params\": {
      \"name\": \"executePlan\",
      \"arguments\": {
        \"sessionId\": \"$SESSION_ID\",
        \"plan\": {
          \"steps\": [
            {\"type\": \"click\", \"selector\": \"//XCUIElementTypeButton[@name='登录']\"},
            {\"type\": \"sendKeys\", \"selector\": \"//XCUIElementTypeTextField[@name='用户名']\", \"params\": {\"text\": \"testuser\"}},
            {\"type\": \"sendKeys\", \"selector\": \"//XCUIElementTypeSecureTextField[@name='密码']\", \"params\": {\"text\": \"pass123\"}},
            {\"type\": \"click\", \"selector\": \"//XCUIElementTypeButton[@name='提交']\"},
            {\"type\": \"wait\"},
            {\"type\": \"screenshot\"}
          ]
        }
      }
    },
    \"id\": 2
  }")

# 提取 traceId
TRACE_ID=$(echo $TRACE_RESPONSE | jq -r '.result.content[0].text' | jq -r '.traceId')
echo "Trace ID: $TRACE_ID"

# 3. 等待执行完成（轮询）
for i in {1..10}; do
  sleep 2
  TRACE_STATUS=$(curl -s -X POST $BASE_URL \
    -H "Content-Type: application/json" \
    -d "{
      \"jsonrpc\": \"2.0\",
      \"method\": \"tools/call\",
      \"params\": {
        \"name\": \"getTrace\",
        \"arguments\": {
          \"traceId\": \"$TRACE_ID\"
        }
      },
      \"id\": 3
    }")

  STATUS=$(echo $TRACE_STATUS | jq -r '.result.content[0].text' | jq -r '.status')
  echo "Status: $STATUS"

  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ]; then
    break
  fi
done

# 4. 结束会话
curl -s -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d "{
    \"jsonrpc\": \"2.0\",
    \"method\": \"tools/call\",
    \"params\": {
      \"name\": \"endSession\",
      \"arguments\": {
        \"sessionId\": \"$SESSION_ID\"
      }
    },
    \"id\": 4
  }"

echo "Test completed!"
```

---

## 7. 故障排查

### 问题：连接被拒绝

**检查**:
1. Gateway 是否正在运行？
   ```bash
   ps aux | grep gateway
   ```

2. 端口是否被占用？
   ```bash
   lsof -i :8080
   ```

3. 配置文件是否正确？
   ```bash
   ./gateway --config config.yaml 2>&1 | grep -i error
   ```

### 问题：stdio 模式无响应

**检查**:
1. 查看 stderr 日志输出（stdout 是响应）
2. 确保 JSON 请求在一行内
3. 确保请求以换行符结束

### 问题：MCP Server 未出现在 Claude Desktop

**检查**:
1. 配置文件路径是否正确（必须是绝对路径）
2. 二进制文件是否有执行权限
   ```bash
   chmod +x /path/to/gateway
   ```
3. 查看 Claude Desktop 日志（通常在 `~/Library/Logs/Claude/`）

---

## 更多资源

- [需求文档](./requirements.v4.3.md)
- [变更日志](./CHANGELOG.v4.3.md)
- [完整 API 文档](./appium_mcp_docs_v4_3_detailed/)
