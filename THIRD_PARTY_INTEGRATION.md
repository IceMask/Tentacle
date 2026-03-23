# 第三方集成指南

本文档说明第三方应用和开发者如何集成和使用 MCP Mobile Worker。

## 目录

1. [集成方式概览](#集成方式概览)
2. [方式 1: 标准 MCP 客户端（stdio）](#方式-1-标准-mcp-客户端stdio)
3. [方式 2: HTTP JSON-RPC API](#方式-2-http-json-rpc-api)
4. [方式 3: gRPC 集成](#方式-3-grpc-集成)
5. [方式 4: WebSocket 实时事件](#方式-4-websocket-实时事件)
6. [SDK 和代码库](#sdk-和代码库)

---

## 集成方式概览

| 集成方式 | 适用场景 | 协议 | 实时性 |
|---------|---------|------|--------|
| **MCP stdio** | AI 工具、桌面应用 | 标准 MCP | 否 |
| **HTTP JSON-RPC** | Web 应用、API 调用 | JSON-RPC 2.0 | 否 |
| **gRPC** | 微服务、高性能场景 | gRPC | 否 |
| **WebSocket** | 实时监控、事件订阅 | WebSocket | 是 |

---

## 方式 1: 标准 MCP 客户端（stdio）

### 适用于

- AI 应用（类似 Claude Desktop）
- 命令行工具
- 需要标准 MCP 协议的应用

### 部署方式

#### 选项 A: 子进程模式（推荐）

第三方应用启动 gateway 作为子进程：

**Python 示例**:

```python
import subprocess
import json

class MCPClient:
    def __init__(self, gateway_path, config_path):
        self.process = subprocess.Popen(
            [gateway_path, '--stdio', '--config', config_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

    def send_request(self, method, params, request_id):
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": request_id
        }
        self.process.stdin.write(json.dumps(request) + '\n')
        self.process.stdin.flush()

        # 读取响应
        response_line = self.process.stdout.readline()
        return json.loads(response_line)

    def initialize(self):
        return self.send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "my-app",
                "version": "1.0.0"
            }
        }, 1)

    def list_tools(self):
        return self.send_request("tools/list", {}, 2)

    def call_tool(self, tool_name, arguments):
        return self.send_request("tools/call", {
            "name": tool_name,
            "arguments": arguments
        }, 3)

    def close(self):
        self.process.terminate()
        self.process.wait()

# 使用示例
client = MCPClient('/path/to/gateway', '/path/to/config.yaml')

# 1. 初始化
init_result = client.initialize()
print("Server info:", init_result['result']['serverInfo'])

# 2. 列出工具
tools = client.list_tools()
print("Available tools:", [t['name'] for t in tools['result']['tools']])

# 3. 调用工具
result = client.call_tool("startSession", {
    "projectId": "my-project",
    "w3cCapsJson": {
        "platformName": "iOS",
        "deviceName": "iPhone 13"
    }
})
print("Session created:", result)

client.close()
```

**Node.js 示例**:

```javascript
const { spawn } = require('child_process');
const readline = require('readline');

class MCPClient {
    constructor(gatewayPath, configPath) {
        this.process = spawn(gatewayPath, ['--stdio', '--config', configPath]);
        this.requestId = 0;
        this.pendingRequests = new Map();

        // 设置输出读取
        const rl = readline.createInterface({
            input: this.process.stdout,
            crlfDelay: Infinity
        });

        rl.on('line', (line) => {
            const response = JSON.parse(line);
            const resolver = this.pendingRequests.get(response.id);
            if (resolver) {
                resolver(response);
                this.pendingRequests.delete(response.id);
            }
        });

        // 错误日志（stderr）
        this.process.stderr.on('data', (data) => {
            console.error('Gateway stderr:', data.toString());
        });
    }

    async sendRequest(method, params) {
        const id = ++this.requestId;
        const request = {
            jsonrpc: "2.0",
            method,
            params,
            id
        };

        return new Promise((resolve, reject) => {
            this.pendingRequests.set(id, resolve);
            this.process.stdin.write(JSON.stringify(request) + '\n');

            // 超时处理
            setTimeout(() => {
                if (this.pendingRequests.has(id)) {
                    this.pendingRequests.delete(id);
                    reject(new Error('Request timeout'));
                }
            }, 30000);
        });
    }

    async initialize() {
        return this.sendRequest('initialize', {
            protocolVersion: "2024-11-05",
            capabilities: {},
            clientInfo: {
                name: "my-node-app",
                version: "1.0.0"
            }
        });
    }

    async listTools() {
        return this.sendRequest('tools/list', {});
    }

    async callTool(toolName, arguments) {
        return this.sendRequest('tools/call', {
            name: toolName,
            arguments
        });
    }

    close() {
        this.process.kill();
    }
}

// 使用示例
(async () => {
    const client = new MCPClient('/path/to/gateway', '/path/to/config.yaml');

    try {
        // 初始化
        const init = await client.initialize();
        console.log('Server:', init.result.serverInfo);

        // 列出工具
        const tools = await client.listTools();
        console.log('Tools:', tools.result.tools.map(t => t.name));

        // 调用工具
        const session = await client.callTool('startSession', {
            projectId: 'my-project',
            w3cCapsJson: {
                platformName: 'iOS',
                deviceName: 'iPhone 13'
            }
        });
        console.log('Session:', session);

    } finally {
        client.close();
    }
})();
```

#### 选项 B: 独立服务模式

第三方应用通过 systemd/docker 部署 gateway，然后通过 IPC 连接：

**Docker Compose 示例**:

```yaml
version: '3.8'

services:
  mcp-gateway:
    build: .
    command: /app/gateway --stdio --config /config/config.yaml
    volumes:
      - ./config.yaml:/config/config.yaml
    # stdio 模式不需要暴露端口
    restart: unless-stopped
```

---

## 方式 2: HTTP JSON-RPC API

### 适用于

- Web 应用
- REST API 客户端
- 不支持 stdio 的环境

### 部署方式

#### 启动 HTTP 服务器

```bash
./gateway --config config.yaml
# 默认监听 http://localhost:8080
```

#### Docker 部署

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o gateway ./cmd/gateway

FROM alpine:latest
COPY --from=builder /app/gateway /usr/local/bin/
COPY config.yaml /etc/mcp/config.yaml
EXPOSE 8080
CMD ["gateway", "--config", "/etc/mcp/config.yaml"]
```

```yaml
# docker-compose.yaml
version: '3.8'

services:
  mcp-gateway:
    build: .
    ports:
      - "8080:8080"
    environment:
      - POSTGRES_HOST=postgres
      - REDIS_HOST=redis
    depends_on:
      - postgres
      - redis
      - s3

  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: mcp_mobile_worker
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

volumes:
  postgres_data:
  redis_data:
```

### 客户端示例

#### Python SDK

```python
import requests

class MCPHTTPClient:
    def __init__(self, base_url="http://localhost:8080"):
        self.base_url = base_url
        self.jsonrpc_url = f"{base_url}/jsonrpc"
        self.request_id = 0

    def _call(self, method, params):
        self.request_id += 1
        response = requests.post(self.jsonrpc_url, json={
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.request_id
        })
        response.raise_for_status()
        result = response.json()

        if "error" in result:
            raise Exception(f"RPC Error: {result['error']}")

        return result["result"]

    def list_tools(self):
        return self._call("tools/list", {})

    def call_tool(self, tool_name, arguments):
        return self._call("tools/call", {
            "name": tool_name,
            "arguments": arguments
        })

    def start_session(self, project_id, capabilities):
        result = self.call_tool("startSession", {
            "projectId": project_id,
            "w3cCapsJson": capabilities
        })
        # 解析 MCP 返回的文本内容
        import json
        return json.loads(result["content"][0]["text"])

    def execute_plan(self, session_id, plan):
        result = self.call_tool("executePlan", {
            "sessionId": session_id,
            "plan": plan
        })
        import json
        return json.loads(result["content"][0]["text"])

    def health_check(self):
        result = self.call_tool("healthCheck", {})
        import json
        return json.loads(result["content"][0]["text"])

# 使用示例
client = MCPHTTPClient("http://localhost:8080")

# 健康检查
health = client.health_check()
print("Health:", health)

# 创建会话
session = client.start_session("my-project", {
    "platformName": "iOS",
    "deviceName": "iPhone 13"
})
print("Session ID:", session["sessionId"])

# 执行计划
trace = client.execute_plan(session["sessionId"], {
    "steps": [
        {"type": "click", "selector": "//button[@name='Login']"},
        {"type": "screenshot"}
    ]
})
print("Trace ID:", trace["traceId"])
```

#### JavaScript/TypeScript SDK

```typescript
interface MCPResponse<T> {
    jsonrpc: string;
    result?: T;
    error?: {
        code: number;
        message: string;
        data?: any;
    };
    id: number;
}

interface ToolResult {
    content: Array<{
        type: string;
        text: string;
    }>;
    isError: boolean;
}

class MCPHTTPClient {
    private baseUrl: string;
    private requestId: number = 0;

    constructor(baseUrl: string = 'http://localhost:8080') {
        this.baseUrl = baseUrl;
    }

    private async call<T>(method: string, params: any): Promise<T> {
        const response = await fetch(`${this.baseUrl}/jsonrpc`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                method,
                params,
                id: ++this.requestId
            })
        });

        if (!response.ok) {
            throw new Error(`HTTP error: ${response.status}`);
        }

        const result: MCPResponse<T> = await response.json();

        if (result.error) {
            throw new Error(`RPC Error: ${result.error.message}`);
        }

        return result.result!;
    }

    async listTools() {
        return this.call<{ tools: any[] }>('tools/list', {});
    }

    async callTool(toolName: string, args: any): Promise<ToolResult> {
        return this.call<ToolResult>('tools/call', {
            name: toolName,
            arguments: args
        });
    }

    async startSession(projectId: string, caps: any) {
        const result = await this.callTool('startSession', {
            projectId,
            w3cCapsJson: caps
        });
        return JSON.parse(result.content[0].text);
    }

    async executePlan(sessionId: string, plan: any) {
        const result = await this.callTool('executePlan', {
            sessionId,
            plan
        });
        return JSON.parse(result.content[0].text);
    }

    async healthCheck() {
        const result = await this.callTool('healthCheck', {});
        return JSON.parse(result.content[0].text);
    }
}

// 使用示例
const client = new MCPHTTPClient('http://localhost:8080');

(async () => {
    // 健康检查
    const health = await client.healthCheck();
    console.log('Health:', health);

    // 创建会话
    const session = await client.startSession('my-project', {
        platformName: 'iOS',
        deviceName: 'iPhone 13'
    });
    console.log('Session ID:', session.sessionId);

    // 执行计划
    const trace = await client.executePlan(session.sessionId, {
        steps: [
            { type: 'click', selector: "//button[@name='Login']" },
            { type: 'screenshot' }
        ]
    });
    console.log('Trace ID:', trace.traceId);
})();
```

### OpenAPI/Swagger 规范

提供 OpenAPI 3.0 规范文件，供第三方自动生成客户端：

```yaml
# openapi.yaml
openapi: 3.0.0
info:
  title: MCP Mobile Worker API
  version: 4.3.0
  description: Appium mobile testing platform with MCP protocol support

servers:
  - url: http://localhost:8080
    description: Local development

paths:
  /jsonrpc:
    post:
      summary: JSON-RPC endpoint
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/JSONRPCRequest'
      responses:
        '200':
          description: Successful response
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/JSONRPCResponse'

components:
  schemas:
    JSONRPCRequest:
      type: object
      required:
        - jsonrpc
        - method
        - id
      properties:
        jsonrpc:
          type: string
          enum: ['2.0']
        method:
          type: string
          enum:
            - initialize
            - tools/list
            - tools/call
            - resources/list
            - resources/read
        params:
          type: object
        id:
          oneOf:
            - type: string
            - type: number

    JSONRPCResponse:
      type: object
      required:
        - jsonrpc
        - id
      properties:
        jsonrpc:
          type: string
          enum: ['2.0']
        result:
          type: object
        error:
          $ref: '#/components/schemas/JSONRPCError'
        id:
          oneOf:
            - type: string
            - type: number

    JSONRPCError:
      type: object
      required:
        - code
        - message
      properties:
        code:
          type: integer
        message:
          type: string
        data:
          type: object
```

---

## 方式 3: gRPC 集成

### 适用于

- 微服务架构
- 高性能要求
- 需要强类型的场景

### Proto 定义

```protobuf
// api/proto/mcp/v1/mobile.proto
syntax = "proto3";

package mcp.mobile.v1;

option go_package = "mcp_for_appium/api/proto/mcp/v1";

service MobileTestingService {
  rpc StartSession(StartSessionRequest) returns (StartSessionResponse);
  rpc ExecutePlan(ExecutePlanRequest) returns (stream PlanEvent);
  rpc EndSession(EndSessionRequest) returns (EndSessionResponse);
  rpc GetTrace(GetTraceRequest) returns (GetTraceResponse);
}

message StartSessionRequest {
  string project_id = 1;
  map<string, string> capabilities = 2;
}

message StartSessionResponse {
  string session_id = 1;
  map<string, string> negotiated_capabilities = 2;
}

// ... 其他消息定义
```

### 客户端示例

```python
import grpc
from api.proto.mcp.v1 import mobile_pb2, mobile_pb2_grpc

channel = grpc.insecure_channel('localhost:9090')
stub = mobile_pb2_grpc.MobileTestingServiceStub(channel)

# 创建会话
response = stub.StartSession(mobile_pb2.StartSessionRequest(
    project_id='my-project',
    capabilities={
        'platformName': 'iOS',
        'deviceName': 'iPhone 13'
    }
))
print(f"Session ID: {response.session_id}")

# 执行计划（流式响应）
for event in stub.ExecutePlan(mobile_pb2.ExecutePlanRequest(
    session_id=response.session_id,
    plan=mobile_pb2.Plan(steps=[...])
)):
    print(f"Event: {event.status} - {event.message}")
```

---

## 方式 4: WebSocket 实时事件

### 适用于

- 实时监控
- 事件订阅
- Dashboard/UI 应用

### 连接示例

```javascript
const tokenResp = await fetch('http://localhost:8080/api/ws/traces/trace_123/subscription-token', {
    method: 'POST',
    headers: {
        Authorization: 'Bearer YOUR_TOKEN'
    }
});

const { subscriptionToken } = await tokenResp.json();
const ws = new WebSocket(`ws://localhost:8080/ws/plan-events?subscriptionToken=${subscriptionToken}&traceId=trace_123`);

ws.onopen = () => {
    console.log('Connected to event stream');
};

ws.onmessage = (event) => {
    const planEvent = JSON.parse(event.data);
    console.log('Event:', planEvent);

    // 更新 UI
    updateProgress(planEvent.stepIndex, planEvent.status);
};

ws.onerror = (error) => {
    console.error('WebSocket error:', error);
};

ws.onclose = () => {
    console.log('Connection closed');
    // 使用 JSON-RPC getTrace 补偿获取当前 trace 状态和事件
    fetch('/jsonrpc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            jsonrpc: '2.0',
            method: 'getTrace',
            params: { traceId },
            id: 1
        })
    })
        .then(resp => resp.json())
        .then(payload => {
            (payload.result?.events || []).forEach(updateProgress);
        });
};
```

---

## SDK 和代码库

### 官方 SDK 计划

我们计划为以下语言提供官方 SDK：

- [ ] **Python SDK** (`pip install mcp-mobile-worker`)
- [ ] **JavaScript/TypeScript SDK** (`npm install @mcp/mobile-worker`)
- [ ] **Go SDK** (`go get github.com/yourorg/mcp-mobile-worker-go`)
- [ ] **Java SDK** (Maven/Gradle)

### 社区贡献

欢迎社区为其他语言贡献 SDK！

### SDK 特性

官方 SDK 将包含：

- ✅ 自动重连
- ✅ 请求重试
- ✅ 类型安全
- ✅ 异步支持
- ✅ WebSocket 事件订阅
- ✅ 完整的错误处理
- ✅ 日志和调试

---

## 认证和安全

### API Token

第三方应用需要在请求中携带认证信息：

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -d '{...}'
```

### HMAC 签名

对于高安全要求场景：

```python
import hmac
import hashlib
import time

def sign_request(method, path, body, secret):
    timestamp = str(int(time.time()))
    nonce = generate_nonce()

    message = f"{method}|{path}|{hashlib.sha256(body.encode()).hexdigest()}|{timestamp}|{nonce}"
    signature = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()

    return {
        'X-MCP-Timestamp': timestamp,
        'X-MCP-Nonce': nonce,
        'X-MCP-Signature': signature
    }

# 使用
headers = sign_request('POST', '/jsonrpc', request_body, 'your-secret')
```

---

## 配额和限流

### 查询配额

```bash
echo "当前版本暂未公开单独的 quota HTTP 接口"
```

---

## 支持和资源

### 文档

- [API 参考文档](./appium_mcp_docs_v4_3_detailed/)
- [使用示例](./USAGE_EXAMPLES.md)
- [需求规范](./requirements.v4.3.md)

### 社区

- **GitHub Issues**: 报告问题和请求功能
- **讨论区**: 技术讨论和问答
- **示例代码库**: 查看完整示例

### 商业支持

如需企业级支持，请联系：support@example.com

---

## 常见问题

### Q: 是否需要安装 Appium？

A: 是的，需要单独部署 Appium 服务器。MCP Mobile Worker 通过 HTTP 连接到 Appium。

### Q: 支持多租户吗？

A: 支持。通过 `projectId` 和认证 token 实现租户隔离。

### Q: 性能如何？

A:
- 单个 Worker 支持 5-10 个并发会话
- P95 延迟 < 3s（计划执行）
- 支持水平扩展

### Q: 如何监控服务状态？

A: 提供 Prometheus metrics 端点：`http://localhost:9090/metrics`

### Q: 数据如何备份？

A:
- PostgreSQL：使用标准 pg_dump
- S3 artifacts：启用版本控制和生命周期策略
- Redis：启用 RDB/AOF 持久化
