# MCP Mobile Worker - Appium 自动化测试平台

> **版本**: v4.3
> **协议支持**: 标准 MCP (Model Context Protocol) + JSON-RPC + REST + gRPC + WebSocket

## 概述

MCP Mobile Worker 是一个面向 AI Agent 的统一移动测试执行平台，支持 iOS 和 Android 自动化测试。通过标准 MCP 协议，可直接被 Claude Desktop 等 AI 工具发现和调用。

### v4.3 新特性

- ✅ **标准 MCP 协议支持**：实现 Anthropic MCP 标准（协议版本 2024-11-05）
- ✅ **stdio 传输模式**：通过 `gateway --stdio` 启动，支持 Claude Desktop 配置
- ✅ **MCP Tools 注册表**：8 个核心业务方法作为 Tools 暴露给 LLM
- ✅ **向后兼容**：保持原有 HTTP/REST/gRPC/WebSocket 接口不变

### 快速开始

#### 1. 作为 MCP Server 使用（Claude Desktop 集成）

在 Claude Desktop 配置文件中添加：

```json
{
  "mcpServers": {
    "appium-mobile-testing": {
      "command": "/path/to/gateway",
      "args": ["--stdio"],
      "env": {
        "CONFIG_PATH": "/path/to/config.yaml"
      }
    }
  }
}
```

配置文件位置：
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

#### 2. 作为 HTTP 服务使用

```bash
# 启动 HTTP 服务器模式（默认）
./gateway --config config.yaml

# 访问 JSON-RPC 端点
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/list",
    "params": {},
    "id": 1
  }'
```

### MCP Tools 列表

通过 MCP 协议可用的工具：

1. **startSession** - 创建移动设备测试会话
2. **executePlan** - 执行自动化测试计划
3. **endSession** - 关闭会话释放资源
4. **getSemanticSnapshot** - 获取 UI 元素树快照
5. **takeScreenshot** - 对设备屏幕截图
6. **cancelPlan** - 取消正在执行的计划
7. **getTrace** - 获取执行记录详情
8. **healthCheck** - 检查服务健康状态

### 文档

- [需求说明 v4.3](./requirements.v4.3.md)
- [架构设计 v4.3](./module_v4.3_design.md)
- [详细文档](./appium_mcp_docs_v4_3_detailed/)

## 系统架构

flowchart TD
  %% ========= Ingress =========
  A[Client<br/>REST / JSON-RPC] --> B[Gateway]
  A2[Client<br/>WebSocket] --> WS[WS Hub]

  subgraph G1[Gateway]
    B --> B1[Auth 中间件<br/>HMAC / OIDC / PAT]
    B1 --> B2[RateLimit / 配额探测]
    B2 --> B3{Idempotency-Key 命中?}
    B3 -- 是 --> B3r[返回缓存结果]
    B3 -- 否 --> B4[Schema 校验<br/>JSON-RPC/REST]
    B4 --> B5[路由: /sessions /plans:execute /cancel ...]
  end

  %% ========= Orchestrator =========
  B5 --> O[Orchestrator]
  subgraph O1[Orchestrator]
    O --> O0[DAO / Cache / S3 Client 就绪检查]
    O0 --> O1a[CreateTrace / 事务]
    O1a --> O2[EventsPublisher<br/>插入事件(PlanQueued)]
    O2 --> O3[Dispatcher<br/>写入 Redis Streams 分片]
    O3 --> O4[WorkerRegistry.Assign<br/>选择合适 Worker/ADF]
    O4 --> O5{ADF 租赁超时?}
    O5 -- 是 --> O5f[Fallback=本地执行<br/>事件 phase=fallback]
    O5 -- 否 --> O6[派发作业给 Worker]
  end

  %% ========= Worker 执行 =========
  O6 --> W[Worker]
  subgraph W1[Worker]
    W --> W0[并发控制器 Acquire]
    W0 --> W1a[Appium Client<br/>会话建立/恢复]
    W1a --> W2[执行计划步骤循环]
    subgraph STEP[Step 执行]
      direction TB
      S1[定位策略矩阵<br/>and/near→视觉兜底] --> S2[动作执行]
      S2 --> S3[可选验证/失败截图]
      S3 --> S4[emit PlanEvent<br/>非阻塞/背压保护]
    end
    W2 --> STEP --> W3[心跳/指标上报]
    W3 --> W4{取消/超时?}
    W4 -- 是 --> W4c[停止≤1s；最终事件=PlanCanceled]
    W4 -- 否 --> W5[完成/失败；最终事件=PlanFinished/Failed]
    W5 --> W6[并发控制器 Release]
  end

  %% ========= 事件与制品 =========
  W4c -->|事件流| EDB[(Postgres<br/>Traces/Events)]
  W5 -->|事件流| EDB
  W -->|Screenshot/Logs 大文件| S3[S3 对象存储]
  S3 -.-> O7[CompleteArtifactUpload<br/>ETag/SHA256 校验]

  %% ========= 发布与补偿 =========
  EDB --> PUB[EventsPublisher<br/>广播触发]
  PUB --> WS
  WS -->|实时 at-most-once| A2[Client WS 订阅者]
  EDB --> A3[GetEvents(sinceId)<br/>(A2A 补偿)]

  %% ========= 观测与链路 =========
  B & O & W --> M[Metrics / Prometheus]
  B & O & W --> T[OTEL Tracing]
  classDef faded fill:#f7f7f7,stroke:#bbb,color:#333;

  %% notes
  class S3,T,M faded
