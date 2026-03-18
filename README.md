# MCP Mobile Worker - Appium 自动化测试平台

> **版本**: v4.3
> **协议支持**: 标准 MCP (Model Context Protocol) + JSON-RPC + REST + gRPC + WebSocket

## 概述

MCP Mobile Worker 是一个面向 AI Agent 的统一移动测试执行平台，支持 iOS 和 Android 自动化测试。通过标准 MCP 协议，可直接被 Claude Desktop 等 AI 工具发现和调用。

### v4.3 新特性

- ✅ **标准 MCP 协议支持**：实现 Anthropic MCP 标准（协议版本 2024-11-05）
- ✅ **stdio 传输模式**：通过 `gateway --stdio` 启动，支持 Claude Desktop 配置
- ✅ **MCP Tools 注册表**：26 个工具（会话/执行/元素/手势/实用/Device Farm/调试）暴露给 LLM
- ✅ **向后兼容**：保持原有 HTTP/REST/gRPC/WebSocket 接口不变

### 当前运行边界

- 推荐运行模式：`monolith`
- `distributed`：**experimental**，当前不建议作为生产入口模式
- Gateway 的 HTTP 与 `stdio` 入口当前都以 `monolith` 为正式支持模式
- JSON-RPC 预留方法 `replay` `subscribe` `unsubscribe` 当前仍未实现

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

#### 3. 初始化 PostgreSQL Schema

在首次启动 `gateway` 或 `orchestrator` 之前，先应用仓库自带的 schema migration：

```bash
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/mcp_mobile_worker?sslmode=disable'
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/001_init.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/002_audit_logs.sql
```

迁移目录说明见：

- [internal/storage/postgres/migrations/README.md](./internal/storage/postgres/migrations/README.md)

如需做完整的 migration 回放验证，可以直接运行：

```bash
go run ./cmd/migration_replay_check
```

### MCP Tools 列表

通过 MCP 协议可用的工具：

- 共 **26** 个工具（截至 2026-03-16）
- 核心会话/执行工具：`startSession` `executePlan` `endSession` `cancelPlan` `getTrace`
- 交互式元素工具：`findElement` `clickElement` `sendKeysToElement` `clearElement` `getElementText` `getElementAttribute` `isElementDisplayed`
- 手势工具：`tap` `swipe` `longPress` `pressBack` `hideKeyboard`
- 实用工具：`getSemanticSnapshot` `takeScreenshot` `healthCheck`
- Device Farm 工具：`createDeviceFarmUpload` `getDeviceFarmUpload` `getDeviceFarmRuntimeContext` `scheduleDeviceFarmRun` `getDeviceFarmRun`
- 设备调试工具：`adbShell`

补充说明：

- 上述 **26 个** 名称是当前实际可发现的 MCP tools
- JSON-RPC schema 中还保留了 `replay` `subscribe` `unsubscribe` 三个方法名，但它们当前 **未实现**，不计入可用 MCP tools 集

### Device Farm 参数缓存说明（run_api 模式）

- 服务端按 `projectArn` 维度缓存以下 ARN：`appArn` `testPackageArn` `devicePoolArn`
- 缓存 TTL：**24 小时**
- 上传元数据（用于把 uploadArn 归类为 app/test package）缓存 TTL：**2 小时**
- 调用优先级：客户端显式参数 > 服务端缓存 > 默认/兜底逻辑
- `getDeviceFarmRuntimeContext` 可供客户端 Agent 先探测当前可复用上下文，再决定是否补齐参数

### 文档

- [需求说明 v4.3](./requirements.v4.3.md)
- [架构设计 v4.3](./module_v4.3_design.md)
- [详细文档](./appium_mcp_docs_v4_3_detailed/)
- [PostgreSQL Migrations](./internal/storage/postgres/migrations/README.md)

## 系统架构

```mermaid
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
```
