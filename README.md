# MCP Mobile Worker - Appium 自动化测试平台

> **发布阶段**: `v0.x` 预发布
> **当前实现口径**: `v4.4` 需求/设计基线
> **协议支持**: 标准 MCP (Model Context Protocol) + JSON-RPC + gRPC + WebSocket + HTTP 辅助端点

## 概述

MCP Mobile Worker 是一个面向 AI Agent 的统一移动测试执行平台，支持 iOS 和 Android 自动化测试。通过标准 MCP 协议，可直接被 Claude Desktop 等 AI 工具发现和调用。

### 当前发布定位

- ✅ **标准 MCP 协议支持**：实现 MCP 标准核心能力（目标协议版本 2025-06-18）
- ✅ **stdio 传输模式**：通过 `gateway --stdio` 启动，支持 Claude Desktop 配置
- ✅ **Streamable HTTP 传输模式**：通过 `/mcp` 提供标准 MCP HTTP endpoint
- ✅ **MCP Tools 注册表**：26 个工具（会话/执行/元素/手势/实用/Device Farm/调试）暴露给 LLM
- ✅ **HTTP 辅助能力**：保留健康检查、指标与浏览器 WebSocket subscription token 签发端点
- ✅ **当前 GA 候选模式**：`monolith`
- ⚠️ **distributed**：功能已补齐 ownership lease、结果回传、worker 恢复、stale-result protection 与端到端回归，但首个稳定版仍明确按 `experimental` 管理

### 当前运行边界

- 推荐运行模式：`monolith`
- `distributed`：**experimental**，`v1.0.0` 首个稳定版仍不作为生产 GA 入口模式
- Gateway 的 HTTP 与 `stdio` 入口当前都以 `monolith` 为正式支持模式
- JSON-RPC 预留方法 `replay` `subscribe` `unsubscribe` 当前仍未实现

### 功能矩阵

| 能力 | 当前级别 | 说明 |
| --- | --- | --- |
| `monolith` 执行模式 | `GA candidate` | 当前推荐的正式运行模式 |
| `distributed` 执行模式 | `experimental` | 已具备结果回传、lease、worker 恢复与 distributed E2E，但首个稳定版仍按实验特性发布 |
| MCP `stdio` | `GA candidate` | 可供 Claude Desktop 等 MCP 客户端发现和调用 |
| MCP Streamable HTTP `/mcp` | `GA candidate` | 标准 MCP HTTP endpoint，要求 post-initialize 请求携带 `MCP-Protocol-Version: 2025-06-18` |
| HTTP JSON-RPC `/jsonrpc` | `compatibility` | 向后兼容入口，非标准 MCP Streamable HTTP transport |
| HTTP 辅助端点 | `GA candidate` | 用于 `/healthz`、`/metrics` 与浏览器 WebSocket token 签发 |
| WebSocket 事件订阅 | `GA candidate` | 浏览器需先换短时 subscription token，trace 订阅已校验持久化 ownership |
| `replay` / `subscribe` / `unsubscribe` JSON-RPC 方法 | `planned` | schema 预留，尚未实现 |
| PAT / OIDC / HMAC 鉴权 | `GA candidate` | 已有权威模型与安全边界，但仍建议先按受控环境启用 |
| Device Farm `run_api` / `remote_access` | `experimental` | `run_api` 用于上传/调度/查状态，`remote_access` 用于实时 Appium 交互 |
| Screenshot / artifact 持久化 | `GA candidate` | monolith 主链路和 artifact 集成链都已覆盖 |

### 快速开始

#### 1. 本地开发：作为 MCP Server 使用（Claude Desktop 集成）

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

#### 2. 本地开发：作为 HTTP 服务使用

```bash
# 启动 HTTP 服务器模式（默认，本地调试可使用明文 HTTP）
./gateway --config config.yaml

# 标准 MCP Streamable HTTP initialize
curl -X POST http://localhost:8080/mcp \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-06-18",
      "capabilities": {},
      "clientInfo": {"name": "local-test", "version": "1.0.0"}
    },
    "id": 1
  }'

# initialize 后的 MCP HTTP 请求需要协议版本 header
curl -X POST http://localhost:8080/mcp \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -H "MCP-Protocol-Version: 2025-06-18" \
  -d '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":2}'

# 兼容旧集成的 JSON-RPC endpoint 仍保留
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":1}'
```

#### 3. 初始化 PostgreSQL Schema

在首次启动 `gateway` 或 `orchestrator` 之前，先应用仓库自带的 schema migration：

```bash
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/mcp_mobile_worker?sslmode=disable'
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/001_init.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/002_audit_logs.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/003_reserved_slot.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/004_pat_tokens.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/005_hmac_keys.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/006_trace_execution_state.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/007_trace_session_ownership.sql
```

迁移目录说明见：

- [internal/storage/postgres/migrations/README.md](./internal/storage/postgres/migrations/README.md)

如需做完整的 migration 回放验证，可以直接运行：

```bash
go run ./cmd/migration_replay_check
```

如需运行最小 monolith 集成链路验证，可以直接运行：

```bash
go test ./internal/integration -count=1 -v
```

这条集成测试会自动拉起：

- embedded PostgreSQL
- miniredis
- fake Appium HTTP server

并真实覆盖：

- `startSession`
- `executePlan`
- `getTrace`
- `endSession`

如需运行 distributed 端到端链路验证，可以直接运行：

```bash
go test ./internal/integration -run TestDistributedFlowEndToEnd -count=1 -v
```

### 生产部署默认建议

- 生产环境外部流量应启用 TLS，不建议继续使用 README 上面的本地明文 HTTP 示例作为生产部署参考
- 内部 RPC 推荐至少使用 `tls`，不要把 `insecure` 当成生产默认
- 浏览器 WebSocket 应走短时 subscription token，不应直接暴露长期 PAT / OIDC Bearer
- 生产推荐配置请以 [config.production.example.yaml](./config.production.example.yaml) 为基线，本地联调用 [config.example.yaml](./config.example.yaml) 做显式降级

### MCP Tools 列表

通过 MCP 协议可用的工具：

- 共 **26** 个工具（截至 2026-03-23）
- 核心会话/执行工具：`startSession` `executePlan` `endSession` `cancelPlan` `getTrace`
- 交互式元素工具：`findElement` `clickElement` `sendKeysToElement` `clearElement` `getElementText` `getElementAttribute` `isElementDisplayed`
- 手势工具：`tap` `swipe` `longPress` `pressBack` `hideKeyboard`
- 实用工具：`getSemanticSnapshot` `takeScreenshot` `healthCheck`
- Device Farm 工具：`createDeviceFarmUpload` `getDeviceFarmUpload` `getDeviceFarmRuntimeContext` `scheduleDeviceFarmRun` `getDeviceFarmRun`
- `startSession` 在 `devicefarm.mode=remote_access` 时可通过 `w3cCapsJson` 里的 `devicefarm:deviceArn` / `devicefarm:appArn` / `devicefarm:projectArn` / `devicefarm:sessionName` 直连 AWS Device Farm remote access Appium endpoint
- 设备调试工具：`adbShell`

补充说明：

- 上述 **26 个** 名称是当前实际可发现的 MCP tools
- 运维可通过 `gateway.disable_adb_shell_tool=true` 完全移除 `adbShell` 的发现与调用入口
- JSON-RPC schema 中还保留了 `replay` `subscribe` `unsubscribe` 三个方法名，但它们当前 **未实现**，不计入可用 MCP tools 集

### Device Farm 参数缓存说明（run_api 模式）

- 服务端按 `projectArn` 维度缓存以下 ARN：`appArn` `testPackageArn` `devicePoolArn`
- 缓存 TTL：**24 小时**
- 上传元数据（用于把 uploadArn 归类为 app/test package）缓存 TTL：**2 小时**
- 调用优先级：客户端显式参数 > 服务端缓存 > 默认/兜底逻辑
- `getDeviceFarmRuntimeContext` 可供客户端 Agent 先探测当前可复用上下文，再决定是否补齐参数

### Device Farm 实时 Appium 会话说明（remote_access 模式）

- `devicefarm.mode=remote_access` 时，`startSession` 会先创建一个 Device Farm remote access session，再使用 AWS 返回的 `remoteDriverEndpoint` 创建 Appium session
- 必填能力：`devicefarm:deviceArn`
- 选填能力：`devicefarm:appArn` `devicefarm:projectArn` `devicefarm:sessionName`
- 兼容说明：旧配置中的 `devicefarm.mode=test_grid` 仍可启动，但在运行时会按 `remote_access` 处理

### 文档

- [需求说明 v4.4](./requirements.v4.4.md)
- [架构设计 v4.4](./module_v4.4_design.md)
- [需求说明 v4.3（历史基线）](./requirements.v4.3.md)
- [架构设计 v4.3（历史基线）](./module_v4.3_design.md)
- [详细文档 v4.3（实现细节参考，需结合 v4.4 阅读）](./appium_mcp_docs_v4_3_detailed/)
- [本地开发配置示例](./config.example.yaml)
- [生产部署配置示例](./config.production.example.yaml)
- [PostgreSQL Migrations](./internal/storage/postgres/migrations/README.md)
- [Changelog](./CHANGELOG.md)
- [Release Notes v1.0.0 Draft](./RELEASE_NOTES_v1.0.0.md)
- [v1.0.0 Release Checklist](./v1.0.0_release_checklist.md)
- [Security Policy](./SECURITY.md)
- [MIT License](./LICENSE)

### 许可证

本项目采用 `MIT` 协议发布，完整文本见 [LICENSE](./LICENSE)。

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
