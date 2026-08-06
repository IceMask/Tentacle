# MCP Mobile Worker - Appium 自动化测试平台

> **发布阶段**: `v0.x` 预发布
> **当前实现口径**: `v4.4` 需求/设计基线 + MCP `2026-07-28` alignment
> **协议支持**: 标准 MCP (Model Context Protocol) + JSON-RPC + gRPC + WebSocket + HTTP 辅助端点

## 概述

MCP Mobile Worker 是一个面向 AI Agent 的统一移动测试执行平台，支持 iOS 和 Android 自动化测试。通过标准 MCP 协议，可直接被 Claude Desktop 等 AI 工具发现和调用。

### 当前发布定位

- ✅ **标准 MCP 协议支持**：支持最新 `2026-07-28` 无状态协议，并兼容 `2025-11-25` / `2025-06-18` initialize-based 客户端
- ✅ **stdio 传输模式**：通过 `gateway --stdio` 启动，支持 Claude Desktop 配置
- ✅ **Streamable HTTP 传输模式**：通过 `/mcp` 提供标准 MCP HTTP endpoint
- ✅ **MCP Tools 注册表**：内置 26 个工具；启用 `gateway.disable_adb_shell_tool` 后对外暴露 25 个
- ✅ **HTTP 辅助能力**：保留健康检查、指标与浏览器 WebSocket subscription token 签发端点
- ✅ **当前 GA 候选模式**：`monolith`
- ⚠️ **distributed**：功能已补齐 ownership lease、结果回传、worker 恢复、stale-result protection 与端到端回归，但首个稳定版仍明确按 `experimental` 管理

### 当前运行边界

- 推荐运行模式：`monolith`
- `distributed`：**experimental**，`v1.0.0` 首个稳定版仍不作为生产 GA 入口模式
- Gateway 的 HTTP 与 `stdio` 入口当前都以 `monolith` 为正式支持模式
- Gateway 尚未连接外部 orchestrator；standalone orchestrator/worker 当前用于 distributed 集成验证和后续部署接线
- JSON-RPC 预留方法 `replay` `subscribe` `unsubscribe` 当前仍未实现

### 功能矩阵

| 能力 | 当前级别 | 说明 |
| --- | --- | --- |
| `monolith` 执行模式 | `GA candidate` | 当前推荐的正式运行模式 |
| `distributed` 执行模式 | `experimental` | 已具备结果回传、lease、worker 恢复与 distributed E2E，但首个稳定版仍按实验特性发布 |
| MCP `stdio` | `GA candidate` | 可供 Claude Desktop 等 MCP 客户端发现和调用 |
| MCP Streamable HTTP `/mcp` | `GA candidate` | `2026-07-28` 仅使用 POST，并校验 `MCP-Protocol-Version`、`Mcp-Method` 与按方法要求的 `Mcp-Name`；同时保留 legacy initialize 路径 |
| HTTP JSON-RPC `/jsonrpc` | `compatibility` | 向后兼容入口，非标准 MCP Streamable HTTP transport |
| HTTP 辅助端点 | `GA candidate` | 用于 `/healthz`、`/metrics` 与浏览器 WebSocket token 签发 |
| WebSocket 事件订阅 | `GA candidate` | 浏览器需先换短时 subscription token，trace 订阅已校验持久化 ownership |
| `replay` / `subscribe` / `unsubscribe` JSON-RPC 方法 | `planned` | schema 预留，尚未实现 |
| PAT / OIDC / HMAC 鉴权 | `GA candidate` | 已有权威模型与安全边界，但仍建议先按受控环境启用 |
| Device Farm `run_api` / `remote_access` | `experimental` | `run_api` 用于上传/调度/查状态，`remote_access` 用于实时 Appium 交互 |
| Screenshot / artifact 持久化 | `GA candidate` | monolith 主链路和 artifact 集成链都已覆盖 |

### 快速开始

#### 1. 准备依赖、配置和 PostgreSQL Schema

本地运行 `gateway` 需要：

- `go.mod` 声明的 Go `1.24.5` 或兼容工具链
- PostgreSQL 和 Redis
- 已创建且可执行 `HeadBucket` / `PutObject` 的 S3 或 S3-compatible bucket

`gateway` 会在监听端口前检查 PostgreSQL、Redis 和 S3 bucket；缺少可访问的 bucket 时会直接启动失败。
实际创建和执行移动会话时，还需要 Appium 2 可执行文件，或一个可访问的 Appium server；仅执行协议发现和健康检查时不要求 Appium 已启动。

```bash
cp config.example.yaml config.yaml
mkdir -p bin
go build -o ./bin/gateway ./cmd/gateway

export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/mcp_mobile_worker?sslmode=disable'
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/001_init.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/002_audit_logs.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/003_reserved_slot.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/004_pat_tokens.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/005_hmac_keys.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/006_trace_execution_state.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/007_trace_session_ownership.sql
```

迁移说明见 [PostgreSQL Migrations](./internal/storage/postgres/migrations/README.md)。

#### 2. 本地开发：作为 MCP Server 使用（Claude Desktop 集成）

在 Claude Desktop 配置文件中添加：

```json
{
  "mcpServers": {
    "appium-mobile-testing": {
      "command": "/path/to/repo/bin/gateway",
      "args": ["--stdio"],
      "env": {
        "CONFIG_PATH": "/path/to/repo/config.yaml"
      }
    }
  }
}
```

配置文件位置：
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

#### 3. 本地开发：作为 HTTP 服务使用

```bash
# 启动 HTTP 服务器模式（默认，本地调试可使用明文 HTTP）
./bin/gateway --config config.yaml

# MCP 2026-07-28 无状态发现（现代协议不再调用 initialize）
curl -X POST http://localhost:8080/mcp \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -H "MCP-Protocol-Version: 2026-07-28" \
  -H "Mcp-Method: server/discover" \
  -d '{
    "jsonrpc": "2.0",
    "method": "server/discover",
    "params": {
      "_meta": {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "local-test", "version": "1.0.0"}
      }
    },
    "id": 1
  }'

# 每个现代请求都重复携带版本和客户端能力，不依赖此前连接状态
curl -X POST http://localhost:8080/mcp \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -H "MCP-Protocol-Version: 2026-07-28" \
  -H "Mcp-Method: tools/list" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/list",
    "params": {
      "_meta": {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {}
      }
    },
    "id": 2
  }'

# 本项目当前支持的 tools/call 和 resources/read 还必须发送与 params.name/params.uri 一致的 Mcp-Name
# legacy 客户端仍可先 initialize，并使用 2025-11-25 或 2025-06-18 的 MCP-Protocol-Version

# 兼容旧集成的 JSON-RPC endpoint 仍保留
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":1}'
```

本地示例默认关闭鉴权；启用 PAT、OIDC 或 HMAC 后，上述 HTTP 请求还需要携带对应凭据。

#### 4. 验证

如需做完整的 migration 回放验证，可以直接运行：

```bash
go run ./cmd/migration_replay_check
```

如需运行最小 monolith 集成链路验证，可以直接运行：

```bash
go test ./internal/integration -count=1 -v
```

完整 integration package 会自动拉起：

- embedded PostgreSQL
- miniredis
- fake Appium HTTP server
- fake S3-compatible HTTP endpoint（artifact 场景）

并真实覆盖：

- `startSession`
- `executePlan`
- `getTrace`
- `endSession`

如需运行 distributed 端到端链路验证，可以直接运行：

```bash
go test ./internal/integration -run TestDistributedStartExecuteTraceEndFlow -count=1 -v
```

### 生产部署默认建议

- 生产环境外部流量应启用 TLS，不建议继续使用 README 上面的本地明文 HTTP 示例作为生产部署参考
- 内部 RPC 推荐至少使用 `tls`，不要把 `insecure` 当成生产默认
- 浏览器 WebSocket 应走短时 subscription token，不应直接暴露长期 PAT / OIDC Bearer
- 生产推荐配置请以 [config.production.example.yaml](./config.production.example.yaml) 为基线，本地联调用 [config.example.yaml](./config.example.yaml) 做显式降级
- 生产示例仍包含占位域名、证书、token 和数据库 DSN；部署前必须替换 secret，并为 PostgreSQL、Redis 与 S3 配置符合环境要求的传输加密和访问控制

### MCP Tools 列表

通过 MCP 协议可用的工具：

- 内置注册表共 **26** 个工具，定义来自 `internal/gateway/mcp/tools/*.json`
- 核心会话/执行工具：`startSession` `executePlan` `endSession` `cancelPlan` `getTrace`
- 交互式元素工具：`findElement` `clickElement` `sendKeysToElement` `clearElement` `getElementText` `getElementAttribute` `isElementDisplayed`
- 手势工具：`tap` `swipe` `longPress` `pressBack` `hideKeyboard`
- 实用工具：`getSemanticSnapshot` `takeScreenshot` `healthCheck`
- Device Farm 工具：`createDeviceFarmUpload` `getDeviceFarmUpload` `getDeviceFarmRuntimeContext` `scheduleDeviceFarmRun` `getDeviceFarmRun`
- `startSession` 在 `devicefarm.mode=remote_access` 时可通过 `w3cCapsJson` 里的 `devicefarm:deviceArn` / `devicefarm:appArn` / `devicefarm:projectArn` / `devicefarm:sessionName` 直连 AWS Device Farm remote access Appium endpoint
- 设备调试工具：`adbShell`

补充说明：

- 本地示例默认可发现上述 **26 个** MCP tools
- 运维可通过 `gateway.disable_adb_shell_tool=true` 完全移除 `adbShell`；生产示例默认启用该开关，因此暴露 **25 个**
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
- [MCP 2026-07-28 升级说明](./MCP_2026_07_28_ALIGNMENT.md)
- [MIT License](./LICENSE)

### 许可证

本项目采用 `MIT` 协议发布，完整文本见 [LICENSE](./LICENSE)。

## 系统架构

```mermaid
flowchart TD
  MCP["MCP client"] -->|"POST /mcp"| G["Gateway"]
  STDIO["Local MCP client"] -->|"stdio"| G
  COMPAT["Compatibility client"] -->|"POST /jsonrpc"| G
  BROWSER["Browser client"] -->|"POST /api/ws/traces/{traceId}/subscription-token"| G
  BROWSER -->|"WS /ws/plan-events"| WS["WebSocket Hub"]

  G -->|"protected HTTP routes"| AUTH["Optional PAT / OIDC / HMAC"]

  subgraph MONO["GA candidate: monolith"]
    AUTH --> O["Embedded Orchestrator"]
    G -->|"stdio direct"| O
    O --> Q["Redis Streams queue"]
    Q --> D["Dispatcher"]
    D --> E["Local plan executor"]
    E --> A["Appium or Device Farm remote access"]
  end

  subgraph DIST["Experimental: standalone distributed stack"]
    SO["Standalone Orchestrator"] --> DQ["Redis Streams dispatcher"]
    DQ -->|"ExecutePlan gRPC"| W["Worker with capacity admission"]
    W --> WA["Appium"]
    W -->|"events, lease renewal, completion gRPC"| SO
  end

  O --> PG[("PostgreSQL sessions, traces, events, audit")]
  SO --> PG
  O --> R[("Redis cache, leases, PubSub")]
  SO --> R
  O --> S3["S3 screenshot artifacts"]
  O --> EP["EventsPublisher"]
  SO --> EP
  EP -->|"durable events"| PG
  EP -->|"best-effort PubSub"| R
  R --> WS
```

当前 gateway 不会把 HTTP 或 stdio 请求转发给 standalone orchestrator，因此图中的 distributed stack 不是现有 gateway 的可选后端。事件先持久化到 PostgreSQL，再 best-effort 发布到 Redis PubSub；WebSocket 是实时 at-most-once 通道，历史事件通过 `getTrace` 读取，而不是旧 A2A REST 路由。`/metrics` 当前与 gateway 共用监听端口，`telemetry.metrics_port` 尚未创建独立 listener；gateway 和 orchestrator 初始化 OTEL tracing，worker 当前使用结构化日志。
