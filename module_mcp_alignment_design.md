# MCP 2025-06-18 对齐设计说明（历史基线）

> 本文档保留 legacy alignment 的设计记录。当前 `2026-07-28` 双时代实现说明请参阅 [MCP_2026_07_28_ALIGNMENT.md](./MCP_2026_07_28_ALIGNMENT.md)。

## 1. 设计目标

- 让 `stdio` 成为符合最新 MCP 规范的标准 transport。
- 将当前 HTTP MCP 能力从“自定义 JSON-RPC endpoint”升级为“标准 Streamable HTTP transport”。
- 在不破坏现有业务工具能力的前提下，升级 tool metadata 与 output schema 能力。
- 将协议级 request / notification 行为与现有业务级 direct methods 明确解耦。

## 2. 当前问题摘要

### 2.1 Transport 层

- `stdio` 将所有入站消息都按 request 处理，notification 也会回包。
- HTTP `/jsonrpc` 只支持简单 request-response，没有 Streamable HTTP 语义。
- HTTP 层没有 `MCP-Protocol-Version` header 校验。

### 2.2 Handler 层

- `ProcessRequest` 缺少：
  - `ping`
  - `notifications/initialized`
  - `notifications/cancelled`
- 默认 unknown method 会把合法 notification 打成协议错误。

### 2.3 Tool Registry 层

- `Tool` 缺少 `title`
- `Tool` 缺少 `annotations`
- tool JSON 文件与最新 schema 不一致

### 2.4 Tool Result 层

- `tools/call` 只返回 `content` 文本
- 缺少 `structuredContent`

## 3. 目标架构

### 3.1 分层

本期按以下 4 层改造：

1. `Message Classification Layer`
   - 区分 request / notification / response
2. `Protocol Dispatch Layer`
   - 处理 lifecycle、ping、cancelled、tools、resources 等 MCP methods
3. `Transport Layer`
   - `stdio`
   - `Streamable HTTP`
4. `Tool Metadata / Result Layer`
   - `title`
   - `annotations`
   - `structuredContent`

## 4. 核心设计

### 4.1 Message Classification

新增统一消息分类规则：

- 带 `id` 且无 `result/error`：request
- 无 `id` 且有 `method`：notification
- 带 `result/error`：response

当前 server 只需要完整处理 request 与 notification；response 可保留为非预期输入防御路径。

建议新增内部结构：

- `IsNotification(req *Request) bool`
- `IsRequest(req *Request) bool`

### 4.2 Notification Handling

建议在 `jsonrpc.Handler` 中新增显式分支：

- `notifications/initialized`
  - 记录 debug/info 日志
  - 返回 `nil, nil`
- `notifications/cancelled`
  - 解析 `requestId`
  - 调用 in-flight request registry 的 cancel
  - 返回 `nil, nil`

transport 层必须识别 notification，并在 handler 返回后禁止写 response。

### 4.3 In-Flight Request Registry

为支持 `notifications/cancelled`，需要新增 in-flight request 管理：

- key: JSON-RPC request id
- value: `context.CancelFunc`

建议实现：

- 在 transport 开始处理 request 时创建子 context
- 注册 `requestId -> cancelFunc`
- request 结束后删除映射
- `notifications/cancelled` 到来时查表并执行 cancel

适用范围：

- `stdio`
- `Streamable HTTP`

### 4.4 Ping

在 `ProcessRequest` 中新增：

- `case "ping": result = map[string]interface{}{}` 

保持最小响应语义，不引入业务逻辑。

### 4.5 Streamable HTTP

建议新增独立 transport，而不是继续在现有 `ServeHTTP` 上打补丁。

推荐新增目录：

- `internal/gateway/mcphttp`

内部职责：

- 解析 MCP HTTP request
- 校验 `MCP-Protocol-Version`
- 区分 request / notification
- 支持 streamable response
- 复用现有 `jsonrpc.Handler`

保留现有 `/jsonrpc` 的策略：

- 短期保留，标记为 compatibility endpoint
- 新标准 MCP HTTP 入口使用新路径，例如 `/mcp`

这样可以降低对现有集成的破坏性。

### 4.6 Protocol Version Negotiation

建议引入一个集中版本常量：

- `SupportedProtocolVersion = "2025-06-18"`

协商策略：

- `stdio`
  - 在 `initialize` 中读取 client `protocolVersion`
  - 若兼容则返回 server accepted version
- `HTTP`
  - 除 `initialize` 外，要求请求头带 `MCP-Protocol-Version`
  - 缺失或不兼容时直接拒绝

### 4.7 Tool Metadata Model

建议扩展 `Tool`：

- `Name`
- `Title`
- `Description`
- `InputSchema`
- `Annotations`
- `OutputSchema`（本期可选预留）

建议增加结构：

- `ToolAnnotations`
  - `Title`
  - `ReadOnlyHint`
  - `DestructiveHint`
  - `OpenWorldHint`

虽然最新 schema 中 `title` 是顶层字段，但保留 `annotations.title` 兼容解析可以提升后续容错性。

### 4.8 Tool Result Model

建议把 `tools/call` success payload 统一改为内部结果结构，再序列化输出：

- `content`
- `structuredContent`
- `isError`

推荐策略：

- 默认继续返回文本 `content`
- 对适合结构化的工具额外填 `structuredContent`

首批建议支持结构化结果的工具：

- `healthCheck`
- `getTrace`
- `getArtifacts`
- `getSemanticSnapshot`
- `takeScreenshot`

## 5. 模块改动建议

### 5.1 `internal/gateway/jsonrpc/handler.go`

需要改：

- 增加 `ping`
- 增加 `notifications/initialized`
- 增加 `notifications/cancelled`
- 增加 request / notification 分支辅助方法
- 让 HTTP handler 能识别 notification 不回包

### 5.2 `internal/gateway/stdio/transport.go`

需要改：

- request 与 notification 分流
- in-flight request registry
- 只对 request 写 response
- `notifications/cancelled` 能触发取消

### 5.3 新增 `internal/gateway/mcphttp`

职责：

- 标准 Streamable HTTP transport
- header 校验
- response streaming

### 5.4 `internal/gateway/mcp/tools.go`

需要改：

- 扩展 `Tool` 结构
- registry 支持新字段反序列化

### 5.5 `internal/gateway/mcp/tools/*.json`

需要改：

- 为全部工具补 `title`
- 为全部工具补最小 `annotations`

### 5.6 `internal/gateway/mcp/handler.go`

需要改：

- `initialize` 返回最新 protocol version
- `tools/call` 支持 `structuredContent`

### 5.7 `cmd/gateway/main.go`

需要改：

- 注册新 MCP HTTP transport 路由
- 决定旧 `/jsonrpc` compatibility route 的保留策略

## 6. 测试策略

### 6.1 单元测试

- handler:
  - `ping`
  - initialized notification
  - cancelled notification
- tools:
  - `title`
  - `annotations`
  - `structuredContent`

### 6.2 transport 测试

- stdio:
  - notification 不回包
  - cancelled 中止长请求
- HTTP:
  - version header 校验
  - Streamable HTTP 正常 request
  - Streamable HTTP notification

### 6.3 回归测试

- 现有 `tools/list`
- 现有 `tools/call`
- progress notification
- legacy direct methods

## 7. 风险与取舍

### 7.1 `/jsonrpc` 是否保留

建议：

- 本期保留
- 但明确降级为 compatibility endpoint

原因：

- 可以避免一次性破坏现有外部集成
- 标准 MCP 客户端逐步迁移到 `/mcp`

### 7.2 `structuredContent` 覆盖范围

建议：

- 不一次性覆盖全部 26 个工具
- 先覆盖高价值、天然结构化的结果

### 7.3 取消能力边界

`notifications/cancelled` 本期目标应是：

- 能取消 transport 内的 in-flight request
- 能把 cancellation 传播到当前请求 context

不强求本期把所有业务 handler 都改造成完全可抢占中止，但至少长请求应能感知 context cancellation。

## 8. 交付顺序

1. 统一 request / notification 模型
2. 补 `notifications/initialized`、`notifications/cancelled`、`ping`
3. 升级 `initialize` version
4. 引入 `Streamable HTTP`
5. 补 `MCP-Protocol-Version` header 校验
6. 升级 tool metadata
7. 增量补 `structuredContent`
