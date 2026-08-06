# MCP 2025-06-18 对齐需求说明（历史基线）

> 本文档记录已完成的 `2025-06-18` 对齐批次。当前 `2026-07-28` 实现与兼容边界以 [MCP_2026_07_28_ALIGNMENT.md](./MCP_2026_07_28_ALIGNMENT.md) 为准。

## 1. 背景

当前项目已经具备 MCP `stdio`、HTTP JSON-RPC、tool registry、progress notification 等基础能力，但仍停留在旧版握手与传输模型上。对照最新 MCP 官方规范，存在以下主要缺口：

- 未正确处理 `notifications/initialized`
- 未支持 `ping`
- 未处理 `notifications/cancelled`
- HTTP 仍是自定义 request-response `/jsonrpc`，未升级到 `Streamable HTTP`
- 未校验 `MCP-Protocol-Version` HTTP header
- tool metadata 未包含 `title` 与 `annotations`
- tool result 未提供 `structuredContent`

这些问题会造成：

- 与最新 MCP 客户端互操作性下降
- Claude Desktop 等客户端可能因为握手/保活不完整而出现异常
- 服务端能力声明与真实实现不一致
- 对外 HTTP 入口被误解为标准 MCP transport

## 2. 目标

本期目标是实现“最新 MCP 核心互操作对齐”，具体包括：

1. `stdio` transport 满足最新握手、notification、ping 语义。
2. HTTP MCP 入口升级到标准 `Streamable HTTP` transport。
3. MCP HTTP 入口校验并协商 `MCP-Protocol-Version`。
4. `tools/list` 返回的 metadata 对齐最新 schema。
5. `tools/call` 支持基础 `structuredContent` 返回能力。

## 3. 非目标

本期明确不包含：

- OAuth
- Elicitation
- Async Tasks
- Tool icons
- sampling 中携带 tools / toolChoice
- 全量工具一次性补齐 `outputSchema`

这些能力可以在后续版本中单独规划。

## 4. 功能需求

### 4.1 Lifecycle / Handshake

- server 必须支持 `initialize`。
- client 在 `initialize` 后发送 `notifications/initialized` 时，server 必须静默接收，不返回错误。
- notification 不得触发 JSON-RPC response。
- server 返回的 `protocolVersion` 必须与本期目标版本一致，或按显式协商规则返回受支持版本。

### 4.2 Ping

- server 必须支持 `ping`。
- 对 `ping` 返回空 result `{}`。
- `stdio` 与 HTTP transport 上的 `ping` 行为必须一致。

### 4.3 Cancellation

- server 必须接收 `notifications/cancelled`。
- `notifications/cancelled` 中的 `requestId` 必须映射到当前 in-flight request。
- 对可取消的长请求，应尽快停止关联处理逻辑。
- 对已完成请求，允许忽略迟到的 cancellation notification，但不得报协议错误。

### 4.4 Streamable HTTP

- 必须提供标准 `Streamable HTTP` MCP transport。
- HTTP transport 必须支持 request、notification 与 server-to-client message 的规范化传输语义。
- 现有 `/jsonrpc` request-response 兼容入口如保留，必须在文档中明确标记为 compatibility API，而非标准 MCP transport。

### 4.5 Protocol Version Header

- MCP HTTP 请求必须校验 `MCP-Protocol-Version` header。
- 缺失、非法或不兼容时，必须返回明确错误。
- 成功响应应返回 negotiated/accepted protocol version。

### 4.6 Tool Metadata

- `tools/list` 返回的每个 tool 必须支持：
  - `name`
  - `title`
  - `description`
  - `inputSchema`
  - `annotations`
- `annotations` 至少支持：
  - `readOnlyHint`
  - `destructiveHint`
  - `openWorldHint`
- 对已有 26 个工具，需要补齐最小 metadata，不允许 registry 与 JSON 文件口径不一致。

### 4.7 Tool Result

- `tools/call` 必须继续支持现有 `content` 返回。
- 支持工具可额外返回 `structuredContent`。
- 若工具返回 `structuredContent`，其值必须可 JSON 序列化。
- 本期不要求所有工具一次性提供结构化结果，但至少需要对高价值工具建立标准返回样例。

## 5. 兼容性要求

- 保持现有 `stdio` 客户端与现有 JSON-RPC compatibility client 不被破坏。
- 现有 legacy direct methods 不在本次 MCP 对齐范围内移除，但不得影响 MCP 正常协商。
- progress notification 现有行为必须保持兼容。

## 6. 可观测性要求

- 所有被忽略/接收的 MCP notifications 需要有低噪音日志，而不是 error 级噪音日志。
- 对以下协议级行为记录结构化日志：
  - initialize
  - initialized
  - ping
  - cancelled
  - protocol version negotiation
  - Streamable HTTP session lifecycle

## 7. 测试与验收

### 7.1 单元测试

- `notifications/initialized` 被静默接收且不回包。
- `notifications/cancelled` 被静默接收并触发取消路径。
- `ping` 返回 `{}`。
- `tools/list` 含 `title` 与 `annotations`。
- `tools/call` 可返回 `structuredContent`。

### 7.2 集成测试

- `stdio` 握手链路：`initialize -> notifications/initialized -> ping -> tools/list`
- HTTP MCP 链路：标准 `Streamable HTTP` request/response + progress
- HTTP header 校验失败路径
- 长请求取消路径

### 7.3 文档验收

- README 中明确说明：
  - 标准 MCP transport
  - compatibility endpoint（如存在）
  - protocol version
  - 当前不支持的可选能力

## 8. 交付物

- MCP transport 代码变更
- tool schema / JSON metadata 变更
- 相关测试
- README/集成文档更新
- 版本升级说明
