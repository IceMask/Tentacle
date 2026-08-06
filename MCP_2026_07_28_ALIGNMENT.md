# MCP 2026-07-28 升级说明

本仓库已按官方 Current 版本 `2026-07-28` 完成核心 server/transport 对齐，并继续兼容 initialize-based legacy 客户端。官方参考：

- [MCP 2026-07-28 Specification](https://modelcontextprotocol.io/specification/2026-07-28)
- [2026-07-28 Changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog)
- [Versioning and Compatibility](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)
- [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- [Server Discovery](https://modelcontextprotocol.io/specification/2026-07-28/server/discover)
- [Base Protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic/index)
- [Resources](https://modelcontextprotocol.io/specification/2026-07-28/server/resources)
- [Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)

## 协议变化摘要

| 变化 | `2025-11-25` 及更早 | `2026-07-28` | 本仓库实现 |
| --- | --- | --- | --- |
| 生命周期 | `initialize` + `notifications/initialized` | 无握手、逐请求无状态 | 双时代兼容 |
| 版本/能力 | 初始化时协商 | 每个 request 的 `params._meta` | 强制验证两个必填 namespaced 字段 |
| 服务发现 | 依赖 initialize capabilities | `server/discover` 为服务端必选 | 已实现并返回全部支持版本 |
| 成功结果 | `resultType` 可缺失 | 每个结果必须声明 `resultType` | 现代结果统一补 `complete` |
| Request ID | 旧集成可能混淆 `null` 与 omitted | 仅 string/integer，且不得为 `null` | 三个 transport 统一保留 presence 并校验类型 |
| 服务身份 | initialize 的 `serverInfo` | 推荐每个 result 的 `_meta` | 现代结果统一补 namespaced `serverInfo` |
| HTTP | POST，可选 GET/SSE 和 session | 仅 POST，无 GET stream/session id | `/mcp` GET 返回 405，POST 无 session 状态 |
| HTTP 镜像头 | 仅协议版本 | 另需 `Mcp-Method`；指定方法需 `Mcp-Name` | 校验缺失、编码和 header/body 一致性 |
| 工具/资源缓存 | 无统一字段 | list/template-list/read 支持 `ttlMs`、`cacheScope` | 目录使用 public，授权相关 resource read 使用 private |
| 资源模板 | 可被旧集成混入 `resources/list` | 参数化 URI 由 `resources/templates/list` 返回 | Modern 分离 concrete resources 与 URI templates，legacy 保持原结果 |
| Tool schema | dialect 行为不统一 | 默认且至少支持 JSON Schema 2020-12 | 显式声明并使用 2020-12 校验器 |
| Tool result | structured object 限制较多 | `structuredContent` 可为任意 JSON | 同时返回 text 与原生 structured JSON |

## 当前支持矩阵

- Modern：`2026-07-28`，通过每个 request 的 `_meta` 选择，无 initialize。
- Legacy：`2025-11-25` 和 `2025-06-18`，继续通过 initialize 选择。
- `server/discover` 返回上述三个版本，顺序为 modern 优先。
- Modern 方法：`server/discover`、`tools/list`、`tools/call`、`resources/list`、`resources/templates/list`、`resources/read`，以及 stdio cancellation 处理。
- Modern `resources/list` 当前返回空 concrete resource 集合；三个参数化 Appium URI 由 `resources/templates/list` 返回。
- Legacy 的 `ping`、`notifications/initialized` 与现有兼容方法保持可用，但在 modern `_meta` 下返回 method not found。
- Modern Streamable HTTP 不接收 client notification；取消正在执行的 HTTP 请求应关闭该请求的响应流，`notifications/cancelled` 仅保留给 stdio。
- `/jsonrpc` 继续作为非标准 compatibility endpoint；标准 MCP HTTP endpoint 是 `/mcp`。

## Modern Request 要求

每个 JSON-RPC request 的 `params._meta` 必须包含：

```json
{
  "io.modelcontextprotocol/protocolVersion": "2026-07-28",
  "io.modelcontextprotocol/clientCapabilities": {}
}
```

`io.modelcontextprotocol/clientInfo` 可选，但建议每次提供。HTTP 同时要求：

- `Content-Type: application/json`
- `Accept: application/json, text/event-stream`
- `MCP-Protocol-Version` 与 body protocol version 一致
- `Mcp-Method` 与 JSON-RPC `method` 一致
- `tools/call`、`resources/read`、`prompts/get` 的 `Mcp-Name` 与 `params.name` / `params.uri` 一致
- 不安全 ASCII、非 ASCII 或首尾空白的 `Mcp-Name` 使用 `=?base64?{Base64}?=` sentinel

关键错误映射：

- Header 缺失、格式错误或与 body 不一致：HTTP 400 / JSON-RPC `-32020`
- Modern 必填 `_meta` 字段缺失：HTTP 400 / JSON-RPC `-32602`
- 请求版本不支持：HTTP 400 / JSON-RPC `-32022`，data 包含 `supported` 和 `requested`
- Modern RPC 未实现：HTTP 404 / JSON-RPC `-32601`
- Resource 不存在或对调用方不可见：JSON-RPC `-32602`；后端读取失败：JSON-RPC `-32603`

## 实现位置

- `internal/gateway/mcp/protocol.go`：版本、逐请求 metadata、discover、现代 result/cache decoration 与标准错误。
- `internal/gateway/jsonrpc/modern.go`：modern/legacy 分类与 modern 方法边界。
- `internal/gateway/mcphttp/handler.go`：双时代 Streamable HTTP、标准头和状态码。
- `internal/gateway/mcp/tools.go`：确定性排序和 JSON Schema 2020-12 预编译校验。
- `internal/gateway/mcp/handler.go`：legacy-compatible 基础结果与 `structuredContent`。

## 验证

```bash
go test ./internal/gateway/mcp ./internal/gateway/jsonrpc ./internal/gateway/mcphttp -count=1
go test ./... -count=1
go vet ./...
```

测试覆盖 modern discovery、逐请求 metadata、unsupported-version retry data、legacy 回归、request ID 校验、HTTP header mismatch、Base64 sentinel、modern 404、HTTP notification rejection、resource/template 分离、resource error codes、结果装饰、确定性 tool ordering，以及 `unevaluatedProperties` 的 JSON Schema 2020-12 行为。

## 非阻塞扩展项

Prompts、subscriptions/listen、sampling、elicitation、MCP Apps、Tasks 等仍是未声明的可选 capability，不影响当前 tools/resources server 对 `2026-07-28` 核心协议的支持。后续增加任何 capability 时，应同时加入 discover capability 声明、逐请求 client capability 检查及对应 conformance tests。
