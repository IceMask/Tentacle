# MCP 2025-06-18 对齐清单（历史基线）

> 本清单记录已完成的 legacy alignment 批次。当前 `2026-07-28` 无状态协议支持请参阅 [MCP_2026_07_28_ALIGNMENT.md](./MCP_2026_07_28_ALIGNMENT.md)。

## 1. 范围结论

- 本期目标：
  - 对齐 `stdio` 握手、通知、保活与 tool metadata 的最新规范。
  - 将当前 HTTP `/jsonrpc` 兼容入口升级为标准 `Streamable HTTP` MCP transport。
  - 保持现有业务工具、进度通知、同步等待式 `executePlan` 行为不倒退。
- 本期不做：
  - OAuth
  - Elicitation
  - Async Tasks（实验性）
  - Tool icons
  - sampling 中携带 tools / toolChoice
- 本期按“支持但不强依赖”处理：
  - `structuredContent`
  - `outputSchema`

## 2. P0：必须先完成

### P0-1 握手与通知语义修复
- 静默接收 `notifications/initialized`。
- 静默接收 `notifications/cancelled`。
- transport 层禁止对 JSON-RPC notification 写回任何 response/error。
- `notifications/cancelled` 需要真正联动到 server 内部 cancel path，而不是仅忽略。

### P0-2 `ping` 支持
- 新增 `ping` request handler。
- 对 `ping` 返回空 result `{}`。
- `stdio` 与 HTTP transport 行为保持一致。

### P0-3 协议版本升级
- `initialize` 返回的 `protocolVersion` 升级为目标版本。
- 对外能力声明与实际实现保持一致，不能继续保留旧版本口径。

### P0-4 Streamable HTTP transport
- 提供标准 `Streamable HTTP` MCP transport。
- 区分 request 与 notification 的 HTTP 语义。
- 支持服务器通过同一 transport 正确发送 progress / event 类消息。
- 保留现有 `/jsonrpc` 兼容入口时，必须在文档中显式标注为 non-MCP compatibility endpoint。

### P0-5 `MCP-Protocol-Version` HTTP header 校验
- 对 MCP HTTP 请求校验 `MCP-Protocol-Version`。
- 对缺失/不兼容 header 返回明确错误。
- 在 response 中返回 negotiated/accepted protocol version。

## 3. P1：应该尽快完成

### P1-1 Tool metadata 升级
- 为 `Tool` 增加 `title`。
- 为 `Tool` 增加 `annotations`。
- 补齐工具 JSON 定义文件中的 metadata。
- 为高风险工具标注 `destructiveHint`，为只读工具标注 `readOnlyHint`。

### P1-2 Tool result 升级
- 在保持现有 `content` 不变的前提下，为适合结构化输出的工具补 `structuredContent`。
- 为后续 `outputSchema` 引入预留设计，但本期不要求所有工具都实现。

### P1-3 HTTP/stdio 通用 notification model
- 统一 request / notification 的解析与分发模型。
- 为后续 server-originated notifications 留出复用通道。

## 4. P2：可第二批完成

### P2-1 兼容入口治理
- 评估是否保留现有 `/jsonrpc` compatibility endpoint。
- 如果保留，增加清晰文档和测试，避免被误解为标准 MCP transport。

### P2-2 `structuredContent` 覆盖面扩展
- 从高价值工具开始逐步扩展，而不是一次性覆盖全部 26 个工具。

### P2-3 `outputSchema` 与 richer tool contracts
- 在后续版本中为稳定工具补充 `outputSchema`。

## 5. 验收标准

- `stdio`：
  - `initialize -> notifications/initialized -> tools/list -> ping` 全链路通过。
  - notification 不产生 response。
  - `notifications/cancelled` 能中止长请求或至少触发明确取消路径。
- HTTP：
  - 支持标准 `Streamable HTTP`。
  - `MCP-Protocol-Version` 缺失或不匹配时返回可诊断错误。
  - `ping` 与 `tools/call` 都能通过标准 transport 测试。
- Tool metadata：
  - `tools/list` 返回 `title` 与 `annotations`。
- Tool result：
  - 至少一组结构化工具结果带 `structuredContent`。

## 6. 实施顺序建议

1. 先改 transport 栈和 notification 语义。
2. 再补 `ping` 与协议版本升级。
3. 再引入 `Streamable HTTP` 与 version header 校验。
4. 最后补 `title`、`annotations`、`structuredContent`。
