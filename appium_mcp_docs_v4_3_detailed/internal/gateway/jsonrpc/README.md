# internal/gateway/jsonrpc

## handler.go

### NewHandler/ServeHTTP
**Purpose**：JSON-RPC 入口，路由到具体 method。

**Logic — External Calls**
    - schema.Validate(req)
    - 调用 orchestrator 对应方法

**Logic — Local Implementation**
    - 解析 body -> decodeAndValidate -> 调用 -> writeResult/ writeError。
    - mapErrorToJSONRPC 做协议层错误映射。

**Errors & Edge Cases**
    - 未知 method -> -32601；无效参数 -> -32602；内部错误 -> -32603。

**Acceptance**
    - 错误码与 HTTP/gRPC 映射一致；schema additionalProperties:false。



### decodeAndValidate / mapErrorToJSONRPC / writeResult / writeError
**Purpose**：辅助处理函数。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 统一 JSON 解码、ID 提取、错误映射与响应序列化。

**Errors & Edge Cases**
    - ID 缺失 -> 400；invalid json -> -32700。

**Acceptance**
    - 模糊测试（fuzz）不过崩；对齐 JSON-RPC 2.0 规范。


## methods.go

### startSession / endSession
**Purpose**：启动与结束会话。

**Logic — External Calls**
    - orchestrator.StartSession()
    - orchestrator.EndSession()

**Logic — Local Implementation**
    - `startSession` 校验 `project/actor/labels/w3cCapsJson`；强制 `Idempotency-Key`，回传 `sessionId` 与能力声明。
    - `endSession` 幂等处理，不存在的会话映射到 `E.SESSION.NOT_FOUND`。

**Errors & Edge Cases**
    - Schema 校验失败 -> -32602。
    - 会话重复关闭 -> 返回 `ok=true`。

**Acceptance**
    - 幂等键命中返回首个响应；审计日志记录主体与 session。


### executePlan / getSemanticSnapshot / takeScreenshot / getArtifacts
**Purpose**：计划执行、语义树、截图与工件查询。

**Logic — External Calls**
    - orchestrator.ExecutePlan()
    - orchestrator.GetSemanticSnapshot()
    - orchestrator.TakeScreenshot()
    - orchestrator.GetArtifacts()

**Logic — Local Implementation**
    - `executePlan` 校验 plan schema，生成或复用 `traceId`。
    - `getSemanticSnapshot` 返回 `{changed, rev, snapshotRef, tree?}`；`sinceRev` 命中缓存时直接返回 `changed=false` 并复用 `rev/snapshotRef`。
    - `takeScreenshot` 允许 `includeThumb`，截屏前遮蔽 `secure` 区域，并生成最长边 640px 的缩略图。
    - `getArtifacts` 支持 `types` 过滤与游标分页。

**Errors & Edge Cases**
    - 并发配额不足 -> E.RATE.LIMITED。
    - trace/session 失效 -> E.SESSION.DEAD / E.TRACE.NOT_FOUND。

**Acceptance**
    - JSON schema 校验覆盖所有字段；返回值与 `schemas/jsonrpc` 保持一致。


### replay
**Purpose**：回放历史 trace。

**Logic — External Calls**
    - orchestrator.ReplayPlan()

**Logic — Local Implementation**
    - 校验 `target`（auto/local/adf）；允许复用 traceId 需显式参数。

**Errors & Edge Cases**
    - 源 trace 不存在 -> E.TRACE.NOT_FOUND。

**Acceptance**
    - 回放事件标记 `phase=replay`；输出中包含新 traceId。


### subscribe / unsubscribe
**Purpose**：管理 WebSocket 渠道。

**Logic — External Calls**
    - websocket.Hub.Subscribe()/Unsubscribe()

**Logic — Local Implementation**
    - `subscribe` 校验 trace 存在并返回 `channelId`；`sinceEventId` 仅在 ring buffer 仍保留该事件时生效，超出缓存需调用 A2A `GetEvents` 补齐。
    - `unsubscribe` 幂等释放，无论 channel 是否存在。

**Errors & Edge Cases**
    - 超出并发订阅限制 -> E.WS.TOO_MANY_SUBS。

**Acceptance**
    - channel 回收后队列归零；Hub 指标同步更新。


### describeCapabilities
**Purpose**：输出节点能力（apiVersion、planOps、features、extensions、deprecated.sunsetAt）。

**Logic — External Calls**
    - capabilities.Service.List()

**Logic — Local Implementation**
    - 按配置拼装 JSON；include JSON-RPC/A2A/StreamLogs 开关。

**Errors & Edge Cases**
    - 配置缺失 -> E.CONFIG.MISSING。

**Acceptance**
    - 输出通过 JSON schema 与示例对比；字段完整。


### healthCheck
**Purpose**：返回 Gateway→Orchestrator→Worker 的健康状态。

**Logic — External Calls**
    - orchestrator.HealthCheck()

**Logic — Local Implementation**
    - 允许可选参数（如 `traceId`）用于诊断；默认执行轻量探活（数据库读、Redis ping、最新 worker 心跳延迟）。
    - 将内部状态映射为 `healthy|degraded|unavailable` 并包含 `issues[]` 列表。

**Errors & Edge Cases**
    - Orchestrator 返回错误 -> 映射为 JSON-RPC 错误 `E.HEALTH.DOWN`。

**Acceptance**
    - 与 REST `/healthz`（若存在）返回一致；调用时可见 Prometheus/OTEL 指标同步记录。


## schema.go

### NewValidator/Validate/loadSchemas
**Purpose**：加载与校验 JSON 模式。

**Logic — External Calls**
    - embed.FS 读取内置 schema

**Logic — Local Implementation**
    - 构建引用关系图，检测循环与冲突；缓存校验器。

**Errors & Edge Cases**
    - 循环引用/重复定义 -> E.SCHEMA.CONFLICT

**Acceptance**
    - 所有 schema 通过 lint；`sinceRev`/差量字段一致。
