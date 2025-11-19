# Module I v4.2 — 详细需求（逐文件实现细则 & 补充修订）

> 本文件为“整合版”，包含方法级需求、实现要点与验收标准，并合并了《功能覆盖差异检查》里指出的漏项补充。

## 目录
- cmd/gateway
- cmd/orchestrator
- cmd/worker
- internal/config
- internal/auth
- internal/gateway (jsonrpc/rest/websocket/middleware/capabilities)
- internal/orchestrator
- internal/worker (+ appium)
- internal/storage (redis/postgres/s3)
- telemetry / errors / util

---

# 逐模块要点（简述）
- 网关：配置加载、遥测初始化、传输依赖、自定义路由、中间件顺序（Auth→RateLimit→Logging→Handler）、优雅关停（≤30s）。
- Orchestrator：DAO/缓存/S3 初始化、WorkerRegistry/Dispatcher/EventsPublisher、gRPC 拦截器、Monitor 误判 <0.1%。
- Worker：并发估算、依赖初始化（Appium/并发控制/快照缓存/Artifact 上传）、注册+心跳、优雅关停。
- 详细方法、外部调用、本地实现与验收条目见对应子目录 README（或参考下方补充修订清单）。

---

# 补充修订：v4.2 功能覆盖完善
> 以下为根据 v2/v3/v4.1.2 设计与需求对 v4.2 进行的补充（重点新增/明确的需求与验收）。

## A2A 接口补充
- 列出 REST 端点：`/api/v1/sessions`, `/api/v1/plans:execute`, `/api/v1/plans/{id}:cancel`, `/api/v1/traces/{id}`, `/api/v1/traces/{id}/events`, `/api/v1/artifacts`。
- gRPC 方法：`StartSession`、`ExecutePlan`（server-streaming）、`CancelPlan`、`GetTrace/Events/Artifacts`、`StartArtifactUpload/CompleteArtifactUpload`。
- `Idempotency-Key` 必填，TTL=24h；HMAC 校验：`method|path|ts|bodyHash|nonce`，窗口 ±5m。

## Artifact 多分片直传与工件通路
- Orchestrator 暴露 `StartArtifactUpload/CompleteArtifactUpload`；Worker ≥5MB 走分片直传（默认 4 并发），阈值建议 256KB。
- 校验 ETag/sha256；操作幂等（重试不重复完成）。

## JSON-RPC 与 Schema 严格校验
- 启用 schema，`additionalProperties:false`；补充 `subscribe`/`unsubscribe`、`describeCapabilities` 等方法说明。
- 补齐九个方法（`startSession`/`endSession`/`executePlan`/`getSemanticSnapshot`/`takeScreenshot`/`getArtifacts`/`replay`/`subscribe`/`unsubscribe`）的逻辑、错误码与验收；`describeCapabilities` 输出 `apiVersion`/`planOps`/`features`/`extensions`/`deprecated.sunsetAt` 并与 REST 返回一致。
- JSON-RPC 新增 `healthCheck`：返回 Gateway→Orchestrator→Worker 状态，映射 `healthy|degraded|unavailable`，错误码 `E.HEALTH.*`。
- 统一 JSON-RPC 错误结构与内部错误码映射。

## WebSocket 背压与事件补偿
- Hub 队列长度 256，策略 drop-oldest；指标：`websocket_drops_total`, `ws_queue_len_histogram`。
- 客户端断线后可携带 `sinceEventId` 读取仍在 ring buffer 中的事件（best-effort）；若事件已被淘汰，必须改用 A2A `GetEvents(sinceEventId)` 补齐，以维持 at-most-once + 事后追补。

## PlanEvent 扩展字段
- `phase`: precheck|exec|fallback|teardown；`snapshotRef`（语义树版本）；`candidates`（定位候选）；`deviceStats`（可选设备指标）。
- 事件体积上限 64KB，超限改存 Artifact，仅在事件里留 `artifactRef`。

## 错误码与跨协议映射
- 新增并明确：`E.RATE.LIMITED→429`、`E.WS.BACKPRESSURE_DROP→503` 等；统一 internal→gRPC→HTTP→JSON-RPC 映射表。

## Selector 策略矩阵
- 无命中：回退 and/near；再无 → `E.SELECTOR.NOT_FOUND`。
- 多命中：按可见性/距离排序；仍多 → `E.SELECTOR.AMBIGUOUS`。
- DOM 波动：自动等待 ≤5s；仍失败 → `E.TIMEOUT.STEP`。
- 兜底：视觉定位（优先本地 OpenCV，远程失败自动降级本地）。

## 并发配额与排队机制
- Redis Key：`concurrency:{tenant}:{project}`；默认配额 user=5/project=20；无名额进入等待队列，超时 → `E.RATE.LIMITED`。
- 指标：`concurrency_in_use`、`queue_wait_seconds`。

## Snapshot 差量机制与缓存
- `snapshot_cache` TTL ≈ 1.5s；结构变化立即失效。
- `GetSemanticSnapshot(sinceRev)`：统一返回 `{changed, rev, snapshotRef, tree?}`；无变化返回 `changed:false` 并复用 `rev/snapshotRef`，有变化返回新 `rev` 与差量/全量。

## Appium 客户端容错与熔断
- 仅对 `ENETRESET/5xx` 重试 ≤2 次（带抖动）；连接池 keep-alive；为各请求设置 ctx 超时；可选熔断。

## 取消与幂等性保障
- `CancelPlan` 幂等：终态重复取消直接 200/OK；取消成功后“最后事件即 PlanCanceled”，后续不得再发事件。
- `StreamLogs` gRPC 输出 trace 级日志流：背压指标 `stream_logs_backpressure_total`；不可用时返回 `E.LOGS.UNAVAILABLE`。
- `UploadArtifacts` 维持客户端流式通道：按 `traceId+key` 聚合分片、校验 sha256/ETag、重复上传返回首个结果；与分片直传策略并存。
- `takeScreenshot`：允许 `includeThumb`，返回 ArtifactRef；截屏前遮蔽 `secure` 区域，并生成原图 + 最长边 640px 缩略图。
- `HealthCheck` gRPC：Worker 需反馈 Appium/依赖状态；Orchestrator 聚合后提供给 Gateway JSON-RPC/REST；故障映射 `E.HEALTH.DEGRADED/E.HEALTH.DOWN`。
- 幂等缓存 TTL=24h；命中直接返回首个响应。

## 崩溃恢复与 SLO
- 崩溃恢复成功率 ≥90%，≤10s；失败 ≤2s 返回 `E.SESSION.BROKEN`。
- Appium RTT p95 < 350ms；多分片直传使截图获取 p95 延迟下降 ≥30%。

## OTEL 全链路追踪
- 在 span attributes 注入 `traceId/sessionId/stepIndex` 串联 gateway/orchestrator/worker；任一 `traceId` 可还原完整链路。

## 验收标准补充
- WS 丢弃有指标记录，断线可 A2A 补齐；PlanEvent 扩展字段按条件出现；取消幂等用例通过。
- 三通道（A2A/JSON-RPC/gRPC）错误码一致映射；OTEL/metrics/logs 可用于一次性复盘完整链路。
