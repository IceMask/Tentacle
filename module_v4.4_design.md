# MCP Mobile Worker — 系统设计说明（v4.4 综合版）

> 本设计文档对齐《requirements.v4.4.md》，在 Module I 的实现蓝图基础上，补齐鉴权、execution lease、WebSocket 浏览器安全、TLS 分阶段边界与审计模型，并扩展 Module II 与 Module III 的组件职责、接口、数据流与部署建议，供架构评审与各子团队实施参考。

## 1. 架构全景

```
┌────────────────┐      ┌───────────────────┐      ┌─────────────────────┐
│ MCP/Agent 客户端 │─HTTP→│ Gateway (Module I) │─gRPC→│ Orchestrator (Module I) │
│ - JSON-RPC      │  WS  │ - JSON-RPC/A2A REST │     │ - 调度/并发配额           │
│ - A2A REST/gRPC │      │ - WebSocket Hub     │     │ - 事件落库/发布           │
└────────────────┘      │ - Capabilities/健康 │     │ - Worker Registry        │
                         └─────────┬─────────┘     └──────────┬────────────┘
                                   │ gRPC                               │ gRPC
                                   ▼                                   ▼
                          ┌────────────────┐                 ┌─────────────────┐
                          │ Worker 节点(MI) │                 │ Module II 服务群 │
                          │ - Plan Executor │                 │ - 订阅治理       │
                          │ - Appium Client │                 │ - 成本 & 合规     │
                          │ - Snapshot Cache│                 │ - 多租户调度      │
                          └──────┬──────────┘                 └───────┬─────────┘
                                 │ HTTP/WS                                   │ 事件/配置
                                 ▼                                           ▼
                          ┌──────────────┐                     ┌─────────────────┐
                          │ Appium/ADF   │                     │ Module III 引擎  │
                          │ 设备与云机池 │                     │ - Agent 工作流   │
                          └──────────────┘                     │ - 插件市场       │
                                                               └─────────────────┘

         ┌──────────────────────────┐      ┌────────────────────────────┐
         │ 数据与观测平面 (共享)     │      │ 存储/队列                   │
         │ - Prometheus/Grafana     │      │ - Postgres (会话/事件/审计) │
         │ - OTLP Collector         │      │ - Redis (Streams/配额/nonce)│
         │ - Loki/ELK (日志)        │      │ - S3/OSS (Artifact/备份)    │
         └──────────────────────────┘      └────────────────────────────┘
```

## 2. Module I 设计要点

### 2.1 Gateway
- **main.go**：配置加载 → Telemetry 初始化 → gRPC 客户端与缓存初始化 → 构建 JSON-RPC Handler (`internal/gateway/jsonrpc`), REST Router (`internal/gateway/rest`), WebSocket Hub (`internal/gateway/websocket`) → 注册中间件（Auth→RateLimit→Logging）→ 按环境策略执行 TLS/HTTP Serve。
- **JSON-RPC**：`methods.go` 按函数拆分逻辑；`healthCheck` 通过 orchestrator 聚合依赖状态；统一 `mapErrorToJSONRPC` 映射内部错误码。
- **REST**：
  - `POST /sessions` 等 handler 直接调用 orchestrator gRPC；`/capabilities` 复用 `capabilities.Service`。
  - `Idempotency-Key` 通过 `util/idempotency.go` 写入 Redis（TTL 24h）。
  - 浏览器订阅前通过 `POST /api/v1/traces/{id}:subscribe` 申请短时 subscription token。
- **Auth Middleware**：
  - 按凭证类型分流：完整 HMAC 头 → HMAC；Bearer JWT → OIDC；`Bearer mcp_v1_...` → PAT。
  - 多套完整凭证并存时返回鉴权冲突，不做 OIDC 与 PAT 的盲目回退。
  - PAT 正式模式读取 Postgres 权威源，Redis 仅做缓存；HMAC 读取 key registry；OIDC 使用 `issuer_url + audience` 初始化 verifier。
- **WebSocket Hub**：
  - `publishCh` 异步广播，队列长度 256 drop-oldest；慢连接检测+断开；指标 `websocket_drops_total`、`ws_queue_len_histogram`。
  - 浏览器握手默认同源校验；配置 allowlist 时仅允许 allowlist；无 `Origin` 的非浏览器客户端可放行。
  - 浏览器 MVP 阶段统一使用 query parameter 传递 subscription token；服务端日志不得记录完整 token 明文。
  - 语义保持 at-most-once，消费者需结合 A2A 事件拉取补齐。

### 2.2 Orchestrator
- **service.go**：
  - `StartSession/ExecutePlan/CancelPlan/HealthCheck/...` 将 DAO、WorkerRegistry、EventsPublisher、S3 Presign 组合。
  - `HealthCheck` 读取 Postgres、Redis ping、Worker 状态摘要、TLS / RPC 安全状态并生成 `issues[]`（类型/严重度/建议）。
- **dispatcher.go**：
  - 基于 Redis Streams，按 `tenant/project` 分片；`XAUTOCLAIM` 做故障接管；支持优先级权重。
  - distributed trace 在 worker `accepted` 后创建 execution lease，并在 lease 过期时触发 orphan 判定。
- **worker_registry.go**：
  - 维护 worker 状态机：`healthy`、`degraded`、`offline`、`draining`。
  - `Assign` 考虑标签、负载与 worker 状态；`degraded` 降权，`offline` / `draining` 不接新任务。
  - `StartMonitor` 依据 miss 次数与恢复稳定窗口执行状态迁移。
- **execution_lease_store.go**：
  - 保存 `trace_id`、`worker_id`、`attempt`、`lease_expires_at`、`last_renewed_at`、`status`。
  - lease TTL 由 `heartbeat_interval` 与 `grace_period` 推导，不单独暴露独立 TTL 开关。
  - 迟到结果必须校验 attempt，仅当前 active attempt 可更新 trace 状态。
- **events_publisher.go**：先落库再发布，WebSocket 仅保证 at-most-once；背压或慢连接时允许丢弃并计数，客户端需通过 A2A `GetEvents` 补齐。
- **grpc_server.go**：注册 Unary/Stream 拦截器（Auth、Metrics、Tracing、RateLimit）；注册健康检查服务供 Ops 探活。

### 2.3 Worker
- **grpc_server.go**：实现 `StartSession/ExecutePlan/GetSemanticSnapshot/TakeScreenshot/StreamLogs/UploadArtifacts/HealthCheck/CancelPlan`；使用 `concurrency.Controller` 获取执行许可。
- **executor.go**：Plan 步骤状态机；`emit` 非阻塞并有背压统计；失败统一截图/Artifact。
- **appium/client.go**：HTTP 池化、带 context 请求、有限重试、断路器；错误映射策略。
- **snapshot_cache.go**：本地缓存 + 可选 Redis；`Get` 返回 `{changed, rev, snapshotRef, tree?}`，命中缓存时重用 `rev/snapshotRef`，结构变更即递增 `rev` 并刷新引用。
- **artifacts.go**：小对象走 gRPC `UploadArtifacts` 聚合；大对象通过 orchestrator `Start/CompleteMultipart` 获取 presign 并走 S3 直传。
- **screenshots.go**：截屏前遮蔽 `secure` 区域；生成原图与最长边 640px 的缩略图；均按 ArtifactRef 存储/索引。
- **heartbeat.go**：
  - 默认 10s 周期上报 liveness，不直接代表 trace ownership。
  - distributed 模式下，worker 在持有 execution lease 时同时负责 lease 续约。
  - worker 从 `offline` 恢复后先回 `degraded`，连续稳定窗口后再回 `healthy`。
- **result reporter**：
  - 结果上报必须携带 `attempt`。
  - 如果 orchestrator 判定该结果属于过期 attempt，则只记录审计/日志，不得覆盖当前 trace 终态。

### 2.4 数据层
- **Postgres**：
  - 表 `sessions`, `traces`, `plan_events`, `artifacts`, `audit_logs`, `pat_tokens`, `hmac_keys`。
  - `pat_tokens` 使用 `token_id + secret_hash` 模型，`status` 仅表达运营状态，过期由 `expires_at` 推导。
  - `hmac_keys` 使用 `status + not_before/not_after` 联合计算运行时可验签状态。
  - 审计字段至少包含 actor、credential、tenant、resource、action、result、source_ip、request_id。
  - 使用行级锁/乐观锁，所有查询命中索引；迁移脚本放 `internal/storage/postgres/migrations`。
- **Redis**：
  - `Streams` 用于排队；
  - `Hash` 保存 Worker 状态与心跳；
  - `SETNX + EX` 实现 nonce、幂等键、配额计数；
  - execution lease 可使用 Redis TTL 存储运行时过期控制，但 trace/attempt 的最终状态仍需回写 Postgres。
- **S3**：默认 SSE、最小权限；Multipart 上传校验 ETag 列表。

### 2.5 观测与安全实现
- Prometheus 注册在 `telemetry/metrics.go`；指标标签 `tenant/project/traceId` 控制基数。
- OTEL `InitTracer` 注入 `traceId/sessionId/stepIndex` 属性；采样策略可配置（默认 25%）。
- 日志由 `telemetry/logging.go` 输出 JSON，调用 `util/mask.go` 脱敏。
- Auth 中间件：按凭证类型路由 HMAC / OIDC / PAT；失败 401/403；鉴权成功与失败都写审计。
- TLS / RPC 安全：
  - 外部入口在生产环境强制 TLS1.2+。
  - 内部 RPC 使用 `rpc.security.mode` 描述 `insecure` / `tls` / `mtls`，默认目标态为 `tls`，后续演进到 `mtls`。
- 健康检查至少暴露：`external_tls_enabled`、`rpc_security_mode`、`certificate_days_remaining`、`auth_failure_rate`、`websocket_origin_policy_mode`。

## 3. Module II 设计蓝图

### 3.1 组件拓扑
- **Subscription Service**：REST/gRPC 接口管理 Webhook；持久化 Postgres（表 `subscriptions`, `webhook_endpoints`, `delivery_attempts`）。
- **Delivery Orchestrator**：消费来自 Module I 的事件流（Kafka/NATS/Redis Stream mirror），按订阅过滤、批量/单条推送。
- **Retry Scheduler**：基于 Redis ZSet 或 Kafka 延迟队列，实现指数退避；失败达到阈值 → 通知 & 自动暂停订阅。
- **Policy Distributor**：将租户策略（keep-alive、取消超时、配额）下发到 Module I（Config API 或 Redis Key）。
- **Analytics & Cost Service**：
  - ETL 作业汇聚执行日志、ADF 使用、存储账单；
  - 生成成本报表（S3 + dashboard）；
  - 预算告警（Prometheus Alert + 通知渠道）。
- **Data Governance Service**：处理数据地域、保留、导出/删除请求；与 S3 Lifecycle/Glue/EMR 等联动。

### 3.2 数据流
1. Module I 在事件落库后通过消息总线广播（含 tenant/project/traceId）。
2. Subscription Service 读取事件，应用租户策略（过滤/批量/格式转换），写入 Delivery Orchestrator。
3. Delivery Orchestrator 按订阅调用 Webhook（带 HMAC），写入 `delivery_attempts`。
4. Retry Scheduler 根据结果决定重试/暂停；模块向 Observability 平台写入成功率指标。
5. Analytics Service 周期性扫描执行记录、设备使用、artifact 尺寸，生成成本报表并推送租户。
6. Data Governance Service 响应租户数据导出/删除请求，操作完成后写入审计与报告。

### 3.3 外部接口（示例）
- `POST /governance/v1/subscriptions`：创建订阅（需要租户认证）。
- `PATCH /governance/v1/subscriptions/{id}`：更新策略（重试次数、退避、格式）。
- `POST /governance/v1/subscriptions/{id}:pause|resume`。
- `GET /governance/v1/reports/cost?from=&to=`：下载成本报表。
- `POST /governance/v1/data/export|delete`：GDPR/CCPA 请求。
- `GET /governance/v1/policies`：调度与 keep-alive 策略（Module I 轮询或订阅）。

### 3.4 部署建议
- Module II 服务建议与 Module I 解耦，支持独立扩缩；
- 依赖 Kafka/NATS (事件传播)、Postgres（事务）、Redis（速率控制）、对象存储（报表）。
- 需提供多租户隔离（命名空间/数据库 schema），支持跨区域部署与灾备。

## 4. Module III 设计蓝图

### 4.1 Agent 工作流引擎
- **Plan Builder**：根据历史 Trace/视觉候选生成稳定 Plan；可调用 LLM/规则引擎；存储在 `plans_repository`。
- **Execution Verifier**：批量回放生成的 Plan，采集稳定性评分、差异报告。
- **Version Manager**：记录 Plan 版本、A/B 验证结果；暴露 API 给 IDE/CLI。

### 4.2 插件与生态
- **Plugin Runtime**：定义 `CapabilityManifest`（名称、类型、依赖、授权）；插件以 OCI 镜像/包形式部署。
- **Marketplace API**：租户可启/停插件；治理模块负责准入审核、计费。
- 插件类别：视觉断言、意图识别、智能探索、分析报表。

### 4.3 计费与 SaaS 运维
- **Billing Collector**：汇聚执行/资源使用，按费率表计价；输出发票 API。
- **Tenant Isolation**：支持 dedicated/pooled 集群；路由层根据租户策略选择执行平面。
- **Compliance Pipeline**：自动化 SOC2/ISO 证据收集，集成 SIEM。

### 4.4 开发者工具链
- **IDE Extension**：调用 Module I/II API 获取 capabilities、回放，直接生成脚本。
- **CLI/SDK**：封装常见操作（提交计划、监控事件、下载工件）。
- **Trace Portal**：使用 Module I/II 数据提供可视化 replay、指标联动、故障定位。

## 5. 部署 & 运维
- 推荐 Kubernetes 部署：
  - `gateway` Deployment（水平扩展，HPA 基于 QPS/CPU）。
  - `orchestrator` StatefulSet（主备或 raft 选主）。
  - `worker` DaemonSet/Deployment（按设备池分组）。
  - Module II/III 服务根据功能拆分 Deployment。
- 配置管理：使用 `deploy/server.yaml` + 环境变量覆盖；Module II/III 可使用集中配置中心（Consul/ZooKeeper/ConfigMap）。
- 监控与告警：Prometheus + Grafana；Alertmanager 根据错误码/延迟/心跳制定告警；日志集中至 Loki/ELK。
- 灾备：Postgres 主从、Redis 哨兵、S3 版本/跨区域复制；定期演练（失去 Worker 节点、Redis 故障、Appium host 下线、ADF 超时）。

## 6. 风险与后续工作
- 明确 JSON-RPC/A2A 网关是否共进程，影响部署拓扑与安全策略。
- `StreamLogs` 与 WebSocket 如需至少一次语义，需设计 ACK/重放；当前默认 at-most-once。
- `UploadArtifacts` 大对象限制需与运维敲定，避免超大文件影响 Redis/S3。
- Module II 需落实密钥轮换、租户配额下发协议；Module III 插件安全沙箱与审核流程待定。
- 制定统一 SLA/SLO、Runbook 与演练计划，覆盖控制面不可用、计划执行率、工件上传成功率等指标。
