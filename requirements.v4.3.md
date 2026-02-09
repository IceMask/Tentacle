# requirements.v4.3.md — MCP Mobile Worker 需求说明（v4.3 版本）

> **版本变更说明（v4.3）**：在 v4.2 基础上，新增标准 MCP（Model Context Protocol）协议支持，提供 stdio 传输模式，使平台可直接被 Claude Desktop 等 AI 工具通过标准 MCP 协议发现和调用。核心业务方法（startSession, executePlan 等）作为 MCP Tools 暴露给 LLM，同时保持向后兼容。

> 本版在 v3 的 Module I 基线之上，吸收 v4.1.2 详细规范以及治理/增值规划，明确 Module I 必须交付的接口与验收标准，并补充 Module II（平台治理）与 Module III（增值扩展）的路线图与约束，供各语言实现与平台部署团队共同遵循。

## 1. 背景 & 目标
- 构建面向 AI Agent 的统一移动执行内核：默认正确（auto-wait）、可回放、稳态低时延、强可观测，支持多协议（JSON-RPC/A2A/WS/gRPC）与跨区域部署。
- 体系分三层：
  - **Module I 核心执行层（必须实现）**：计划执行、语义快照、事件流、工件、A2A、Appium/ADF 调度、观测、基础安全。
  - **Module II 平台级治理（可拆分部署）**：订阅/Webhook 管理、成本与配额治理、数据合规、多租户审计。
  - **Module III 增值扩展（路线图）**：探索/生成引擎、视觉与智能插件生态、计费与 SaaS 化运维。
- 发布阶段：
  - **MVP**：Module I v4.2 全量功能（JSON-RPC/A2A/WS、Plan 执行、工件直传、ADF、观测、安全基线、健康检查）。
  - **Beta**：Module II 的订阅治理、成本监控、租户审计、数据生命周期；Module III 选定试点能力（视觉插件/Agent 回放）。
  - **GA**：Module I 高可用与灾备、Module II/III 的多区域化、计费、SaaS 运维、合规认证。

## 2. Module I — 核心执行层（必须交付）

### 2.1 协议与兼容性

#### 2.1.1 标准 MCP 协议支持（v4.3 新增）
- **传输模式**：
  - **stdio 模式**（新增）：Gateway 支持 `--stdio` 参数启动，通过标准输入/输出进行 JSON-RPC 通信，符合 Anthropic MCP 标准（协议版本 2024-11-05）。
  - **HTTP 模式**（向后兼容）：继续支持 HTTP POST `/jsonrpc` 端点。

- **MCP 元协议方法**（stdio 和 HTTP 模式均支持）：
  - `initialize`：MCP 握手，返回服务器能力和版本信息
  - `tools/list`：列出所有可用的业务工具（startSession, executePlan 等）
  - `tools/call`：调用指定工具，格式：`{"name": "toolName", "arguments": {...}}`
  - `resources/list`：列出可访问资源（traces, artifacts, sessions）
  - `resources/read`：读取指定资源内容

- **MCP Tools 注册表**：
  - 核心业务方法作为 MCP Tools 暴露：`startSession`, `executePlan`, `endSession`, `getSemanticSnapshot`, `takeScreenshot`, `cancelPlan`, `getTrace`, `healthCheck`
  - 每个 Tool 包含完整的 JSON Schema 定义（inputSchema），支持参数验证
  - Tool 调用结果包装为 MCP 标准格式：`{"content": [{"type": "text", "text": "..."}], "isError": false}`

- **Claude Desktop 集成配置**：
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

#### 2.1.2 传统协议（向后兼容）
- 支持 MCP JSON-RPC（HTTP POST `/jsonrpc`）、WebSocket 事件流、A2A REST (`/api/v1/*`)、A2A gRPC (`mcp.mobile.v1.a2a`) 以及 Worker gRPC (`mobile_mcp.v1.worker`)。
- 所有接口需返回 `apiVersion` 或在 `describeCapabilities`/`GET /capabilities` 中声明版本、特性稳定级别与弃用计划（含 `sunsetAt`）。
- gRPC/JSON 结构仅允许向后兼容扩展；弃用项至少提前两个次要版本公告。
- JSON-RPC 请求/响应必须通过 `schemas/jsonrpc/*.json` 校验（`additionalProperties:false`），包含以下方法：
  `startSession`, `endSession`, `executePlan`, `getSemanticSnapshot`, `takeScreenshot`,
  `getArtifacts`, `replay`, `subscribe`, `unsubscribe`, `describeCapabilities`, `healthCheck`。
- A2A REST 端点：`POST /sessions`、`POST /plans:execute`、`POST /plans/{traceId}:cancel`、`GET /traces/{id}`、`GET /traces/{id}/events`、`GET /artifacts`、`GET /capabilities`（复用 JSON-RPC 输出）。
- WebSocket `GET /ws/plan-events?traceId=`：at-most-once 投递；断线通过 `GetEvents(sinceEventId)` 补齐。

### 2.2 核心功能
#### 会话与计划
- `StartSession` 接收 `project/actor/labels/w3cCapsJson`，返回 `sessionId`、协商后的能力；`EndSession` 幂等释放资源。
- `ExecutePlan`：
  - 请求包含 `sessionId`、`traceId`（可由服务端生成）、`Plan`（遵循 `plan.schema.json`）。
  - Server-streaming `PlanEvent`，字段包含 `seq`, `stepIndex`, `status (running|passed|failed|skipped)`, `message`, `metrics.attempt|wdCalls|elapsedMs`, `artifactRefs[]`，可选 `phase|snapshotRef|candidates|deviceStats`。
  - 默认超时：Step=30s、Plan=10m；Cancel 传播预算 1s；Appium 请求 8s；auto-wait 上限 5s。
- `GetSemanticSnapshot` 支持 `sinceRev` 差量，返回 `{changed,bool, rev,string, tree?, snapshotRef}`；命中缓存时 `changed=false`。
- `Replay` 允许指定历史 `traceId` 与目标 (`auto|local|adf`)，生成新的 trace（可选复用）；事件需标记 `phase=replay`。
- `StreamLogs`（可选）：按 `traceId` 推送 `LogEvent(level,msg,ts)`，需记录背压指标；不可用时返回 `E.LOGS.UNAVAILABLE`。
- `UploadArtifacts`（gRPC 客户端流）：按 `traceId+key` 聚合 `ArtifactChunk`，支持断点续传、分片幂等与 SHA256/ETag 校验；≥阈值可触发 S3 Multipart 直传。
- `TakeScreenshot`：提供全图与缩略图（默认长边 640px），对 `secure` 字段脱敏；返回 `ArtifactRef`。

#### ADF / Appium
- ADF 任务：资源选择、排队、预热、执行、失败分类与回收；`adf.queue.maxWait` 超时自动回退到本地（默认 `local`），事件标记 `phase=fallback`。
- Appium 客户端需支持 HTTP keep-alive、ENETRESET/5xx 重试（≤2 次，抖动 200–1200ms）、断路器与连接池参数可配置（默认 `maxIdleConns=200`）。
- 崩溃恢复：检测 Appium/WDA 断开、进程异常，最多一次自动重建会话；失败 2s 内返回 `E.SESSION.BROKEN`。
- Snapshot Cache：本地 + Redis 共享缓存，TTL 1.5s，结构变化立即失效。

#### 并发与调度
- 并发配额：租户/user/project 三层；默认 user=5/project=20，可通过配置与 API 调整；Redis key `concurrency:{tenant}:{project}`。
- 调度策略：Redis Streams 分区，公平调度 + 权重；记录排队时长与超时事件；支持优先级配置与保底资源。
- Worker 心跳：10s 周期，连续 3 次失败降权/摘除；恢复后 60s 内重新纳入；Gateway `healthCheck` 需聚合心跳延迟。

### 2.3 观测与健康
- Prometheus 指标：`execute_latency_seconds`, `plan_event_seq_gaps`, `concurrency_in_use`, `queue_wait_seconds`, `appium_rtt_ms`, `websocket_drops_total`, `ws_queue_len_histogram`, `stream_logs_backpressure_total`, `artifact_upload_latency`, `adf_lease_success_rate`, `error_code_total`, `health_status`。
- OTEL Tracing：Gateway→Orchestrator→Worker→Appium，全链路传播 `traceId/sessionId/stepIndex`。
- 日志：结构化 JSON，自动注入 `requestId/traceId/sessionId`；敏感字段遮蔽（`token|secret|password|pin`）。
- `healthCheck`：JSON-RPC 方法 & REST `/healthz`（选），返回 `status=healthy|degraded|unavailable` 与 `issues[]`；错误码 `E.HEALTH.DEGRADED/E.HEALTH.DOWN`。

### 2.4 安全
- 鉴权：PAT（哈希存储、轮换/吊销）、OIDC（JWKS 缓存、aud/iss 校验）、HMAC（method|path|bodyHash|ts|nonce，窗口 ±5m，nonce Redis 去重）。
- TLS1.2+（推荐 1.3），支持 mTLS；证书到期提前 30 天告警并支持自动轮换。
- 日志与事件需脱敏 `secure` 数据；Artifact 存储启用 SSE、版本/生命周期策略；提供租户级合规删除。
- Webhook 管理（基础版）：签名校验、来源 IP 白名单、重放保护（nonce）、失败重试 3 次，更多治理由 Module II 承担。

### 2.5 性能 & 默认值
- Appium RTT p95 < 350ms（引入 2% 抖动注入用于退化测试）。
- 计划执行：p95 < 3s 警戒线；排队时长、直传延迟需对比基准下降 ≥30%。
- 表格：

| 配置键 | 默认 | 说明 |
| --- | --- | --- |
| `timeouts.step` | 30s | 最小 1ms，上限可配置 |
| `timeouts.plan` | 10m | 超时返回 `E.TIMEOUT.PLAN` |
| `wait.auto.max` | 5s | auto-wait 上限 |
| `retry.transient.max` | 3 | 瞬时错误最大重试 |
| `retry.transient.jitter` | 200–1200ms | 抖动范围 |
| `snapshot.ttl` | 1.5s | 语义树缓存 |
| `artifact.chunk.maxBytes` | 5MB (待确认) | 客户端流分片上限 |
| `artifact.directUpload.threshold` | 256KB | 触发直传阈值 |
| `websocket.backpressure.alert` | 3 events | 连续丢弃阈值 |
| `idempotency.key.ttl` | 24h | REST 幂等键 |

### 2.6 验收与测试
- 必须提供：
  - JSON-RPC/A2A Schema & Proto lint；互操作测试涵盖成功/失败。
  - Pytest/Go test 覆盖 Plan 执行、selector 回退、artifact 上传、并发/配额、健康检查。
  - WebSocket 回放测试：模拟断线重连，验证 at-most-once 与补偿流程。
  - 直传性能基准：p95 截图发布延迟下降 ≥30%。
  - 崩溃恢复：100 次注入测试成功率 ≥90%，失败时 <=2s 返回 `E.SESSION.BROKEN`。
- 发布需附带：`pytest`/`go test` 输出、`ajv` 校验日志、Prometheus 截图、性能报表。

## 3. Module II — 平台级治理组件（可拆分部署）

### 3.1 订阅 & Webhook 管理
- 提供独立 API：注册/更新/暂停/删除订阅；审批流与租户配额；支持租户隔离。
- Webhook 重试策略可配置（最大次数、退避、超时）；连续失败触发自动暂停与通知。
- 提供订阅健康仪表（成功率、耗时、失败分类）与手动重播、批量补偿接口。
- 推送策略可按租户下发：长计划 keep-alive（默认 30s）、取消超时（默认 5m）；核心执行层需接受治理策略下发。
- 可选集中事件网关：支持批量转发、过滤、格式转换（JSON→租户自定义格式），并输出队列指标。

### 3.2 数据治理与合规
- 成本监控：跟踪云设备、ADF slot、Artifact 存储成本；输出租户月报、预算阈值告警。
- 数据地域化：租户可指定存储区域（如 `ap-northeast-1`、`us-west-2`），治理层负责策略分发与合规校验。
- 数据保留：日志 ≥30 天、审计 ≥90 天、artifact 默认 30 天（可配置）；到期需不可恢复删除并产生日志。
- 数据访问审计：下载、查看 artifact/log 都需写入审计；提供租户级查询导出。
- GDPR/CCPA 支持：租户数据导出与删除请求 API，7 天内生成合规报告。
- 敏感数据扫描：定期检测日志/artifact 中的 API Key、PII；发现异常自动告警并启动整改流程。

### 3.3 多租户资源治理
- 配额 & 限流：按租户/项目/用户设置并发、速率、排队长度，支持 API/控制台动态调整。
- 优先级调度：同租户内支持权重；跨租户设置保底资源与最大占比（默认 40%）。
- 资源耗尽处理：触发告警、返回 `E.RATE.LIMITED` 或 `E.TIMEOUT.PLAN`，提供排队 ETA 与降级建议（降低并行度、延后执行）。
- 排队超时：通知调用方，可选择取消/改用 ADF；记录审计，仪表盘展示排队分布。
- 成本审计导出：计划提交量、成功率、耗时、成本估算；支持预算阈值（80% 通知、100% 停止/审批）。

## 4. Module III — 增值扩展路线
- **Agent 工作流引擎**：Plan 录制/生成、稳定性评估、多语言导出、历史版本管理与回放。
- **视觉与智能插件生态**：视觉断言、意图识别、智能探索、模型推理服务的标准化接口与市场。
- **高级治理/计费**：跨租户计费、订阅管理、客户自助面板、计量/计费 API（对接财务系统）。
- **SaaS 运维**：多区域联邦部署、租户隔离集群、弹性扩缩容策略、深度合规（ISO/SOC2）、外部审计集成。
- **开发者工具链**：IDE 插件、CLI/SDK、低代码 Plan 编辑器、Trace 可视化门户。
- **安全增强**：HSM 集成、零信任设备接入、多因子审批工作流、机密计算。

## 5. 开放问题 & 后续决策
- JSON-RPC 与 A2A 是否共进程或经独立网关代理？需确定端口与部署策略。
- `describeCapabilities` 与 `/capabilities` JSON Schema 最终定稿与弃用通知流程待评审。
- `StreamLogs` / WebSocket 是否需要至少一次投递保证？若是需设计 ACK/缓冲/重放机制。
- `UploadArtifacts` 分片与总大小限制待运维确认，并同步落入配置校验。
- Webhook HMAC 密钥轮换与多租户密钥管理流程需 Module II 给出方案。
- SLA/SLO 目标、告警分级、应急响应 Runbook 需与运维团队确认（建议 Plan 成功率 ≥98%，控制面不可用 ≤60s）。

