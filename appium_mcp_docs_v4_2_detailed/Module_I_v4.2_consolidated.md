# Module I v4.2 — 详细需求（逐文件实现细则）

> 本整合文件自动汇总子目录 README，包含每个方法需要实现的逻辑、外部调用、内部实现与验收标准。



---

## cmd/gateway/README.md

# cmd/gateway

## main.go


### loadEnvAndConfig()
**Purpose**：装载与校验配置；为后续组件提供一致的 Config。

**Logic — External Calls**
    - config.Load()
    - config.MustLoad()

**Logic — Local Implementation**
    - 按优先级合并：默认值 < 文件 < ENV。
    - 对 duration、枚举、互斥项进行语义校验。
    - 将敏感字段（tokens/secrets）标记供日志脱敏使用。

**Errors & Edge Cases**
    - 缺少必填项 -> E.CONFIG.MISSING
    - 非法取值 -> E.CONFIG.INVALID
    - 互斥冲突 -> E.CONFIG.CONFLICT

**Acceptance**
    - 非法配置直接进程退出（退出码≠0）。
    - 表驱动单测覆盖所有边界（空、极大、极小、非法枚举）。



### initTelemetry()
**Purpose**：初始化指标、追踪、日志。

**Logic — External Calls**
    - telemetry.InitMetrics()
    - telemetry.InitTracer()
    - telemetry.NewLogger()

**Logic — Local Implementation**
    - 将 requestId/traceId 注入 Logger 的默认字段。
    - 为 /metrics 路径注册 handler（若由外层提供则跳过）。

**Errors & Edge Cases**
    - OTEL 端点不可达 -> 降级仅日志；不得阻断启动。

**Acceptance**
    - Prometheus 能抓到 metrics；OTLP 导出器连通；日志为结构化 JSON 并脱敏。



### initTransports()
**Purpose**：初始化面向 Orchestrator/Redis/S3 的客户端。

**Logic — External Calls**
    - 拨号 gRPC Orchestrator 客户端（带拦截器）。
    - 按需创建 Redis/S3 客户端并做健康检查。

**Logic — Local Implementation**
    - 连接池参数来自 Config；对超时/重试做统一封装。

**Errors & Edge Cases**
    - 依赖不可连 -> 记录 FATAL 并退出。

**Acceptance**
    - 所有依赖在 3 次指数退避重试内连通；否则退出。



### initRouters()
**Purpose**：装配 REST、JSON-RPC、WebSocket 的路由与中间件。

**Logic — External Calls**
    - jsonrpc.NewHandler()
    - rest.NewRouter()
    - websocket.NewHub()

**Logic — Local Implementation**
    - 中间件顺序：Auth → RateLimit → Logging → Handler。
    - 注册健康检查与版本信息端点。

**Errors & Edge Cases**
    - 路由冲突 -> 启动前检测并报错。

**Acceptance**
    - 启动后 curl 所有路由返回 2xx/4xx 预期码；无 5xx。



### serve()
**Purpose**：启动 HTTP/HTTPS 并实现优雅关闭。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 根据 Config 选择 HTTP 或 TLS。
    - 监听 SIGTERM/SIGINT，触发 server.Shutdown(ctx, ≤30s)。

**Errors & Edge Cases**
    - 端口被占用 -> E.NET.ADDRINUSE
    - 证书无效 -> E.TLS.INVALID

**Acceptance**
    - 发 SIGTERM 后 30s 内优雅退出；并确保 in-flight 请求完成或取消。


---

## cmd/orchestrator/README.md

# cmd/orchestrator

## main.go


### initStores()
**Purpose**：初始化持久层与缓存层。

**Logic — External Calls**
    - postgres.NewDAO()
    - redis.NewCache()
    - s3.NewClient()

**Logic — Local Implementation**
    - DAO 执行迁移/索引检查（只读场景跳过迁移）。
    - Redis 做 ping 与权限检查；S3 做 bucket/head 检查。

**Errors & Edge Cases**
    - 数据库不可达/凭证错误 -> E.STORE.CONN
    - 索引缺失 -> E.STORE.INDEX

**Acceptance**
    - 健康检查全绿；DAO 读写基准用例通过（创建 Trace/列出 Events）。



### initCore()
**Purpose**：构造调度域核心对象。

**Logic — External Calls**
    - NewWorkerRegistry()
    - NewDispatcher()
    - NewEventsPublisher()

**Logic — Local Implementation**
    - 将 DAO、Streams、Cache 注入，形成闭环。
    - 注册系统事件（worker 上线/下线、调度失败告警）。

**Errors & Edge Cases**
    - 循环依赖/空依赖 -> 启动即失败。

**Acceptance**
    - 组件互测：提交一个最小 plan，能完成排队→分派→事件落库→WS 发布。



### serveGRPC()
**Purpose**：启动 gRPC 服务并注册拦截器。

**Logic — External Calls**
    - NewGRPCServer(cfg, svc)

**Logic — Local Implementation**
    - 启用 unaryAuth / unaryMetrics / unaryTracing。
    - 限制最大消息大小与并发。

**Errors & Edge Cases**
    - 端口占用 -> E.NET.ADDRINUSE

**Acceptance**
    - grpcurl 健康调用返回 OK；拦截器暴露 metrics 与 trace。



### runMonitors()
**Purpose**：周期检查 worker 心跳与租约。

**Logic — External Calls**
    - worker_registry.StartMonitor()

**Logic — Local Implementation**
    - 采用 min-heap 或定时扫描回收超时租约。

**Errors & Edge Cases**
    - 误判离线 -> 降级仅减权，不立即剔除。

**Acceptance**
    - 误判率 < 0.1%，离线 2 * TTL 内被剔除。



### serveMetrics()/gracefulShutdown()
**Purpose**：指标服务与退出清理。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 在退出前 flush 事件队列、等待 in-flight 调度结束（≤30s）。

**Errors & Edge Cases**
    - 长时间阻塞 -> 超时强制退出并记录未完成数。

**Acceptance**
    - 退出时持久层无未提交事务；事件队列空或已记录处置状态。


---

## cmd/worker/README.md

# cmd/worker

## main.go


### computeConcurrency()
**Purpose**：根据设备数与 CPU 核数估算并发。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - limit = min(int(devices*1.5), 2*NumCPU)。
    - 支持环境变量/配置强制覆盖。

**Errors & Edge Cases**
    - devices 未配置 -> 回退到 NumCPU。

**Acceptance**
    - 覆盖后可热更新或重启生效；并发观测指标随之变化。



### initDeps()
**Purpose**：初始化 Appium 客户端、并发控制器、快照缓存与制品上传。

**Logic — External Calls**
    - appium.NewClient()
    - worker.NewConcurrencyController()
    - artifacts.NewUploader()

**Logic — Local Implementation**
    - 初始化 SnapshotCache，配置 TTL=~1.5s。
    - 对 Appium 客户端设置 keep-alive 与超时。

**Errors & Edge Cases**
    - Appium 不可连 -> 阻断启动或进入降级（仅视觉定位）。

**Acceptance**
    - Appium RTT p95 < 350ms；快照命中率达到目标；上传通路可用。



### serveGRPC()/registerAndHeartbeat()/gracefulShutdown()
**Purpose**：暴露 gRPC 服务、注册至 Orchestrator 并定时心跳；优雅下线。

**Logic — External Calls**
    - orchestrator.RegisterWorker()
    - StartHeartbeat()

**Logic — Local Implementation**
    - 心跳失败累计 3 次仅报警；注册时带能力声明（caps）。

**Errors & Edge Cases**
    - 重复注册/ID 冲突 -> 采用租约键替换。

**Acceptance**
    - 中断网络后重连成功；停止时先注销再停服；≤30s 退出。


---

## internal/config/README.md

# internal/config

## loader.go


### Load()/MustLoad()
**Purpose**：装载配置为 Config 结构体。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 读取多源（文件/ENV），并做类型解析与默认值填充。
    - MustLoad 在错误时 panic/exit。

**Errors & Edge Cases**
    - JSON/YAML 解析失败 -> E.CONFIG.PARSE

**Acceptance**
    - 全部字段可被 ENV 覆盖；默认值表有单测。



### applyEnvOverrides(*Config)
**Purpose**：按命名约定将 ENV 覆盖到 Config。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 支持嵌套字段通过前缀展开，如 GATEWAY__PORT。
    - 布尔/数值/时间统一解析。

**Errors & Edge Cases**
    - ENV 值非法 -> E.CONFIG.ENV.INVALID

**Acceptance**
    - ENV 覆盖后再次校验通过。



### parseDurations(*Config)
**Purpose**：解析所有 duration 字段。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 统一 time.ParseDuration 并校验上/下界。

**Errors & Edge Cases**
    - 小于 0 或超上限 -> E.CONFIG.DURATION

**Acceptance**
    - 边界值用例通过；单位支持 ms/s/m/h。



### validate(*Config)
**Purpose**：必填、范围与互斥项校验。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 对互斥项（如 OIDC 与 PAT-only）做布尔代数检查。
    - 对端口/URL/枚举进行白名单校验。

**Errors & Edge Cases**
    - 冲突 -> E.CONFIG.CONFLICT

**Acceptance**
    - 覆盖 100% 互斥组合的表驱动测试。


---

## internal/auth/README.md

# internal/auth

## pat.go

### NewPATValidator(HashLookup) / Validate(token)
**Purpose**：校验 Personal Access Token。

**Logic — External Calls**
    - HashLookup.Lookup(tokenID)

**Logic — Local Implementation**
    - 拆解 token（id+签名），对比哈希并检查过期/撤销标记。
    - 审计：写入 subject、ip、user-agent。

**Errors & Edge Cases**
    - 哈希不匹配 -> 401；已撤销/过期 -> 403。

**Acceptance**
    - 伪造/过期 token 均拒绝；审计日志可检索。


## oidc.go

### NewOIDCValidator(jwksURL, audience) / Validate(jwt) / refreshJWKS()
**Purpose**：基于 JWKS 校验 OIDC JWT。

**Logic — External Calls**
    - 抓取 JWKS；按 kid 选择公钥；检查 aud/iss/exp/nbf。

**Logic — Local Implementation**
    - 缓存 JWKS，定期刷新；允许 ±skew 的时钟偏移。

**Errors & Edge Cases**
    - JWKS 不可达 -> 使用缓存；完全无缓存 -> 503。

**Acceptance**
    - aud/iss 严格匹配；时钟偏移容差内通过。


## hmac.go

### Verify(sig, payload, ts, nonce) / computeHMAC(secret, msg)
**Purpose**：校验请求签名与重放保护。

**Logic — External Calls**
    - Redis: SETNX nonce + EX
    - HMAC-SHA256 比较

**Logic — Local Implementation**
    - 构造消息串：method + path + ts + bodyhash。常量时比较避免时序攻击。

**Errors & Edge Cases**
    - 重复 nonce -> 409；过期 ts -> 401。

**Acceptance**
    - ±5 分钟窗口内有效；nonce 仅一次；失败均有标准化错误码。


## context.go

### WithSubject/SubjectFrom
**Purpose**：在 ctx 中携带主体信息。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 使用自定义 ctx key；跨协议透传。

**Errors & Edge Cases**
    - 空 subject -> 匿名/受限路由。

**Acceptance**
    - 在日志/trace 中均可见 subject 字段。


---

## internal/gateway/jsonrpc/README.md

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


### startSession / endSession
**Purpose**：启动与结束移动会话。

**Logic — External Calls**
    - orchestrator.StartSession()
    - orchestrator.EndSession()

**Logic — Local Implementation**
    - `startSession`：读取 `project/actor/labels/w3cCapsJson`，强制 `Idempotency-Key`，写入审计日志并回传 `sessionId` 与 `capabilities`。
    - `endSession`：根据 `sessionId` 调用 orchestrator；重复调用需直接返回 `ok=true`（幂等）。

**Errors & Edge Cases**
    - 会话不存在 -> E.SESSION.NOT_FOUND。
    - 缺少必填字段/Schema 校验失败 -> -32602 + 错误详情。

**Acceptance**
    - 幂等键命中返回首个响应；结束后 Orchestrator 释放资源并写事件。


### executePlan / getSemanticSnapshot / takeScreenshot / getArtifacts
**Purpose**：计划执行、语义树查询、截图与工件获取。

**Logic — External Calls**
    - orchestrator.ExecutePlan()
    - orchestrator.GetSemanticSnapshot()
    - orchestrator.TakeScreenshot()
    - orchestrator.GetArtifacts()

**Logic — Local Implementation**
    - `executePlan`：校验 plan schema（`additionalProperties:false`），生成 `traceId`（缺省则服务端生成）并返回 `traceId`、首个事件指针。
    - `getSemanticSnapshot`：支持 `sinceRev`，返回 `{changed, rev, snapshotRef, tree?}`；无变化时重用之前的 `rev/snapshotRef` 并标记 `changed=false`。
    - `takeScreenshot`：允许 `includeThumb`，返回 ArtifactRef；截屏前遮蔽 `secure` 区域，并生成最长边 640px 的缩略图与原图集合。
    - `getArtifacts`：分页/过滤 `types`、按 `cursor` 读取。

**Errors & Edge Cases**
    - 计划无效 -> E.PLAN.INVALID。
    - 并发配额不足 -> E.RATE.LIMITED。
    - snapshot 过期/会话失效 -> E.SESSION.DEAD。

**Acceptance**
    - 执行成功可通过 WebSocket/A2A 拉取事件；`getSemanticSnapshot` 命中缓存时返回 `rev` 不递增；截图/工件元数据遵循 schema。


### replay
**Purpose**：按 `traceId` 回放历史计划。

**Logic — External Calls**
    - orchestrator.ReplayPlan()

**Logic — Local Implementation**
    - 校验 `target`（auto/local/adf）；生成新的 `traceId`（除非 `reuseTraceId=true`）。
    - 回放前检查历史工件是否可用，否则返回错误。

**Errors & Edge Cases**
    - 源 `traceId` 不存在 -> E.TRACE.NOT_FOUND。
    - 目标执行环境不可用 -> E.SCHED.NO_WORKER。

**Acceptance**
    - 回放事件带 `phase=replay`；日志中指明来源 trace。


### subscribe / unsubscribe
**Purpose**：管理 WebSocket 渠道订阅。

**Logic — External Calls**
    - websocket.Hub.Subscribe()/Unsubscribe()

**Logic — Local Implementation**
    - `subscribe`：校验 trace 是否存在；创建 channelId 与过期时间；`sinceEventId` 仅在 Hub ring buffer 仍保留该事件时有效，超出缓冲需改用 A2A `GetEvents` 补齐。
    - `unsubscribe`：根据 `channelId` 释放资源，即便连接已断开也需幂等成功。

**Errors & Edge Cases**
    - 超过订阅上限 -> E.WS.TOO_MANY_SUBS。
    - 未找到 channelId -> 返回 `ok=true`（幂等）。

**Acceptance**
    - channelId 可用于诊断；退订后不再推送事件，Hub 队列长度归零。


### describeCapabilities
**Purpose**：对外暴露当前节点能力与版本信息。

**Logic — External Calls**
    - capabilities.Service.List()

**Logic — Local Implementation**
    - 汇总 `apiVersion`、`capabilities.planOps`、`features` 稳定级别、`extensions`、`deprecated.sunsetAt`。
    - 根据配置动态反映可用协议（JSON-RPC/A2A/StreamLogs 等）。

**Errors & Edge Cases**
    - 缺少关键配置 -> E.CONFIG.MISSING。

**Acceptance**
    - 输出结构与 `requirements.md` 示例一致；变更后 JSON Schema 校验通过。


### healthCheck
**Purpose**：检查 Gateway→Orchestrator→Worker 的链路健康状态。

**Logic — External Calls**
    - orchestrator.HealthCheck()

**Logic — Local Implementation**
    - 接收可选 `traceId`/`sessionId` 用于链路追踪；默认执行轻量探活（数据库读 + Redis ping + Worker 心跳延迟检查）。
    - 返回 `status`（`healthy|degraded|unavailable`）与诊断详情数组。

**Errors & Edge Cases**
    - 任一依赖不可达 -> `status=degraded/unavailable`，错误码映射 `E.HEALTH.DEGRADED` / `E.HEALTH.DOWN`。

**Acceptance**
    - `/capabilities`、JSON-RPC `describeCapabilities` 均可引用健康信息；Prometheus 指标与响应保持一致。


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


---

## internal/gateway/rest/README.md

# internal/gateway/rest

## router.go

### NewRouter
**Purpose**：注册 REST 路由与中间件。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 路由：/sessions, /plans:execute, /plans/{{id}}:cancel, /traces/{{id}}, /traces/{{id}}/events, /artifacts, /capabilities。
    - 中间件顺序：Auth→RateLimit→Logging→Handler。

**Errors & Edge Cases**
    - 路由冲突 -> 启动失败。

**Acceptance**
    - e2e 覆盖全部路由 2xx/4xx 路径；无 5xx。


## handlers.go

### CreateSession
**Purpose**：创建会话并返回资源位置。

**Logic — External Calls**
    - orchestrator.StartSession()
    - respondCreated()

**Logic — Local Implementation**
    - requireIdempotencyKey；写审计日志。

**Errors & Edge Cases**
    - 重复幂等键 -> 返回首个结果。

**Acceptance**
    - Location 头存在；返回 201；多次提交结果一致。


### ExecutePlan
**Purpose**：提交计划执行请求。

**Logic — External Calls**
    - orchestrator.ExecutePlan()

**Logic — Local Implementation**
    - 校验计划 schema；支持 HMAC 校验。

**Errors & Edge Cases**
    - 无效计划 -> 422；限流 -> 429。

**Acceptance**
    - 提交后可从 events 流拉取到首个事件。


### CancelPlan
**Purpose**：取消正在执行的计划。

**Logic — External Calls**
    - orchestrator.CancelPlan()

**Logic — Local Implementation**
    - 幂等：重复取消不报错。

**Errors & Edge Cases**
    - traceId 不存在 -> 404。

**Acceptance**
    - 1s 内 worker 停止发送事件；最终状态为 PlanCanceled。


### GetTrace / GetEvents / GetArtifacts
**Purpose**：查询执行结果、事件与制品。

**Logic — External Calls**
    - DAO.ListEvents/Artifacts
    - 游标分页

**Logic — Local Implementation**
    - sinceId/sinceTime 游标处理；空结果返回 204/空数组。

**Errors & Edge Cases**
    - 游标非法 -> 400。

**Acceptance**
    - 可持续拉取；顺序一致；无重复。


---

## internal/gateway/websocket/README.md

# internal/gateway/websocket

## hub.go

### NewHub/Run/Publish/Subscribe/Unsubscribe
**Purpose**：广播执行事件并管理订阅。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 每个 traceId 维护 ring buffer 与队列长度上报。
    - Publish 时如队列满则丢弃并计数。

**Errors & Edge Cases**
    - 单连接积压 -> 触发丢弃并记录。

**Acceptance**
    - at-most-once；丢弃计数与队列直方图上报。


### broadcast / metricsOnDrop
**Purpose**：内部发送与指标记录。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 非阻塞写；慢连接检测与断开。

**Errors & Edge Cases**
    - panic 防护；写失败 -> 断开连接。

**Acceptance**
    - Hub 不被慢连接拖垮；CPU/内存稳定。


## connection.go

### NewConn/Write/ReadPump/Close
**Purpose**：封装单个 WS 连接。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - ReadPump 处理 ping/pong/close；Write 限流与合并。

**Errors & Edge Cases**
    - 异常关闭 -> 清理订阅与资源。

**Acceptance**
    - 异常关闭不泄漏；慢连接不阻塞 Hub。


---

## internal/gateway/middleware/README.md

# internal/gateway/middleware

## auth.go

### Auth() / extractPAT / extractOIDC / verifyHMAC
**Purpose**：鉴权中间件，按路由策略选择 HMAC>OIDC>PAT。

**Logic — External Calls**
    - auth.PAT/OIDC/HMAC 验证
    - Redis 校验 nonce

**Logic — Local Implementation**
    - 从 Header 提取凭证；将 subject 写入 ctx。

**Errors & Edge Cases**
    - 凭证缺失 -> 401；签名错误/过期 -> 403。

**Acceptance**
    - 错误码一致；审计日志记录主体与路由。


## logging.go

### Logging()
**Purpose**：请求/响应日志贯穿。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 生成/提取 requestId；与 traceId 贯穿；敏感字段脱敏。

**Errors & Edge Cases**
    - 日志爆量 -> 采样限流。

**Acceptance**
    - 每个请求都有起止日志且可通过 ID 关联。


## ratelimit.go

### RateLimit() / keyFunc / allowNow
**Purpose**：按 subject/route 限流。

**Logic — External Calls**
    - Redis INCR + TTL

**Logic — Local Implementation**
    - 计算 key；超配额返回 429。

**Errors & Edge Cases**
    - Redis 故障 -> 进入漏斗退化策略。

**Acceptance**
    - 命中 429 时长与复位时间可从响应/指标获知。


---

## internal/gateway/capabilities/README.md

# internal/gateway/capabilities

## service.go

### NewService(cfg) / List(ctx) / featureFlagsFromConfig
**Purpose**：对外声明当前节点能力。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 汇总开关与版本，生成 Capabilities JSON。

**Errors & Edge Cases**
    - 缺少必要字段 -> 500。

**Acceptance**
    - 字段包含 apiVersion/planOps/features/extensions/deprecated.sunsetAt；与示例一致。


---

## internal/orchestrator/README.md

# internal/orchestrator

## service.go

### StartSession/ExecutePlan/CancelPlan/HealthCheck/GetTrace/GetEvents/ReplayPlan/TakeScreenshot/StartArtifactUpload/CompleteArtifactUpload
**Purpose**：编排层核心 API。

**Logic — External Calls**
    - DAO.* 持久化
    - EventsPublisher.Publish()
    - WorkerRegistry.Assign()
    - WorkerRegistry.HealthSnapshot()
    - S3 Presign/Multipart

**Logic — Local Implementation**
    - 参数校验→分配 worker→建 trace→发布事件→更新状态→返回。
    - withPlanTimeout 合并超时（Step>Plan>Global）。
    - HealthCheck 聚合 Postgres/Redis/Worker 心跳信息并输出诊断列表。
    - 取消为幂等操作。

**Errors & Edge Cases**
    - 分配失败 -> E.SCHED.NO_WORKER
    - 事务失败 -> 回滚并返回错误事件

**Acceptance**
    - 事件顺序与 seq 连续；取消后最终状态一致。


## grpc_server.go

### NewGRPCServer/Start/Stop（含拦截器）
**Purpose**：对外暴露 gRPC 服务。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 拦截器：unaryAuth/unaryMetrics/unaryTracing；限制最大消息大小。

**Errors & Edge Cases**
    - 鉴权失败 -> UNAUTHENTICATED/ PERMISSION_DENIED。

**Acceptance**
    - grpcurl 验收通过；metrics/trace 可观察。


## dispatcher.go

### NewDispatcher/EnqueuePlan/dispatchLoop/startPlanOnWorker/Cancel
**Purpose**：计划入队、消费与投递。

**Logic — External Calls**
    - Redis Streams：XADD/XREADGROUP/XACK/XAUTOCLAIM/XTRIM
    - Worker RPC

**Logic — Local Implementation**
    - ensureConsumerGroups；一致性哈希 shardFor(project)；失败重试与退避。

**Errors & Edge Cases**
    - 跨实例重复投递 -> 通过 XGROUP/XAUTOCLAIM 防重。

**Acceptance**
    - 不重投不丢失；修剪策略生效（时间/条数）。


## worker_registry.go

### Register/Heartbeat/Assign/AvailableWorkers/StartMonitor
**Purpose**：维持 worker 目录与选址。

**Logic — External Calls**
    - Redis Hash/TTL 租约

**Logic — Local Implementation**
    - syncFromRedis 合并快照；leaseKey 续约；基于标签/负载的 Assign。

**Errors & Edge Cases**
    - 误判离线 -> 先降权再剔除。

**Acceptance**
    - 误判率 < 0.1%；分配倾斜度在阈值内。


## session_store.go

### CreateSession/GetSession/UpdateStatus/ListSessionsByProject
**Purpose**：存取 Session 状态。

**Logic — External Calls**
    - DAO.* 调用

**Logic — Local Implementation**
    - 悲观/乐观锁避免脏写；必要索引覆盖。

**Errors & Edge Cases**
    - 并发更新冲突 -> 重试。

**Acceptance**
    - 无脏写；查询命中索引。


## fallback.go

### CheckAndFallback(traceId, plan)
**Purpose**：ADF 队列超时回退到本地执行或失败。

**Logic — External Calls**
    - 读取 ADF 队列等待时间/阈值

**Logic — Local Implementation**
    - 超过阈值则标记 phase=fallback 并切换策略。

**Errors & Edge Cases**
    - 回退失败 -> 返回明确错误码。

**Acceptance**
    - 超时必触发回退/失败路径且事件带标记。


## events_publisher.go

### Publish(event)
**Purpose**：事件发布至 WS Hub 并确保落库已完成。

**Logic — External Calls**
    - 调用 Hub.Publish

**Logic — Local Implementation**
    - 先持久化后发布；WebSocket 仅提供 at-most-once，丢弃会记录指标，客户端需通过 A2A `GetEvents(sinceEventId)` 补齐。

**Errors & Edge Cases**
    - Hub 背压 -> 计数后丢弃但不影响落库。

**Acceptance**
    - WS 丢弃不反压 Orchestrator；指标可观测。


---

## internal/worker/README.md

# internal/worker

## grpc_server.go

### StartSession/EndSession/ExecutePlan/GetSemanticSnapshot/TakeScreenshot/StreamLogs/UploadArtifacts/CancelPlan
**Purpose**：Worker 侧对客户端暴露的 gRPC RPC 集合。

**Logic — External Calls**
    - 向 Orchestrator 回传事件流
    - artifacts.Uploader / S3 Presign（当触发直传兜底）

**Logic — Local Implementation**
    - `StartSession`：创建/恢复 Appium 会话，登记心跳与并发槽。
    - `EndSession`：释放设备与会话资源，允许幂等。
    - `ExecutePlan`：执行前 Acquire 并发，执行后 Release；事件含 `phase`/`metrics`。
    - `GetSemanticSnapshot`：配合 snapshot cache 与 Redis 差量返回，统一返回 `{changed, rev, snapshotRef, tree?}`；命中缓存时直接复用缓存条目。
    - `TakeScreenshot`：捕获屏幕后执行 `secure` 区域遮蔽，生成原图与最长边 640px 缩略图，再上传（<阈值走 gRPC 流，>=阈值触发分片直传）。
    - `StreamLogs`：按 `traceId` 推送 `LogEvent`，客户端流式订阅，背压时降级；不可用时发送终止错误并记录指标。
    - `UploadArtifacts`：客户端流式 `ArtifactChunk` 聚合，校验 `traceId+key` 幂等，支持断点重传与 sha256 校验；`eof=true` 触发写元数据并确认。
    - `CancelPlan`：取消时中断上下文，1s 内停止事件；重复取消直接返回 OK。

**Errors & Edge Cases**
    - 取消未中断 -> BUG；应 1s 内停止发送。
    - 日志通道不可用 -> 返回 E.LOGS.UNAVAILABLE 并关闭流。
    - 重复/乱序分片 -> 拒绝并返回 E.STORAGE.ARTIFACT_WRITE；客户端需重传。

**Acceptance**
    - 并发槽严格；取消生效≤1s；事件含 `phase/metrics`。
    - `StreamLogs` 背压有指标 `stream_logs_backpressure_total`；断开后可重连续订。
    - `UploadArtifacts` 具备重试用例：重复上传返回首个结果；最大分片、总大小均校验。


## executor.go

### Execute(ctx, req) -> (events<-chan, errs<-chan)
**Purpose**：逐步执行 plan 并发出事件。

**Logic — External Calls**
    - getHandler(op) 查找处理器

**Logic — Local Implementation**
    - 构建 StepContext；emit 非阻塞（有背压保护）；finalize 统一出口。

**Errors & Edge Cases**
    - 处理器缺失 -> E.STEP.UNSUPPORTED
    - 长时间无进展 -> 超时事件

**Acceptance**
    - 序列中的每步都生成 begin/finish；失败截图入 Artifact。


## step_handlers/*

### HandleTapAndWaitVisible / findElement / waitVisible 等
**Purpose**：元素定位 + 行为 + 验证 + 失败截图。

**Logic — External Calls**
    - Appium.FindElement/Click
    - 视觉定位兜底（远程/本地）

**Logic — Local Implementation**
    - “选择器回退矩阵”：id→accessibilityId→xpath→视觉模板。
    - waitVisible 使用指数回退直到超时。

**Errors & Edge Cases**
    - 误点/误检 -> 二次确认（可选文本断言）。

**Acceptance**
    - 统一错误码；失败必附截图；事件聚合含耗时与重试次数。


## contextmgr

### GetContexts/SwitchTo/EnsureWebViewReady
**Purpose**：WebView/Native 上下文切换。

**Logic — External Calls**
    - Appium get/set context

**Logic — Local Implementation**
    - EnsureWebViewReady 轮询可达 DOM，设置最大 5s。

**Errors & Edge Cases**
    - 切换失败 -> 回退 native 并告警。

**Acceptance**
    - 5s 内成功或明确失败；能力声明 webview=false 时跳过。


## locators/visual

### MatchTemplateLocal/MatchTemplateRemote/BestOf
**Purpose**：模板匹配定位。

**Logic — External Calls**
    - 远端 gRPC 模型（可选）

**Logic — Local Implementation**
    - OpenCV 本地匹配；双路结果融合（阈值/置信）。

**Errors & Edge Cases**
    - 远端超时 -> 自动降级本地。

**Acceptance**
    - 两实现误差 ≤3%；失败不阻断其他定位策略。


## snapshot_cache.go

### Get/Set/Invalidate
**Purpose**：页面结构快照缓存。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 维护单调递增的 `rev` 与对应 `snapshotRef`；Get 返回 `{changed, rev, snapshotRef, tree?}`，缓存命中时直接复用引用。

**Errors & Edge Cases**
    - TTL 过短导致抖动 -> 调参至 ~1.5s。

**Acceptance**
    - 命中率与 miss 指标达标；`rev` 单调递增且 `snapshotRef` 在 `changed=false` 时保持稳定。


## artifacts.go

### Upload/CaptureAndUploadScreenshot/StartMultipart/CompleteMultipart/AbortMultipart
**Purpose**：制品上传与截图。

**Logic — External Calls**
    - S3 Presign/Multipart

**Logic — Local Implementation**
    - 截图→遮蔽 `secure` 区域→生成最长边 640px 缩略图→上传；≥5MB 走并发分片；校验 ETag/sha256。

**Errors & Edge Cases**
    - 上传失败 -> 重试退避；最终失败记录错误事件。

**Acceptance**
    - 大文件分片；幂等完成；遮蔽内容在 Artifact 中不可见；缩略图最长边固定 640px；p95 截图延迟下降 ≥30%。


## heartbeat.go

### StartHeartbeat/StopHeartbeat
**Purpose**：周期上报状态与度量。

**Logic — External Calls**
    - Orchestrator.Heartbeat()

**Logic — Local Implementation**
    - Ticker 触发；连续 3 次失败仅报警。

**Errors & Edge Cases**
    - 上报阻塞 -> 超时保护。

**Acceptance**
    - 断网恢复后自动恢复心跳；不中断服务。


## concurrency.go

### NewController/Acquire/Release/WithPermit
**Purpose**：并发槽控制。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 基于信号量或 channel；WithPermit 包装执行函数。

**Errors & Edge Cases**
    - Acquire 超时 -> 返回可重试错误。

**Acceptance**
    - 超限严格阻塞；取消可中断。


## metrics.go

### RegisterWorkerMetrics/ObserveStepMetrics/ObserveAppiumRTT
**Purpose**：上报核心指标。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 维度：tenant/project/step/op；RTT 采样并计算 p95。

**Errors & Edge Cases**
    - 指标爆量 -> 采样。

**Acceptance**
    - Dashboard 可复盘一次完整执行链路。


---

## internal/worker/appium/README.md

# internal/worker/appium

## client.go

### NewClient/NewSession/DeleteSession
**Purpose**：管理 Appium 会话生命周期。

**Logic — External Calls**
    - HTTP 调用 /session 创建/删除

**Logic — Local Implementation**
    - 设置默认 headers/超时/keep-alive。

**Errors & Edge Cases**
    - 创建失败 -> 重试 3 次；仍失败返回上层。

**Acceptance**
    - 会话泄漏监控；删除成功率≥99.9%。



### FindElement/Click/SendKeys/Screenshot/PageSource
**Purpose**：元素查找与交互。

**Logic — External Calls**
    - HTTP 调用 Appium API

**Logic — Local Implementation**
    - 统一 do() 实现方法/路径/错误映射。SendKeys 支持 secure。

**Errors & Edge Cases**
    - 元素不存在 -> E.APP.ELEM_NOT_FOUND

**Acceptance**
    - 错误码映射准确；SendKeys.isSecure 不落日志。



### do(ctx, method, path, body, v)
**Purpose**：统一 HTTP 访问与错误处理。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 实现重试策略：仅 ENETRESET/5xx；指数退避与上限。
    - 将 Appium 错误翻译成内部码。

**Errors & Edge Cases**
    - 超时 -> E.APP.TIMEOUT；连接复用失败 -> 重建。

**Acceptance**
    - 连接复用有效；重试次数符合策略。


---

## internal/storage/README.md

# internal/storage

- [redis](redis/README.md)
- [postgres](postgres/README.md)
- [s3](s3/README.md)


---

## internal/storage/redis/README.md

# internal/storage/redis

## cache.go

### NewCache/Get/Set/Del/StoreNonce
**Purpose**：键值存取与 nonce 去重。

**Logic — External Calls**
    - Redis PING
    - SET NX EX

**Logic — Local Implementation**
    - 统一序列化协议（JSON/MsgPack）；支持 TTL。

**Errors & Edge Cases**
    - Redis 不可达 -> 短暂回退内存（可选）。

**Acceptance**
    - nonce 幂等；slot 计数一致；延迟在阈值内。



### AcquireConcurrencySlot/ReleaseConcurrencySlot
**Purpose**：按 tenant/project 维度配额。

**Logic — External Calls**
    - INCR/DECR + EXPIRE

**Logic — Local Implementation**
    - 溢出检测并拒绝。

**Errors & Edge Cases**
    - 并发更新冲突 -> 重试。

**Acceptance**
    - 配额不越限；释放后可立即再次获取。


## streams.go

### XAdd/EnsureGroup/XReadGroup/XAck/XAutoClaim/XTrim
**Purpose**：Redis Streams 操作封装。

**Logic — External Calls**
    - XGROUP CREATE
    - XREADGROUP
    - XAUTOCLAIM

**Logic — Local Implementation**
    - 按项目做分片流名；统一序列化。

**Errors & Edge Cases**
    - 消费者漂移 -> XAUTOCLAIM 接管。

**Acceptance**
    - 可续读；闲置转移成功；修剪策略生效。


---

## internal/storage/postgres/README.md

# internal/storage/postgres

## dao.go

### Traces：CreateTrace/GetTrace/UpdateTraceStatus/ListByProject
**Purpose**：追踪实体 CRUD。

**Logic — External Calls**
    - SQL 执行

**Logic — Local Implementation**
    - 使用事务与索引；避免 N+1；状态机校验。

**Errors & Edge Cases**
    - 状态非法转换 -> 409。

**Acceptance**
    - Explain 计划命中索引；并发下无脏写。


### Events：InsertPlanEvent/ListEvents(traceId, sinceId, limit)
**Purpose**：事件写入与分页读取。

**Logic — External Calls**
    - 批量插入/游标分页

**Logic — Local Implementation**
    - 保证 seq 单调；limit 上限。

**Errors & Edge Cases**
    - sinceId 非法 -> 400。

**Acceptance**
    - 顺序一致；无重复；分页稳定。


### Artifacts：InsertArtifact/ListArtifacts(traceId)
**Purpose**：制品索引存取。

**Logic — External Calls**
    - SQL + S3 key 关联

**Logic — Local Implementation**
    - 保存缩略图元信息与哈希。

**Errors & Edge Cases**
    - 重复 key -> 幂等返回。

**Acceptance**
    - 列举完整；与 S3 实物一致。


### Sessions：CreateSession/GetSession/UpdateStatus/ListByProject
**Purpose**：会话状态存取。

**Logic — External Calls**
    - SQL

**Logic — Local Implementation**
    - 乐观锁或版本号字段防止覆盖写。

**Errors & Edge Cases**
    - 版本冲突 -> 重试。

**Acceptance**
    - 一致性良好；查询命中索引。


---

## internal/storage/s3/README.md

# internal/storage/s3

## client.go

### NewClient/PresignPut/PresignGet
**Purpose**：S3 基础封装。

**Logic — External Calls**
    - AWS SDK

**Logic — Local Implementation**
    - Presign 带内容类型/过期时间；最小权限策略。

**Errors & Edge Cases**
    - 凭证错误 -> 403；区域不匹配 -> 400。

**Acceptance**
    - Presign 链接可用且时效正确。


### InitiateMultipart/UploadPart/CompleteMultipart/AbortMultipart
**Purpose**：多分片上传。

**Logic — External Calls**
    - AWS SDK Multipart API

**Logic — Local Implementation**
    - 限制最小分片 5MB；校验 ETag 列表；失败自动 Abort。

**Errors & Edge Cases**
    - 分片丢失 -> 失败并清理。

**Acceptance**
    - 大文件成功率高；可断点续传；完成后可下载校验。


---

## telemetry/README.md

# telemetry

## metrics.go

### InitMetrics/RegisterXYZ
**Purpose**：注册与暴露统一指标。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 定义指标：execute_latency_seconds、appium_rtt_ms、concurrency_in_use、queue_wait_seconds、rate_limit_hits_total、websocket_drops_total、ws_queue_len_histogram、adf_lease_success_rate、artifact_upload_latency。
    - 注册 /metrics handler（或导出到已有端点）。

**Errors & Edge Cases**
    - 重复注册 -> 忽略或告警。

**Acceptance**
    - /metrics 可抓取；各指标随业务变化正确变化。


## tracing.go

### InitTracer/StartSpan
**Purpose**：配置 OTEL 导出与起 span。

**Logic — External Calls**
    - OTLP/Jaeger 导出器

**Logic — Local Implementation**
    - 注入 traceId/sessionId/stepIndex 属性；采样策略配置。

**Errors & Edge Cases**
    - 导出异常 -> 降级本地采样。

**Acceptance**
    - 上下游 trace 可串起完整链路。


## logging.go

### NewLogger/With
**Purpose**：结构化日志。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - JSON 编码；脱敏策略；注入 requestId/traceId。

**Errors & Edge Cases**
    - 敏感字段未脱敏 -> 阻断 CI。

**Acceptance**
    - 随机抽样日志检查通过；字段完整且可检索。


---

## errors/README.md

# errors

## codes.go

### New/MapToGRPC/MapToHTTP/MapToJSONRPC
**Purpose**：统一内部错误码与跨协议映射。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 维护错误码表：E.CONFIG.*, E.SCHEMA.*, E.SCHED.*, E.APP.*, E.RATE.LIMITED, E.WS.BACKPRESSURE_DROP 等。

**Errors & Edge Cases**
    - 映射遗漏 -> 单测失败。

**Acceptance**
    - 相同内部码在三种协议上得到一致、可预期的状态码/错误体。


---

## util/README.md

# util

## idempotency.go

### Check/Remember
**Purpose**：基于 Redis 的幂等存根。

**Logic — External Calls**
    - Redis SET NX EX

**Logic — Local Implementation**
    - value 保存首个响应摘要；重复返回相同值。

**Errors & Edge Cases**
    - 键空间膨胀 -> 定期清理策略。

**Acceptance**
    - 重复提交返回首个结果；TTL 默认 24h 可配置。


## backoff.go

### Exponential(attempt, base, cap)
**Purpose**：指数退避与可选抖动。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 实现 jitter；不超过 cap。

**Errors & Edge Cases**
    - 溢出 -> 截断。

**Acceptance**
    - 统计分布符合预期；无超过 cap。


## timeouts.go

### Merge(planDefaults, stepOverrides)
**Purpose**：合并超时配置。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 优先级：Step > Plan > Global；提供默认表。

**Errors & Edge Cases**
    - 覆盖后出现 0/负数 -> 纠正为默认。

**Acceptance**
    - 边界组合测试通过。


## mask.go

### MaskSecrets(s string)
**Purpose**：日志脱敏。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 对 token/password/secret/pin、secure 输入做遮盖。

**Errors & Edge Cases**
    - 误脱敏/漏脱敏 -> 单测红线。

**Acceptance**
    - 随机样本检查不过泄密。
