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
