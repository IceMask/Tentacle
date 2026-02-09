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
    - 限制最大消息大小与并发；注册 HealthCheck 服务供 gateway/ops 探活。

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
