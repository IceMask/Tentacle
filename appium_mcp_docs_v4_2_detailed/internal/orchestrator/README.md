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
    - HealthCheck 汇总数据库、Redis 连接与 Worker 心跳延迟，生成诊断。
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
    - 先持久化后发布；WS 通道仅保证 at-most-once，丢弃会记录指标，调用方需通过 A2A `GetEvents(sinceEventId)` 补齐。

**Errors & Edge Cases**
    - Hub 背压 -> 计数后丢弃但不影响落库。

**Acceptance**
    - WS 丢弃不反压 Orchestrator；指标可观测。
