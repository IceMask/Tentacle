# internal/worker

## grpc_server.go

### StartSession/EndSession/ExecutePlan/GetSemanticSnapshot/TakeScreenshot/StreamLogs/UploadArtifacts/HealthCheck/CancelPlan
**Purpose**：Worker 侧对客户端暴露的 gRPC API。

**Logic — External Calls**
    - 向 Orchestrator 回传事件流
    - artifacts.Uploader / S3 Presign（当触发直传兜底）

**Logic — Local Implementation**
    - `StartSession`：创建或恢复 Appium 会话，登记并发槽；回写 session 元数据。
    - `EndSession`：释放设备资源，幂等处理重复调用。
    - `ExecutePlan`：Acquire 并发 → 执行 → Release；事件含 `phase`、指标、工件引用。
    - `GetSemanticSnapshot`：结合缓存与 Redis 差量返回，统一返回 `{changed, rev, snapshotRef, tree?}`；命中缓存直接复用 `rev/snapshotRef`，失效时回源 Appium。
    - `TakeScreenshot`：执行截图并遮蔽 `secure` 区域，生成原图与最长边 640px 的缩略图，按阈值决定直传或内联上传。
    - `StreamLogs`：按 trace 推送 `LogEvent`，背压统计并在不可用时返回 `E.LOGS.UNAVAILABLE`。
    - `UploadArtifacts`：客户端流式聚合 `ArtifactChunk`，基于 `traceId+key` 幂等校验，验签 sha256/ETag；`eof` 触发持久化与确认。
    - `HealthCheck`：返回节点健康状态（Appium 连通性、最后心跳延迟、依赖状态）；供 orchestrator 周期探测。
    - `CancelPlan`：取消时中断上下文，1s 内停止事件；重复取消直接返回成功。

**Errors & Edge Cases**
    - 取消未中断 -> BUG；应 1s 内停止发送。
    - 日志管道异常 -> 记录 `stream_logs_backpressure_total` 并结束流。
    - 重复/乱序分片 -> 返回 E.STORAGE.ARTIFACT_WRITE，客户端需重传。
    - 健康探测失败 -> 返回 `E.HEALTH.DEGRADED` 并列出具体依赖。

**Acceptance**
    - 并发槽严格；取消生效≤1s；事件含 phase/metrics。
    - `StreamLogs` 背压/不可用有指标与错误码；断线后可换流继续。
    - `UploadArtifacts` 重复提交返回首个结果；分片/总大小校验到位。
    - `HealthCheck` 在依赖故障时准确反映状态并记录指标。


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
    - 维护 rev；Get 返回是否变化与最新树。

**Errors & Edge Cases**
    - TTL 过短导致抖动 -> 调参至 ~1.5s。

**Acceptance**
    - 命中率与 miss 指标达标；一致性正确。


## artifacts.go

### Upload/CaptureAndUploadScreenshot/StartMultipart/CompleteMultipart/AbortMultipart
**Purpose**：制品上传与截图。

**Logic — External Calls**
    - S3 Presign/Multipart

**Logic — Local Implementation**
    - 截图→缩略图→上传；≥5MB 走并发分片；校验 ETag/sha256。

**Errors & Edge Cases**
    - 上传失败 -> 重试退避；最终失败记录错误事件。

**Acceptance**
    - 大文件分片；幂等完成；p95 截图延迟下降 ≥30%。


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
