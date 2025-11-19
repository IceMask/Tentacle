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


### GetCapabilities
**Purpose**：以 REST 形式返回节点能力声明。

**Logic — External Calls**
    - capabilities.Service.List()

**Logic — Local Implementation**
    - 复用 JSON-RPC `describeCapabilities` 输出；根据配置反映协议开关、feature 稳定级别。

**Errors & Edge Cases**
    - 缺少配置/字段 -> 500（E.CONFIG.MISSING）。

**Acceptance**
    - 返回值通过 JSON schema；与 JSON-RPC 版本一致；带鉴权与审计日志。
