flowchart TD
  %% ========= Ingress =========
  A[Client<br/>REST / JSON-RPC] --> B[Gateway]
  A2[Client<br/>WebSocket] --> WS[WS Hub]

  subgraph G1[Gateway]
    B --> B1[Auth 中间件<br/>HMAC / OIDC / PAT]
    B1 --> B2[RateLimit / 配额探测]
    B2 --> B3{Idempotency-Key 命中?}
    B3 -- 是 --> B3r[返回缓存结果]
    B3 -- 否 --> B4[Schema 校验<br/>JSON-RPC/REST]
    B4 --> B5[路由: /sessions /plans:execute /cancel ...]
  end

  %% ========= Orchestrator =========
  B5 --> O[Orchestrator]
  subgraph O1[Orchestrator]
    O --> O0[DAO / Cache / S3 Client 就绪检查]
    O0 --> O1a[CreateTrace / 事务]
    O1a --> O2[EventsPublisher<br/>插入事件(PlanQueued)]
    O2 --> O3[Dispatcher<br/>写入 Redis Streams 分片]
    O3 --> O4[WorkerRegistry.Assign<br/>选择合适 Worker/ADF]
    O4 --> O5{ADF 租赁超时?}
    O5 -- 是 --> O5f[Fallback=本地执行<br/>事件 phase=fallback]
    O5 -- 否 --> O6[派发作业给 Worker]
  end

  %% ========= Worker 执行 =========
  O6 --> W[Worker]
  subgraph W1[Worker]
    W --> W0[并发控制器 Acquire]
    W0 --> W1a[Appium Client<br/>会话建立/恢复]
    W1a --> W2[执行计划步骤循环]
    subgraph STEP[Step 执行]
      direction TB
      S1[定位策略矩阵<br/>and/near→视觉兜底] --> S2[动作执行]
      S2 --> S3[可选验证/失败截图]
      S3 --> S4[emit PlanEvent<br/>非阻塞/背压保护]
    end
    W2 --> STEP --> W3[心跳/指标上报]
    W3 --> W4{取消/超时?}
    W4 -- 是 --> W4c[停止≤1s；最终事件=PlanCanceled]
    W4 -- 否 --> W5[完成/失败；最终事件=PlanFinished/Failed]
    W5 --> W6[并发控制器 Release]
  end

  %% ========= 事件与制品 =========
  W4c -->|事件流| EDB[(Postgres<br/>Traces/Events)]
  W5 -->|事件流| EDB
  W -->|Screenshot/Logs 大文件| S3[S3 对象存储]
  S3 -.-> O7[CompleteArtifactUpload<br/>ETag/SHA256 校验]

  %% ========= 发布与补偿 =========
  EDB --> PUB[EventsPublisher<br/>广播触发]
  PUB --> WS
  WS -->|实时 at-most-once| A2[Client WS 订阅者]
  EDB --> A3[GetEvents(sinceId)<br/>(A2A 补偿)]

  %% ========= 观测与链路 =========
  B & O & W --> M[Metrics / Prometheus]
  B & O & W --> T[OTEL Tracing]
  classDef faded fill:#f7f7f7,stroke:#bbb,color:#333;

  %% notes
  class S3,T,M faded
