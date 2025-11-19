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
