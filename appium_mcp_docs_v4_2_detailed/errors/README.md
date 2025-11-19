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
