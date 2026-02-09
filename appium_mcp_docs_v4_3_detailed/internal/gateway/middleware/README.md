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
