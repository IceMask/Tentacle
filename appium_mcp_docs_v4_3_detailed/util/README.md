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
