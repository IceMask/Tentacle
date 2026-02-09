# internal/storage/redis

## cache.go

### NewCache/Get/Set/Del/StoreNonce
**Purpose**：键值存取与 nonce 去重。

**Logic — External Calls**
    - Redis PING
    - SET NX EX

**Logic — Local Implementation**
    - 统一序列化协议（JSON/MsgPack）；支持 TTL。

**Errors & Edge Cases**
    - Redis 不可达 -> 短暂回退内存（可选）。

**Acceptance**
    - nonce 幂等；slot 计数一致；延迟在阈值内。



### AcquireConcurrencySlot/ReleaseConcurrencySlot
**Purpose**：按 tenant/project 维度配额。

**Logic — External Calls**
    - INCR/DECR + EXPIRE

**Logic — Local Implementation**
    - 溢出检测并拒绝。

**Errors & Edge Cases**
    - 并发更新冲突 -> 重试。

**Acceptance**
    - 配额不越限；释放后可立即再次获取。


## streams.go

### XAdd/EnsureGroup/XReadGroup/XAck/XAutoClaim/XTrim
**Purpose**：Redis Streams 操作封装。

**Logic — External Calls**
    - XGROUP CREATE
    - XREADGROUP
    - XAUTOCLAIM

**Logic — Local Implementation**
    - 按项目做分片流名；统一序列化。

**Errors & Edge Cases**
    - 消费者漂移 -> XAUTOCLAIM 接管。

**Acceptance**
    - 可续读；闲置转移成功；修剪策略生效。
