# internal/storage/postgres

## dao.go

### Traces：CreateTrace/GetTrace/UpdateTraceStatus/ListByProject
**Purpose**：追踪实体 CRUD。

**Logic — External Calls**
    - SQL 执行

**Logic — Local Implementation**
    - 使用事务与索引；避免 N+1；状态机校验。

**Errors & Edge Cases**
    - 状态非法转换 -> 409。

**Acceptance**
    - Explain 计划命中索引；并发下无脏写。


### Events：InsertPlanEvent/ListEvents(traceId, sinceId, limit)
**Purpose**：事件写入与分页读取。

**Logic — External Calls**
    - 批量插入/游标分页

**Logic — Local Implementation**
    - 保证 seq 单调；limit 上限。

**Errors & Edge Cases**
    - sinceId 非法 -> 400。

**Acceptance**
    - 顺序一致；无重复；分页稳定。


### Artifacts：InsertArtifact/ListArtifacts(traceId)
**Purpose**：制品索引存取。

**Logic — External Calls**
    - SQL + S3 key 关联

**Logic — Local Implementation**
    - 保存缩略图元信息与哈希。

**Errors & Edge Cases**
    - 重复 key -> 幂等返回。

**Acceptance**
    - 列举完整；与 S3 实物一致。


### Sessions：CreateSession/GetSession/UpdateStatus/ListByProject
**Purpose**：会话状态存取。

**Logic — External Calls**
    - SQL

**Logic — Local Implementation**
    - 乐观锁或版本号字段防止覆盖写。

**Errors & Edge Cases**
    - 版本冲突 -> 重试。

**Acceptance**
    - 一致性良好；查询命中索引。
