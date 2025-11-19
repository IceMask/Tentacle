# internal/gateway/websocket

## hub.go

### NewHub/Run/Publish/Subscribe/Unsubscribe
**Purpose**：广播执行事件并管理订阅。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 每个 traceId 维护 ring buffer 与队列长度上报；只保证 ring buffer 内的事件可被 `sinceEventId` 补偿。
    - Publish 时如队列满则丢弃并计数；超出 ring buffer 的补齐需依赖 A2A `GetEvents`。

**Errors & Edge Cases**
    - 单连接积压 -> 触发丢弃并记录。

**Acceptance**
    - at-most-once；丢弃计数与队列直方图上报。


### broadcast / metricsOnDrop
**Purpose**：内部发送与指标记录。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 非阻塞写；慢连接检测与断开。

**Errors & Edge Cases**
    - panic 防护；写失败 -> 断开连接。

**Acceptance**
    - Hub 不被慢连接拖垮；CPU/内存稳定。


## connection.go

### NewConn/Write/ReadPump/Close
**Purpose**：封装单个 WS 连接。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - ReadPump 处理 ping/pong/close；Write 限流与合并。

**Errors & Edge Cases**
    - 异常关闭 -> 清理订阅与资源。

**Acceptance**
    - 异常关闭不泄漏；慢连接不阻塞 Hub。
