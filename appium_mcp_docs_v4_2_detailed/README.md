# Module I v4.2 — 函数方法清单补遗（需求落地）

> 本套文档按仓库目录拆分，逐文件列出方法级需求与验收点；不改变既有职责与流程，仅补齐遗漏的子步骤/调用点与统一验收指标。

目录导航：
- [cmd/gateway](cmd/gateway/README.md)
- [cmd/orchestrator](cmd/orchestrator/README.md)
- [cmd/worker](cmd/worker/README.md)
- [internal/config](internal/config/README.md)
- [internal/auth](internal/auth/README.md)
- [internal/gateway/jsonrpc](internal/gateway/jsonrpc/README.md)
- [internal/gateway/rest](internal/gateway/rest/README.md)
- [internal/gateway/websocket](internal/gateway/websocket/README.md)
- [internal/gateway/middleware](internal/gateway/middleware/README.md)
- [internal/gateway/capabilities](internal/gateway/capabilities/README.md)
- [internal/orchestrator](internal/orchestrator/README.md)
- [internal/worker](internal/worker/README.md)
- [internal/storage](internal/storage/README.md)
- [telemetry](telemetry/README.md)
- [errors](errors/README.md)
- [util](util/README.md)

## 统一验收清单（v4.2 汇总）
1. 协议一致性：REST/JSON-RPC/gRPC 错误码映射一致；事件体 ≤64KB 超限落 Artifact。
2. 可靠性：WS at-most-once，断线可 A2A 补齐；Hub 背压丢弃有直方图指标。
3. 幂等/安全：Idempotency-Key 生效；HMAC 时间窗与 nonce 去重；日志脱敏。
4. ADF/配额：ADF 成功率 ≥98%；超时回退；并发不越限且公平。
5. 性能：Appium RTT p95 < 350ms；大对象直传/分片 p95 截图延迟降低 ≥30%。
6. 可观测：指标清单齐全；OTEL 全链路（traceId/sessionId/stepIndex）。
7. 恢复性：崩溃恢复成功 ≥90%，≤10s；失败≤2s 返回 E.SESSION.BROKEN。
8. 安全基线：TLS1.2+（推荐 1.3），证书/密钥轮换演练通过。
