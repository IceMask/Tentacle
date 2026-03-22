# design_patch_auth_lease_ws_v4.3.md — 鉴权、任务 Lease 与 WebSocket 安全设计补丁（草案）

> **状态**：Draft / 待评审  
> **适用范围**：Module I  
> **目的**：补齐当前 v4.3 需求与设计文档中关于 PAT/OIDC/HMAC、Worker 心跳/任务归属、WebSocket 安全、TLS 边界与审计模型的缺口，减少实现阶段的歧义与返工。

## 1. 背景

当前文档已经给出了安全与健康的大方向，但在以下问题上仍缺少可直接实现的契约：

- PAT 仅定义了“哈希存储、轮换/吊销”，未定义权威数据源、查询模型、生命周期字段与审计要求。
- OIDC 在文档中写的是 “JWKS 缓存、aud/iss 校验”，但没有明确 `issuer_url` 与 `jwks_url` 的关系。
- HMAC 仅定义了验签方向，没有定义密钥注册表、轮换窗口、多租户隔离与 canonical request。
- Worker 心跳仅定义了频率与摘除阈值，没有定义任务 ownership、lease 续约、orphan trace 的处理规则。
- WebSocket 只定义了订阅入口，没有定义浏览器握手鉴权、订阅 token、origin allowlist 与租户边界。
- TLS/mTLS 写了目标，没有定义哪些链路必须启用、哪些链路允许分阶段落地。

本补丁的目标不是重写原文，而是把以上缺口补成“可以直接落表、落配置、落状态机”的设计条目。

## 2. 推荐决策总览

| 主题 | 推荐决策 | 原因 |
| --- | --- | --- |
| PAT | Postgres 为权威源，Redis 仅做缓存/加速，启停仅使用 `auth.pat.mode` | revoke/rotate/audit 需要强一致与持久化，同时避免 PAT 出现双重开关 |
| OIDC | 正式配置使用 `issuer_url + audience`，`jwks_url` 仅保留 override | 当前实现依赖 provider discovery，不能只靠裸 JWKS URL |
| HMAC | 设计独立 key registry；签名使用 canonical request；nonce TTL 与签名窗口一致 | 现有描述不足以支撑轮换、多租户和 query 参数防篡改 |
| Auth 路由 | 按凭证类型判定鉴权路径，而不是盲目从 OIDC 回退到 PAT | 降低歧义，提升审计可解释性 |
| Worker | 心跳与任务 ownership 分开建模；引入 execution lease | “worker 活着”与“trace 仍归它执行”不是一回事 |
| Orphan Trace | 仅在 lease 过期后判 orphan；是否自动重试取决于是否已开始执行 step | 避免对有副作用的 UI 操作盲目重放 |
| WebSocket | 浏览器使用短时 subscription token；默认同源，显式 allowlist 可放宽 | 不建议浏览器直接长期持有 PAT/OIDC Bearer |
| 内部链路安全 | 外部 HTTP/WS 生产强制 TLS；内部 gRPC 分阶段从 TLS 演进到 mTLS | 降低首阶段实施复杂度，同时保留升级空间 |
| 审计 | 固定审计事件模型与最小字段集 | 避免实现后各入口各写各的，难以追溯 |

### 2.1 已确认决策（2026-03-22）

以下决策已在本轮评审中达成一致，可视为后续拆分回正式文档时的默认方向：

- PAT 正式权威源采用 Postgres，Redis 仅做缓存与加速，且仅使用 `auth.pat.mode` 作为启停配置。
- PAT token 格式采用 `mcp_v1_<token_id>_<secret>`。
- OIDC 正式配置采用 `issuer_url + audience`，`jwks_url` 仅保留兼容或 override 语义。
- HMAC 必须引入 key registry，且签名必须覆盖 `canonical_query`。
- 鉴权路径按凭证类型判定，不做 OIDC 和 PAT 的盲目回退。
- Worker 状态机采用 `healthy / degraded / offline / draining`。
- distributed 模式下 trace 在 worker `accepted` 后必须持有 execution lease。
- heartbeat 只负责 worker liveness，不直接代表 trace ownership。
- orphan trace 仅在 lease 过期后判定。
- 已开始 step 的 orphan trace 默认 `failed`，不自动重试。
- worker 的迟到结果必须按 `attempt` 做防覆盖校验。
- 浏览器 WebSocket 使用短时 subscription token，不直接复用长期 PAT/OIDC Bearer。
- `traceId` 仅作为资源标识，不作为订阅授权凭据。
- WebSocket 有 `Origin` 时默认同源，配置 allowlist 时仅允许 allowlist。
- 生产环境外部 HTTP / JSON-RPC / REST / WebSocket 必须启用 TLS。
- 内部 gRPC 采用分阶段安全策略：先 TLS，再演进到 mTLS。
- 审计字段与必审计动作集应固定为统一模型，不由各入口自行定义。

## 3. 鉴权模型补丁

### 3.1 PAT（Personal Access Token）

#### 3.1.1 设计结论

- PAT 的权威数据源定义为 Postgres。
- Redis 仅作为 PAT 的读缓存或撤销状态加速层，不作为权威源。
- PAT 展示格式保持为 `mcp_v1_<token_id>_<secret>`。
- 数据库存储 `token_id` 与 `secret_hash`，不存明文 token。
- 默认只在存在可用 PAT source 时激活 PAT 验证能力。

#### 3.1.2 原因

- PAT 校验是高频读，但 revoke/rotate/audit 是强一致写；这类场景更适合 “Postgres 权威 + Redis 缓存”。
- 如果把 Redis 当权威源，重启、主从切换、数据过期和历史审计都会变得复杂。
- `token_id + secret` 的双段设计便于快速定位记录，同时不需要用慢查询扫描全表 hash。

#### 3.1.3 建议表结构

建议新增表 `pat_tokens`：

| 字段 | 说明 |
| --- | --- |
| `token_id` | 主键，随机不可预测字符串 |
| `tenant_id` | 归属租户 |
| `subject_id` | 归属用户/系统主体 |
| `display_name` | token 展示名 |
| `secret_hash` | token secret 的哈希值 |
| `status` | `active` / `rolling` / `revoked` |
| `scopes_json` | 权限范围 |
| `expires_at` | 过期时间，可空 |
| `created_at` | 创建时间 |
| `created_by` | 创建者 |
| `revoked_at` | 吊销时间，可空 |
| `revoked_by` | 吊销者，可空 |
| `rotated_from` | 上一代 token_id，可空 |
| `last_used_at` | 最近使用时间，可空 |
| `last_used_ip` | 最近来源 IP，可空 |
| `metadata_json` | 预留扩展字段 |

#### 3.1.4 行为规则

- 创建 PAT 时只返回一次完整明文 token。
- 校验 PAT 时先按 `token_id` 查询，再比对 `secret_hash`。
- `status = revoked` 的 token 一律拒绝。
- `expires_at` 已过期的 token 视为运行时失效，不再单独持久化 `expired` 状态。
- `active` 与 `rolling` 状态允许验证，但 `rolling` 必须有明确窗口。
- `last_used_at` 与 `last_used_ip` 允许异步刷新，不要求阻塞主请求路径。

#### 3.1.5 PAT 模式建议

建议将 PAT source 显式配置为以下模式之一：

- `disabled`：完全关闭 PAT 鉴权。
- `static`：仅用于开发、自举或离线验证，不作为正式生产模式。
- `postgres`：正式生产模式，使用 `pat_tokens` 作为权威源。

说明：

- 正式设计中，PAT 启停与数据源选择仅由 `auth.pat.mode` 决定。
- 只有在存在明确 PAT source 时，PAT 才应被网关视为可用鉴权方式。

### 3.2 OIDC

#### 3.2.1 设计结论

- 正式配置字段定义为：
  - `issuer_url`
  - `audience`
  - `jwks_url_override`（可选）
  - `clock_skew`
  - `required_claims`
- `jwks_url` 不再作为主配置字段，仅保留兼容语义。

#### 3.2.2 原因

- OIDC 的语义核心不是“能拿到公钥”，而是“能够验证 issuer、audience、token 生命周期和 claims”。
- 如果直接以裸 JWKS URL 为主配置，后续 provider metadata、issuer 语义和多 provider 兼容都会变差。
- 当前实现天然偏向 `issuer_url` 语义，因此设计应主动对齐实现依赖，而不是让实现继续绕过设计。

#### 3.2.3 校验规则

- 必须验证 `iss`。
- 必须验证 `aud`。
- 必须验证 `exp` / `nbf` / `iat` 的时间语义。
- 必须支持有限时钟漂移。
- 可以通过 `required_claims` 额外绑定租户、项目或角色。

### 3.3 HMAC

#### 3.3.1 设计结论

- HMAC 需要独立的 key registry。
- HMAC 签名串改为 canonical request，不只签 path。
- nonce TTL 与签名时间窗绑定，不使用 24 小时级别的超长 TTL。

#### 3.3.2 原因

- 仅有 shared secret 而无 key registry，无法支撑 key rotation、多租户隔离与密钥治理。
- 如果 query 参数未被签名，调用方和服务端对“请求到底被保护了什么”会产生偏差。
- nonce TTL 过长会增加 Redis 成本，也会模糊与签名时间窗之间的语义关系。

#### 3.3.3 建议表结构

建议新增表 `hmac_keys`：

| 字段 | 说明 |
| --- | --- |
| `key_id` | 主键 |
| `tenant_id` | 租户归属 |
| `display_name` | 密钥展示名 |
| `secret_ref` | 指向 KMS/Secret Manager 的引用，或加密后的 secret |
| `status` | `active` / `rolling` / `retired` / `revoked` |
| `not_before` | 生效时间 |
| `not_after` | 停止接受时间 |
| `created_at` | 创建时间 |
| `created_by` | 创建者 |
| `revoked_at` | 吊销时间，可空 |
| `metadata_json` | 预留扩展字段 |

说明：

- `status` 表示运营生命周期状态。
- `not_before` / `not_after` 表示时间窗口约束。
- 运行时是否可验签，应由 `status + 时间窗口` 联合计算，不再额外持久化时间推导状态。

#### 3.3.4 canonical request 建议

建议签名原文使用如下结构：

```text
v1
<HTTP_METHOD>
<CANONICAL_PATH>
<CANONICAL_QUERY>
<BODY_SHA256>
<TIMESTAMP>
<NONCE>
<KEY_ID>
```

说明：

- `CANONICAL_QUERY` 必须排序并稳定编码。
- `BODY_SHA256` 对空 body 也必须有固定值。
- `TIMESTAMP` 与 `NONCE` 必须参与签名。
- `KEY_ID` 必须参与签名，避免请求在不同 key 间被错误复用。

#### 3.3.5 HMAC 行为规则

- 签名窗口建议为 `±5m`，nonce TTL 建议为 `10m-15m`。
- Redis nonce key 建议命名为 `nonce:{tenant_id}:{key_id}:{nonce}`。
- `rolling` 状态允许 old/new key 共存，但必须有明确截止时间。
- `revoked` key 立即拒绝。

#### 3.3.6 HMAC key 状态语义

- `active`：允许签发与验签。
- `rolling`：允许旧新 key 并行验签，但新请求应优先使用新 key。
- `retired`：不再允许签发新请求，但可在短窗口内用于验证历史或重放请求。
- `revoked`：立即拒绝。

### 3.4 鉴权路径选择规则

#### 3.4.1 设计结论

- 有完整 HMAC 头时，只走 HMAC。
- 有 `Authorization: Bearer` 且 token 形态为 JWT 时，只走 OIDC。
- 有 `Authorization: Bearer mcp_v1_...` 时，只走 PAT。
- 一个请求同时携带两套完整凭证时，默认拒绝。

#### 3.4.2 原因

- “HMAC→OIDC→PAT” 更适合表达优先级，而不是表达“可以盲目尝试所有路径”。
- 按 token 形态判定路径，能明显提升审计与错误排查的确定性。
- 多套凭证并存时，如果不拒绝，后续很难解释“到底是谁授权的这次调用”。

#### 3.4.3 建议判定顺序

建议以如下顺序判定请求身份路径：

1. 如果请求携带完整 HMAC 头，则只走 HMAC。
2. 如果请求携带 `Authorization: Bearer <token>` 且 token 形态为 JWT，则只走 OIDC。
3. 如果请求携带 `Authorization: Bearer mcp_v1_...`，则只走 PAT。
4. 如果请求同时携带多套完整凭证，则返回鉴权冲突错误并拒绝请求。

## 4. Worker 心跳与 Execution Lease 补丁

### 4.1 设计结论

- Worker liveness 与 trace ownership 分开建模。
- 心跳只负责说明 worker 是否可用。
- execution lease 才负责说明某个 trace 当前归哪个 worker 执行。

### 4.2 Worker 状态机

定义 worker 运行时状态：

- `healthy`：最近心跳稳定，可正常分配新任务。
- `degraded`：近期存在心跳丢失或错误，但尚未完全摘除，应降低权重。
- `offline`：连续超出阈值未恢复，不得分配新任务。
- `draining`：人为进入排空状态，不接新任务，但保留当前 in-flight。

#### 原因

- `healthy/offline` 二元状态太粗，无法表达“还能活，但不适合继续吃流量”的中间态。
- `draining` 是运维场景刚需，否则滚动升级、节点维护都会变得粗暴。

### 4.3 Heartbeat 规则

- 心跳周期保持 10 秒。
- 缺失 1 次允许保留 `healthy`，但应累积 miss 计数。
- 缺失 2 次进入 `degraded`。
- 缺失达到阈值后进入 `offline`。
- 从 `offline` 恢复后，不直接回 `healthy`，先进入 `degraded`。
- 连续稳定一个观察窗口后，再回到 `healthy`。

#### 原因

- 这样可以减少 worker 状态来回抖动。
- “恢复后 60 秒内重新纳入”这类文字描述，应落成明确状态迁移，而不是散落在实现里。

#### 4.3.1 建议默认值

- `heartbeat_interval = 10s`
- `degraded_after_misses = 2`
- `offline_after_misses = 3`
- `recovery_stabilization_window = 60s`

### 4.4 Execution Lease

建议定义运行时对象 `execution_lease`：

| 字段 | 说明 |
| --- | --- |
| `trace_id` | 对应 trace |
| `worker_id` | 当前持有 lease 的 worker |
| `attempt` | 第几次执行尝试 |
| `status` | `leased` / `expired` / `released` |
| `lease_expires_at` | lease 过期时间 |
| `last_renewed_at` | 最近续约时间 |
| `created_at` | 创建时间 |

行为规则：

- worker 在 `accepted` 任务后获得 lease。
- worker 必须周期性续约 lease。
- worker 正常完成后释放 lease。
- orchestrator 仅在 lease 过期且无终态结果时，判定该 trace 为 orphan。

补充规则：

- lease 必须绑定 `attempt`，以区分当前执行尝试与旧 worker 的迟到结果。
- lease TTL 建议由 `heartbeat_interval` 与 `grace_period` 推导，而不是再提供独立 TTL 开关。
- 推荐公式：`lease_ttl = 3 * heartbeat_interval + grace_period`。

#### 原因

- 当前仅靠心跳无法判断“worker 活着但 trace 卡死”与“worker 已死但 trace 仍显示 running”的区别。
- 有 lease 后，dispatcher、watchdog、cancel、retry 的边界都会更清晰。

### 4.5 Orphan Trace 处理规则

- 如果 trace 还未开始任何 step，可自动重试。
- 如果已经出现 step 级执行迹象，默认进入 `failed`，错误原因标记为 `orphaned`。
- 只有显式标记为可重放/幂等的计划，才允许在 step 已开始后自动重试。

#### 原因

- UI 自动化步骤通常带副作用，自动重试可能造成重复点击、重复提交、重复购买。
- “丢心跳就自动重试”在移动场景里是危险默认值。

#### 4.5.1 迟到结果处理

- worker 恢复后上报的旧结果必须携带原始 `attempt`。
- orchestrator 仅接受与当前 active lease attempt 匹配的结果。
- 如果 trace 已进入新 attempt 或终态，旧结果只能写审计或日志，不得覆盖当前状态。

## 5. WebSocket 安全补丁

### 5.1 设计结论

- 浏览器不直接使用长期 PAT/OIDC Bearer 建立 WebSocket。
- 浏览器先通过受保护接口换取短时 subscription token。
- WebSocket 握手默认同源，允许通过 allowlist 显式放宽。

### 5.2 Subscription Token

建议 subscription token 至少包含：

- `sub`
- `tenant_id`
- `trace_id`
- `exp`
- `jti`
- `scope`

行为规则：

- token 有效期短，建议 60 秒。
- token 仅允许订阅指定 trace。
- token 建议短时重复可用，以兼容浏览器重连。
- `traceId` 仅作为资源标识，不作为授权凭据。
- 服务端应在签发 token 前校验调用方是否有权访问对应 trace。

#### 原因

- 浏览器长期持有 PAT/OIDC Bearer 风险过高。
- 浏览器对自定义握手头的控制能力有限，短期 token 更适合前端与重连场景。
- trace 级绑定比“登录了就能随便订阅”更容易满足最小权限原则。

#### 5.2.1 推荐握手流程

建议浏览器端采用如下流程：

1. 客户端先通过受保护的 HTTP 接口申请订阅，例如 `POST /api/v1/traces/{traceId}:subscribe`。
2. 服务端校验请求主体是否有权访问目标 trace。
3. 服务端签发短时 subscription token。
4. 客户端使用该 token 建立 WebSocket 连接。

说明：

- 浏览器端不建议直接携带长期 PAT 或 OIDC Bearer 发起 WebSocket 握手。
- subscription token 建议只授予 `trace.events.read` 这一类最小权限。

### 5.3 Origin 策略

- 有 `Origin` 时，默认只允许同源。
- 配置了 allowlist 时，仅允许 allowlist。
- 无 `Origin` 的非浏览器客户端可放行。

#### 原因

- 这与浏览器安全模型天然对齐。
- 能同时兼容 CLI、服务端订阅者和浏览器前端。

#### 5.3.1 token 传递建议

- 浏览器客户端在 MVP 阶段统一使用 query parameter 携带短时 subscription token。
- 如果使用 query parameter，日志与错误响应中不得记录完整 token 明文。
- 非浏览器客户端的额外传递方式不纳入本轮正式设计范围，后续如需扩展再单独评审。

## 6. TLS 与内部链路安全补丁

### 6.1 设计结论

- 外部 HTTP/WS：生产模式必须 TLS。
- 内部 gRPC：分阶段落地。
  - 第一阶段：server TLS + client token。
  - 第二阶段：mTLS。

#### 原因

- 当前工程距离完整 mTLS 证书体系仍有距离，分阶段更可执行。
- 先把“禁止明文暴露入口”做好，收益最高。

#### 6.1.1 环境分层建议

- `dev`：允许明文，但必须显式配置，不应默认隐式启用。
- `staging`：外部入口必须 TLS，内部链路建议 TLS。
- `prod`：外部入口必须 TLS，内部链路至少 TLS，并以 mTLS 为目标态。

### 6.2 健康检查扩展

建议把以下安全项纳入健康检查：

- 证书剩余有效期
- 外部入口是否启用 TLS
- 内部 gRPC 是否仍在 insecure fallback
- 最近 auth failure rate

#### 原因

- 安全状态会直接影响“是否适合上线”，不应只存在于配置层。

#### 6.2.1 安全健康项建议

建议在 `healthCheck` 或 `/healthz` 中至少暴露以下安全维度：

- `external_tls_enabled`
- `rpc_security_mode`
- `certificate_days_remaining`
- `auth_failure_rate`
- `websocket_origin_policy_mode`

## 7. 审计模型补丁

### 7.1 最小审计字段

建议统一审计字段：

- `actor_id`
- `actor_type`
- `auth_scheme`
- `credential_id`
- `tenant_id`
- `resource_type`
- `resource_id`
- `action`
- `result`
- `reason`
- `source_ip`
- `request_id`
- `created_at`
- `metadata_json`

### 7.2 必须审计的动作

- auth success
- auth failure
- PAT create / revoke / rotate
- HMAC key create / revoke / rotate
- OIDC provider config change
- worker register / offline / recover / drain
- execution lease expired
- WebSocket subscription create / deny
- WebSocket subscription expire
- orphan trace finalized

#### 原因

- 如果不先定义审计事件集，后面每个入口会各写各的，最终很难汇总与追责。

#### 7.2.1 credential_id 语义建议

- PAT：记录 `token_id`
- HMAC：记录 `key_id`
- OIDC：记录稳定 subject 标识，例如 `issuer + sub`
- subscription token：记录 `jti`

## 8. 配置补丁建议

建议新增或修正配置项：

| 配置项 | 说明 |
| --- | --- |
| `auth.pat.mode` | `disabled` / `static` / `postgres` |
| `auth.pat.cache_ttl` | PAT Redis cache TTL |
| `auth.oidc.issuer_url` | OIDC issuer |
| `auth.oidc.audience` | OIDC audience |
| `auth.oidc.jwks_url_override` | 可选 override |
| `auth.hmac.nonce_ttl` | nonce TTL |
| `auth.hmac.window` | 验签窗口 |
| `gateway.websocket.allowed_origins` | WS browser allowlist |
| `gateway.websocket.subscription_token_ttl` | WS subscription token TTL |
| `orchestrator.worker.degraded_after_misses` | 进入 degraded 的阈值 |
| `orchestrator.worker.offline_after_misses` | 进入 offline 的阈值 |
| `orchestrator.worker.recovery_stabilization_window` | 恢复后重新纳入 healthy 前的稳定观察窗口 |
| `orchestrator.execution_lease.grace_period` | orphan 判定前宽限期 |
| `gateway.tls.required_in_production` | 生产环境是否强制外部 TLS |
| `rpc.security.mode` | `insecure` / `tls` / `mtls` |

## 9. 推荐实施顺序

1. 先拍板 PAT / OIDC / HMAC 的正式配置模型与权威数据源。
2. 再拍板 worker 状态机与 execution lease。
3. 再定义 WebSocket subscription token 与 origin 策略。
4. 最后补审计事件、健康检查与 TLS 分阶段目标。

## 10. 本草案与现有文档的关系

- 本文是对 [requirements.v4.3.md](/Users/stellajen/Documents/workspace/mcp_for_appium/requirements.v4.3.md) 与 [module_v4.3_design.md](/Users/stellajen/Documents/workspace/mcp_for_appium/module_v4.3_design.md) 的设计补丁。
- 本文不替代原需求与设计文档，而是为它们补齐当前实现阶段暴露出的关键缺口。
- 经评审确认后，可将本文内容拆分回原文档的 “安全”、“并发与调度”、“健康检查”、“数据层” 四个章节。

## 11. 待拍板清单（评审会版）

本节用于把全文压缩成评审会可逐项确认的议题列表。建议按 `P0 -> P1 -> P2` 的顺序评审，避免先讨论实现细节再回头改顶层契约。

### 11.1 P0 必须拍板

| 议题 | 当前建议 | 不拍板的风险 | 评审输出 |
| --- | --- | --- | --- |
| PAT 权威源 | `postgres` | PAT 无法形成正式 revoke / rotate / audit 闭环 | 确认 PAT source 模式与 `pat_tokens` 是否立项 |
| OIDC 正式配置 | `issuer_url + audience` | 文档、配置、实现语义继续错位 | 确认配置字段和兼容策略 |
| HMAC key 模型 | `hmac_keys + canonical request + query 签名` | 多租户、轮换、query 防篡改都无法闭环 | 确认 key registry 是否进入 Module I |
| 鉴权路径选择 | 按凭证类型判定，不盲回退 | 同一请求身份归因混乱，审计难解释 | 确认冲突请求是否默认拒绝 |
| Worker 状态机 | `healthy / degraded / offline / draining` | 调度与健康检查继续靠经验实现 | 确认状态集与恢复窗口 |
| Execution Lease | distributed 模式必须引入 | trace ownership 仍不明确，orphan 规则不稳定 | 确认 lease 字段与 TTL 策略 |
| Orphan trace 策略 | step 未开始可重试，已开始默认失败 | 容易出现副作用重放 | 确认默认 retry 边界 |
| 浏览器 WS 身份模型 | subscription token | 长期凭证暴露给前端，授权边界不清 | 确认是否新增订阅 token 接口 |

### 11.2 P1 应尽快拍板

| 议题 | 当前建议 | 评审输出 |
| --- | --- | --- |
| WebSocket token TTL | `60s` | 确认 TTL 与 query 传递方式是否满足浏览器接入 |
| HMAC nonce TTL | `10m-15m` | 确认与签名窗口的关系 |
| Worker 恢复窗口 | `60s` | 确认 `recovery_stabilization_window` |
| Lease TTL 公式 | `3 * heartbeat_interval + grace_period` | 确认是否采用公式化策略 |
| 迟到结果处理 | 仅接受匹配当前 attempt 的结果 | 确认 discard / audit 策略 |
| 外部 TLS 强制策略 | 生产强制 | 确认 dev / staging / prod 环境差异 |
| 内部 RPC 安全阶段 | `tls -> mtls` | 确认第一阶段是否允许 token + TLS |
| 审计最小字段 | 采用第 7 节字段集 | 确认字段是否需要增减 |

### 11.3 P2 可后续细化

| 议题 | 当前建议 | 评审输出 |
| --- | --- | --- |
| OIDC `required_claims` 模型 | map 形式扩展 | 确认是否需要 tenant/project 级 claim 绑定 |
| HMAC `secret_ref` 形态 | KMS/Secret Manager 优先 | 确认首阶段是否允许 DB 加密存储 |
| subscription token 传递方式 | MVP 固定为 query | 确认是否接受后续再评审第二种传递方式 |
| 审计写入方式 | 主链路关键动作同步写，次要动作可异步 | 确认性能与合规权衡 |
| PAT `last_used` 刷新 | 异步更新 | 确认是否允许延迟可见 |

### 11.4 建议评审顺序

1. 先确认身份模型：PAT / OIDC / HMAC。
2. 再确认 distributed 执行边界：worker 状态机 / lease / orphan。
3. 再确认浏览器与 WebSocket 安全模型。
4. 最后确认 TLS 分阶段目标与审计范围。

## 12. 后续实现任务映射

本节把设计决策映射为可执行的实现任务，便于后续拆 issue、排期和验收。

### 12.1 文档与配置任务

| 任务 ID | 任务 | 主要改动 | 依赖 | 验收点 |
| --- | --- | --- | --- | --- |
| D1 | 同步需求文档 | 更新 [requirements.v4.3.md](/Users/stellajen/Documents/workspace/mcp_for_appium/requirements.v4.3.md) 的安全、并发与健康章节 | 本草案确认 | 正式需求文档出现 PAT source、lease、subscription token 等条目 |
| D2 | 同步设计文档 | 更新 [module_v4.3_design.md](/Users/stellajen/Documents/workspace/mcp_for_appium/module_v4.3_design.md) 的组件职责、状态机与数据层章节 | D1 | 设计文档明确 worker 状态机与 auth 路由 |
| D3 | 收敛配置文档 | 更新 [README.md](/Users/stellajen/Documents/workspace/mcp_for_appium/README.md) 与 `config.example.yaml` | D1 | 新配置项有示例与解释 |

### 12.2 数据模型与迁移任务

| 任务 ID | 任务 | 主要改动 | 依赖 | 验收点 |
| --- | --- | --- | --- | --- |
| M1 | 新增 `pat_tokens` 表 | Postgres migration + DAO 接口 | D1 | PAT 表结构、索引、状态字段齐全 |
| M2 | 新增 `hmac_keys` 表 | Postgres migration + DAO 接口 | D1 | key registry 可表达 active/rolling/retired/revoked |
| M3 | 新增 execution lease 持久化模型 | Postgres 或 Redis 权威模型 + DAO/Store 接口 | D2 | trace ownership 可被持久化与查询 |
| M4 | 扩展 audit model | `audit_logs` 字段或 payload 结构调整 | D1 | 审计字段能表达 actor/auth/resource/result |

### 12.3 Gateway / Auth 任务

| 任务 ID | 任务 | 主要改动 | 依赖 | 验收点 |
| --- | --- | --- | --- | --- |
| G1 | PAT 正式接线 | 网关 auth middleware + PAT lookup + cache | M1 | Bearer PAT 可在主程序中完成认证 |
| G2 | OIDC 配置收敛 | `issuer_url + audience` 配置与 validator 初始化 | D1 | OIDC 配置语义与实现一致 |
| G3 | HMAC 正式接线 | canonical request、key registry、nonce 维度收敛 | M2 | REST/JSON-RPC HMAC 验签覆盖 query |
| G4 | 鉴权冲突处理 | 明确多凭证冲突错误码与响应 | G1/G2/G3 | 多凭证请求被稳定拒绝 |
| G5 | WebSocket origin 策略 | allowlist / same-origin / no-origin 规则 | D2 | 浏览器 origin 行为与设计一致 |
| G6 | subscription token 接口 | `POST ...:subscribe` + token 生成与校验 | D2 | 浏览器可通过短时 token 订阅 trace |

### 12.4 Orchestrator / Worker 任务

| 任务 ID | 任务 | 主要改动 | 依赖 | 验收点 |
| --- | --- | --- | --- | --- |
| O1 | Worker 状态机落地 | registry 状态字段、迁移规则、恢复窗口 | D2 | `healthy/degraded/offline/draining` 可观测可测试 |
| O2 | Execution lease 落地 | dispatcher、worker 接受、续约、释放 | M3 | accepted trace 都会生成 lease |
| O3 | Orphan trace 收口 | watchdog、终态事件、attempt 校验 | O2 | orphan trace 不再无限挂起 |
| O4 | 迟到结果防覆盖 | result 回传校验 current attempt | O2 | 旧 worker 结果无法覆盖新 attempt |
| O5 | draining 行为落地 | worker 下线/升级时停止接新任务 | O1 | draining worker 不再接新 trace |

### 12.5 TLS / 健康检查 / 审计任务

| 任务 ID | 任务 | 主要改动 | 依赖 | 验收点 |
| --- | --- | --- | --- | --- |
| S1 | 外部 TLS 强制策略 | gateway 生产模式 TLS 校验与启动策略 | D1 | 生产配置下明文启动被拒绝 |
| S2 | 内部 RPC 安全分阶段 | gRPC TLS 配置与后续 mTLS 抽象 | D2 | internal insecure 有显式配置与健康暴露 |
| S3 | 安全健康项扩展 | `/healthz` 与 `healthCheck` 增加 TLS / auth / worker 风险项 | O1/S1/S2 | 健康输出可见安全状态 |
| S4 | 审计统一落地 | auth success/failure、lease expired、WS 订阅等统一写审计 | M4 | 审计事件集与设计一致 |

### 12.6 测试与验收任务

| 任务 ID | 任务 | 主要改动 | 依赖 | 验收点 |
| --- | --- | --- | --- | --- |
| T1 | PAT / OIDC / HMAC 认证测试 | 单测 + 集成测试 | G1/G2/G3 | 三种认证路径均有成功/失败用例 |
| T2 | 鉴权冲突测试 | 多凭证冲突 case | G4 | 冲突请求稳定返回预期错误 |
| T3 | Worker 状态机测试 | miss / recover / drain 迁移测试 | O1 | 状态迁移满足设计规则 |
| T4 | Lease / orphan 测试 | accepted-no-result、late-result、retry 边界 | O2/O3/O4 | distributed trace 可稳定收口 |
| T5 | WebSocket 安全测试 | origin / subscription token / unauthorized trace | G5/G6 | 浏览器和非浏览器行为都可验证 |
| T6 | TLS / health / audit 测试 | 生产 TLS 拒绝明文、健康字段、审计事件 | S1/S3/S4 | 安全与观测行为可自动回归 |

## 13. 会议输出模板

建议评审会结束时至少形成以下结论，避免“讨论过但没有定稿”：

| 项目 | 输出内容 |
| --- | --- |
| 已确认决策 | 哪些条目确认采用本文建议 |
| 保留问题 | 哪些条目需要二次评审或另开专题 |
| 不采用项 | 哪些建议被否决，以及替代方案是什么 |
| 首批实现范围 | 本轮迭代先落哪几项 |
| 非目标项 | 哪些内容明确不在本轮实现范围内 |
| 文档责任人 | 谁负责把草案拆回正式需求与设计文档 |
