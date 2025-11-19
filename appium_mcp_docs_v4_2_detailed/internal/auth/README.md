# internal/auth

## pat.go

### NewPATValidator(HashLookup) / Validate(token)
**Purpose**：校验 Personal Access Token。

**Logic — External Calls**
    - HashLookup.Lookup(tokenID)

**Logic — Local Implementation**
    - 拆解 token（id+签名），对比哈希并检查过期/撤销标记。
    - 审计：写入 subject、ip、user-agent。

**Errors & Edge Cases**
    - 哈希不匹配 -> 401；已撤销/过期 -> 403。

**Acceptance**
    - 伪造/过期 token 均拒绝；审计日志可检索。


## oidc.go

### NewOIDCValidator(jwksURL, audience) / Validate(jwt) / refreshJWKS()
**Purpose**：基于 JWKS 校验 OIDC JWT。

**Logic — External Calls**
    - 抓取 JWKS；按 kid 选择公钥；检查 aud/iss/exp/nbf。

**Logic — Local Implementation**
    - 缓存 JWKS，定期刷新；允许 ±skew 的时钟偏移。

**Errors & Edge Cases**
    - JWKS 不可达 -> 使用缓存；完全无缓存 -> 503。

**Acceptance**
    - aud/iss 严格匹配；时钟偏移容差内通过。


## hmac.go

### Verify(sig, payload, ts, nonce) / computeHMAC(secret, msg)
**Purpose**：校验请求签名与重放保护。

**Logic — External Calls**
    - Redis: SETNX nonce + EX
    - HMAC-SHA256 比较

**Logic — Local Implementation**
    - 构造消息串：method + path + ts + bodyhash。常量时比较避免时序攻击。

**Errors & Edge Cases**
    - 重复 nonce -> 409；过期 ts -> 401。

**Acceptance**
    - ±5 分钟窗口内有效；nonce 仅一次；失败均有标准化错误码。


## context.go

### WithSubject/SubjectFrom
**Purpose**：在 ctx 中携带主体信息。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 使用自定义 ctx key；跨协议透传。

**Errors & Edge Cases**
    - 空 subject -> 匿名/受限路由。

**Acceptance**
    - 在日志/trace 中均可见 subject 字段。
