# internal/worker/appium

## client.go

### NewClient/NewSession/DeleteSession
**Purpose**：管理 Appium 会话生命周期。

**Logic — External Calls**
    - HTTP 调用 /session 创建/删除

**Logic — Local Implementation**
    - 设置默认 headers/超时/keep-alive。

**Errors & Edge Cases**
    - 创建失败 -> 重试 3 次；仍失败返回上层。

**Acceptance**
    - 会话泄漏监控；删除成功率≥99.9%。



### FindElement/Click/SendKeys/Screenshot/PageSource
**Purpose**：元素查找与交互。

**Logic — External Calls**
    - HTTP 调用 Appium API

**Logic — Local Implementation**
    - 统一 do() 实现方法/路径/错误映射。SendKeys 支持 secure。

**Errors & Edge Cases**
    - 元素不存在 -> E.APP.ELEM_NOT_FOUND

**Acceptance**
    - 错误码映射准确；SendKeys.isSecure 不落日志。



### do(ctx, method, path, body, v)
**Purpose**：统一 HTTP 访问与错误处理。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 实现重试策略：仅 ENETRESET/5xx；指数退避与上限。
    - 将 Appium 错误翻译成内部码。

**Errors & Edge Cases**
    - 超时 -> E.APP.TIMEOUT；连接复用失败 -> 重建。

**Acceptance**
    - 连接复用有效；重试次数符合策略。
