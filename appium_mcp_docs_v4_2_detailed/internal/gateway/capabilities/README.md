# internal/gateway/capabilities

## service.go

### NewService(cfg) / List(ctx) / featureFlagsFromConfig
**Purpose**：对外声明当前节点能力。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 汇总开关与版本，生成 Capabilities JSON。
    - 为 JSON-RPC `describeCapabilities` 与 REST `/capabilities` 复用统一生成逻辑。

**Errors & Edge Cases**
    - 缺少必要字段 -> 500。

**Acceptance**
    - 字段包含 apiVersion/planOps/features/extensions/deprecated.sunsetAt；与示例一致。
    - 调用方通过 JSON schema 校验通过，且 JSON-RPC 返回值与 REST 一致。
