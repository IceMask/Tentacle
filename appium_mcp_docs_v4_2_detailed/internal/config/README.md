# internal/config

## loader.go


### Load()/MustLoad()
**Purpose**：装载配置为 Config 结构体。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 读取多源（文件/ENV），并做类型解析与默认值填充。
    - MustLoad 在错误时 panic/exit。

**Errors & Edge Cases**
    - JSON/YAML 解析失败 -> E.CONFIG.PARSE

**Acceptance**
    - 全部字段可被 ENV 覆盖；默认值表有单测。



### applyEnvOverrides(*Config)
**Purpose**：按命名约定将 ENV 覆盖到 Config。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 支持嵌套字段通过前缀展开，如 GATEWAY__PORT。
    - 布尔/数值/时间统一解析。

**Errors & Edge Cases**
    - ENV 值非法 -> E.CONFIG.ENV.INVALID

**Acceptance**
    - ENV 覆盖后再次校验通过。



### parseDurations(*Config)
**Purpose**：解析所有 duration 字段。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 统一 time.ParseDuration 并校验上/下界。

**Errors & Edge Cases**
    - 小于 0 或超上限 -> E.CONFIG.DURATION

**Acceptance**
    - 边界值用例通过；单位支持 ms/s/m/h。



### validate(*Config)
**Purpose**：必填、范围与互斥项校验。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - 对互斥项（如 OIDC 与 PAT-only）做布尔代数检查。
    - 对端口/URL/枚举进行白名单校验。

**Errors & Edge Cases**
    - 冲突 -> E.CONFIG.CONFLICT

**Acceptance**
    - 覆盖 100% 互斥组合的表驱动测试。
