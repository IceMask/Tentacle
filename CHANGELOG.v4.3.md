# Changelog - v4.3

## 版本信息

- **版本号**: v4.3
- **发布日期**: 2026-01-21
- **基于版本**: v4.2

## 新增特性

### 🎯 标准 MCP 协议支持

实现了 Anthropic Model Context Protocol（MCP）标准协议，使平台能够被 Claude Desktop 等 AI 工具直接发现和调用。

#### 1. stdio 传输模式

**新增文件**:
- `internal/gateway/stdio/transport.go` - stdio 传输层实现

**功能**:
- Gateway 支持 `--stdio` 命令行参数
- 通过标准输入/输出进行 JSON-RPC 通信
- 符合 MCP 协议版本 2024-11-05
- 日志输出到 stderr，stdout 专用于 JSON-RPC 响应

**使用示例**:
```bash
./gateway --stdio --config config.yaml
```

#### 2. MCP 元协议实现

**新增文件**:
- `internal/gateway/mcp/handler.go` - MCP 协议处理器
- `internal/gateway/mcp/tools.go` - MCP Tools 注册表
- `internal/gateway/mcp/mcp_test.go` - 完整测试套件

**实现的 MCP 方法**:
- `initialize` - MCP 握手，返回服务器能力
- `tools/list` - 列出所有可用工具
- `tools/call` - 调用指定工具
- `resources/list` - 列出可访问资源
- `resources/read` - 读取资源内容

#### 3. MCP Tools 注册表

**注册的工具** (8个):

| Tool 名称 | 描述 | 必填参数 |
|----------|------|---------|
| `startSession` | 创建移动设备测试会话 | projectId, w3cCapsJson |
| `executePlan` | 执行自动化测试计划 | sessionId, plan |
| `endSession` | 关闭会话释放资源 | sessionId |
| `getSemanticSnapshot` | 获取 UI 元素树快照 | sessionId |
| `takeScreenshot` | 对设备屏幕截图 | sessionId |
| `cancelPlan` | 取消正在执行的计划 | traceId |
| `getTrace` | 获取执行记录详情 | traceId |
| `healthCheck` | 检查服务健康状态 | 无 |

**特性**:
- 每个工具包含完整的 JSON Schema 定义
- 支持参数验证（required fields, types）
- 结果自动包装为 MCP 标准格式

#### 4. Claude Desktop 集成

**配置支持**:

在 Claude Desktop 配置文件中添加：

```json
{
  "mcpServers": {
    "appium-mobile-testing": {
      "command": "/path/to/gateway",
      "args": ["--stdio"],
      "env": {
        "CONFIG_PATH": "/path/to/config.yaml"
      }
    }
  }
}
```

配置文件位置：
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

### 🔧 架构改进

#### JSON-RPC Handler 重构

**修改文件**:
- `internal/gateway/jsonrpc/handler.go`

**改进**:
- 提取核心处理逻辑到 `ProcessRequest()` 方法
- 支持 HTTP 和 stdio 两种传输模式共享业务逻辑
- 统一错误处理（MCP errors + 传统 errors）
- 添加 `context.Context` 支持

#### Gateway 启动模式

**修改文件**:
- `cmd/gateway/main.go`

**新增**:
- `--stdio` flag 解析
- `runStdioMode()` 函数 - stdio 模式入口
- `runHTTPMode()` 函数 - HTTP 模式入口（重构原有逻辑）
- 优雅关闭支持（signal handling）

## 向后兼容性

✅ **完全向后兼容** - 所有现有接口保持不变：

1. **HTTP JSON-RPC** - `POST /jsonrpc` 端点继续工作
2. **REST API** - `/api/v1/*` 端点不变
3. **WebSocket** - `/ws/plan-events` 不变
4. **gRPC** - 所有 gRPC 服务不变
5. **传统方法调用** - 可直接调用 `startSession`, `executePlan` 等方法（不通过 MCP tools/call）

## 测试

### 新增测试

**测试文件**:
- `internal/gateway/mcp/mcp_test.go`

**测试覆盖**:
- ✅ MCP 协议流程测试（6 个子测试）
- ✅ Tool Registry 测试（4 个子测试）
- ✅ MCP Handler 测试（3 个子测试）
- ✅ 参数验证测试
- ✅ 错误处理测试

**测试结果**:
```
=== RUN   TestMCPIntegration
=== RUN   TestToolRegistry
=== RUN   TestMCPHandler
PASS
ok      mcp_for_appium/internal/gateway/mcp    0.466s
```

## 文档更新

### 新增文档

1. **requirements.v4.3.md** - 更新需求说明
   - 添加 "2.1.1 标准 MCP 协议支持" 章节
   - 详细说明 stdio 模式、MCP 元协议、Tools 注册表
   - Claude Desktop 集成配置示例

2. **module_v4.3_design.md** - 更新设计文档
   - 架构图更新（包含 stdio 传输层）

3. **appium_mcp_docs_v4_3_detailed/** - 完整详细文档
   - 从 v4.2 复制并更新版本号

4. **README.md** - 更新主说明
   - 添加 v4.3 新特性说明
   - 快速开始指南（MCP Server 使用 + HTTP 使用）
   - MCP Tools 列表

5. **CHANGELOG.v4.3.md** - 本文件
   - 完整变更记录

## 依赖变更

无新增外部依赖 - 使用 Go 标准库实现。

## 迁移指南

### 从 v4.2 迁移到 v4.3

**无需任何更改** - v4.3 完全向后兼容 v4.2。

如需启用 MCP 功能：

1. 重新编译 gateway：
   ```bash
   go build ./cmd/gateway
   ```

2. 选择运行模式：
   - **stdio 模式**（用于 Claude Desktop）：
     ```bash
     ./gateway --stdio --config config.yaml
     ```

   - **HTTP 模式**（默认，用于传统客户端）：
     ```bash
     ./gateway --config config.yaml
     ```

3. （可选）在 Claude Desktop 中配置 MCP Server

## 已知限制

1. **stdio 模式数据库依赖** - stdio 模式仍需要完整的数据库/Redis/S3 配置
2. **资源读取未实现** - `resources/read` 方法返回占位数据，待完整实现
3. **部分业务方法待实现** - `endSession`, `getSemanticSnapshot`, `takeScreenshot` 等方法包含 TODO 标记

## 下一步计划（v4.4）

- [ ] 实现完整的 `resources/read` URI 解析
- [ ] 完善所有业务方法实现（移除 TODO）
- [ ] 添加 stdio 模式的集成测试
- [ ] 性能优化：stdio 传输的消息批处理
- [ ] 支持 SSE（Server-Sent Events）传输模式

## 贡献者

- Claude Sonnet 4.5 - 实现与文档

## 版本历史

- **v4.3** (2026-01-21) - 标准 MCP 协议支持 + stdio 传输模式
- **v4.2** (2025) - 核心执行层完整实现
- **v4.1** (2025) - 基础功能实现
- **v3** (2024) - Module I 基线
