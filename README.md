# MCP for Appium

MCP for Appium is a mobile automation gateway that exposes Appium and AWS Device Farm workflows through standard MCP, HTTP JSON-RPC, WebSocket event streaming, and internal gRPC services.

It is designed for AI-assisted mobile testing, scripted automation, and browser-based monitoring. The repository ships with embedded validation tools, PostgreSQL migrations, and production-oriented configuration examples so the same codebase can be used for local development, CI, and hosted deployments.

## Highlights

- Standard MCP server for `stdio` clients such as Claude Desktop
- HTTP JSON-RPC endpoint for service-to-service integrations
- Session lifecycle, element actions, gestures, screenshots, health checks, and semantic snapshots
- Browser event streaming through short-lived WebSocket subscription tokens
- AWS Device Farm support for both scheduled test runs and live remote-access Appium sessions
- PostgreSQL, Redis, and S3-backed persistence for traces, events, and artifacts

## Quick Start

### Claude Desktop

Add the gateway as an MCP server in your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "mcp-for-appium": {
      "command": "/absolute/path/to/gateway",
      "args": ["--stdio", "--config", "/absolute/path/to/config.yaml"]
    }
  }
}
```

Common configuration file locations:

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\\Claude\\claude_desktop_config.json`

### HTTP JSON-RPC

Start the gateway:

```bash
./gateway --config config.yaml
```

List the available tools:

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/list",
    "params": {},
    "id": 1
  }'
```

### Database Migrations

Apply the shipped PostgreSQL migrations before starting `gateway` or `orchestrator`:

```bash
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/mcp_mobile_worker?sslmode=disable'
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/001_init.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/002_audit_logs.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/003_reserved_slot.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/004_pat_tokens.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/005_hmac_keys.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/006_trace_execution_state.sql
psql "$DATABASE_URL" -f internal/storage/postgres/migrations/007_trace_session_ownership.sql
```

Verify the migration set:

```bash
go run ./cmd/migration_replay_check
```

Run the main integration suite:

```bash
go test ./internal/integration -count=1 -v
```

## Public Interfaces

### MCP `stdio`

The `gateway --stdio` entry point exposes the full MCP tool catalog and is the simplest way to connect the project to AI tools and desktop clients.

### HTTP JSON-RPC

The `/jsonrpc` endpoint provides the same operational surface for service integrations that prefer HTTP transport.

### WebSocket Event Streaming

Browser clients can mint a short-lived subscription token through:

```text
POST /api/ws/traces/{traceId}/subscription-token
```

and then subscribe to:

```text
GET /ws/plan-events
```

using the issued token.

### Operational Endpoints

- `/healthz`
- `/metrics`

## Tool Catalog

The repository exposes session, execution, element, gesture, screenshot, health, Device Farm, and optional debugging tools through MCP and JSON-RPC.

Core interactive tools include:

- `startSession`
- `endSession`
- `executePlan`
- `cancelPlan`
- `getTrace`
- `findElement`
- `clickElement`
- `sendKeysToElement`
- `clearElement`
- `getElementText`
- `getElementAttribute`
- `isElementDisplayed`
- `tap`
- `swipe`
- `longPress`
- `pressBack`
- `hideKeyboard`
- `takeScreenshot`
- `getSemanticSnapshot`
- `healthCheck`

Device Farm tools include:

- `createDeviceFarmUpload`
- `getDeviceFarmUpload`
- `getDeviceFarmRuntimeContext`
- `scheduleDeviceFarmRun`
- `getDeviceFarmRun`

Optional device-shell debugging can be exposed through `adbShell`. Operators can remove that tool completely with:

```yaml
gateway:
  disable_adb_shell_tool: true
```

### Supported `executePlan` Step Types

The built-in plan executor supports the following step types:

- `click`
- `clearElement`
- `wait`
- `sendKeys`
- `tap`
- `swipe`
- `longPress`
- `pressBack`
- `hideKeyboard`
- `screenshot`

## AWS Device Farm

### Scheduled Runs

Use `run_api` mode when you want the service to upload assets, schedule Device Farm runs, and query run status.

### Live Remote Sessions

Use `remote_access` mode when `startSession` should create a live Device Farm remote-access Appium session. The following `w3cCapsJson` extension keys are supported:

- `devicefarm:deviceArn` required
- `devicefarm:appArn` optional
- `devicefarm:projectArn` optional
- `devicefarm:sessionName` optional

## Configuration

Start from one of the shipped configuration templates:

- [config.example.yaml](./config.example.yaml) for local development
- [config.production.example.yaml](./config.production.example.yaml) for staging and production-style deployments

## Additional Documentation

- [Third-Party Integration Guide](./THIRD_PARTY_INTEGRATION.md)
- [Usage Examples](./USAGE_EXAMPLES.md)
- [PostgreSQL Migrations](./internal/storage/postgres/migrations/README.md)
- [Changelog](./CHANGELOG.md)
- [Release Notes v1.0.0](./RELEASE_NOTES_v1.0.0.md)
- [Security Policy](./SECURITY.md)
- [MIT License](./LICENSE)

## License

This project is released under the MIT License. See [LICENSE](./LICENSE) for the full text.
