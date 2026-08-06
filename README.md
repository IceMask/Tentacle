# Tentacle

Tentacle is a mobile automation gateway that exposes Appium and AWS Device Farm workflows through standard MCP over `stdio` and Streamable HTTP, HTTP JSON-RPC compatibility, WebSocket event streaming, and internal gRPC services.

It is designed for AI-assisted mobile testing, scripted automation, and browser-based monitoring. The repository ships with embedded validation tools, PostgreSQL migrations, and production-oriented configuration examples so the same codebase can be used for local development, CI, and hosted deployments.

## Highlights

- MCP `2026-07-28` stateless server with `2025-11-25` and `2025-06-18` initialization-based compatibility
- Standard MCP transports through `stdio` and Streamable HTTP `POST /mcp`
- HTTP JSON-RPC endpoint for service-to-service integrations
- Deterministic JSON Schema 2020-12 catalog with 26 built-in tools and an `adbShell` kill switch
- Session lifecycle, element actions, gestures, screenshots, health checks, and semantic snapshots
- Browser event streaming through short-lived WebSocket subscription tokens
- AWS Device Farm support for both scheduled test runs and live remote-access Appium sessions
- PostgreSQL, Redis, and S3-backed persistence for traces, events, and artifacts

## Quick Start

### Prerequisites And Build

Local development uses the Go version declared in `go.mod` and requires PostgreSQL, Redis, and an existing S3 or S3-compatible bucket. Gateway startup checks those three dependencies before listening. Appium 2 is required when a mobile session is created, but not for protocol discovery or health inspection.

```bash
cp config.example.yaml config.yaml
mkdir -p bin
go build -o ./bin/gateway ./cmd/gateway
```

Update `config.yaml` with reachable PostgreSQL, Redis, and S3 settings before starting the binary. The S3 identity needs `HeadBucket` access at startup and object access for screenshot artifacts.

### Claude Desktop

Add Tentacle as an MCP server in your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "tentacle": {
      "command": "/absolute/path/to/repository/bin/gateway",
      "args": ["--stdio", "--config", "/absolute/path/to/config.yaml"]
    }
  }
}
```

Common configuration file locations:

- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\\Claude\\claude_desktop_config.json`

### MCP Streamable HTTP

Start the gateway:

```bash
./bin/gateway --config config.yaml
```

Discover the server through the stateless MCP `2026-07-28` endpoint:

```bash
curl -X POST http://localhost:8080/mcp \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -H "MCP-Protocol-Version: 2026-07-28" \
  -H "Mcp-Method: server/discover" \
  -d '{
    "jsonrpc": "2.0",
    "method": "server/discover",
    "params": {
      "_meta": {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "local-test", "version": "1.0.0"}
      }
    },
    "id": 1
  }'
```

Every modern request repeats the namespaced protocol metadata. HTTP requests also mirror the method in `Mcp-Method`; `tools/call` and `resources/read` mirror their target in `Mcp-Name`.

### HTTP JSON-RPC Compatibility

The non-standard `/jsonrpc` endpoint remains available for existing integrations. List the available tools with:

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":2}'
```

The local configuration disables authentication. When PAT, OIDC, or HMAC authentication is enabled, supply the corresponding credentials to protected HTTP endpoints.

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

Run the distributed end-to-end flow separately with:

```bash
go test ./internal/integration -run TestDistributedStartExecuteTraceEndFlow -count=1 -v
```

## Public Interfaces

### MCP `stdio`

The `gateway --stdio` entry point exposes the full MCP tool catalog and is the simplest way to connect the project to AI tools and desktop clients. It accepts modern per-request metadata and both supported legacy initialize revisions.

### MCP Streamable HTTP

The standard endpoint is `POST /mcp`. Modern `2026-07-28` requests are stateless and validate `MCP-Protocol-Version`, `Mcp-Method`, and method-specific `Mcp-Name` headers. Legacy `2025-11-25` and `2025-06-18` initialize flows remain supported.

### HTTP JSON-RPC

The `/jsonrpc` endpoint preserves the operational surface for older service integrations, but it is not the standard MCP Streamable HTTP transport.

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

## Runtime Boundaries

- `monolith` is the supported gateway execution mode. Both HTTP and `stdio` embed the orchestrator in the gateway process.
- `distributed` remains experimental. The standalone orchestrator and worker implement capacity admission, leases, callbacks, recovery, and stale-result protection, but the gateway does not yet forward ingress traffic to an external orchestrator.
- Plan events are durable in PostgreSQL and published best-effort through Redis PubSub to the at-most-once WebSocket stream. Historical events are read through `getTrace`.
- `/metrics` currently shares the gateway listener; `telemetry.metrics_port` does not create a separate listener. Gateway and orchestrator initialize OTEL tracing, while worker currently uses structured logging.

## Tool Catalog

The repository contains 26 session, execution, element, gesture, screenshot, health, Device Farm, and debugging tools. The local example exposes all 26; the production example disables `adbShell` and therefore exposes 25.

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

The production file is still a template. Replace all placeholder hosts, certificates, tokens, and database settings, and configure transport encryption and access controls for PostgreSQL, Redis, and S3 before deployment.

## Additional Documentation

- [Third-Party Integration Guide](./THIRD_PARTY_INTEGRATION.md)
- [Usage Examples](./USAGE_EXAMPLES.md)
- [PostgreSQL Migrations](./internal/storage/postgres/migrations/README.md)
- [Changelog](./CHANGELOG.md)
- [Release Notes v1.0.0](./RELEASE_NOTES_v1.0.0.md)
- [Security Policy](./SECURITY.md)
- [MCP 2026-07-28 Alignment](./MCP_2026_07_28_ALIGNMENT.md)
- [MIT License](./LICENSE)

## License

This project is released under the MIT License. See [LICENSE](./LICENSE) for the full text.
