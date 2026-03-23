# Release Notes v1.0.0

## Overview

`v1.0.0` is the first stable release of Tentacle. It provides a production-ready MCP and JSON-RPC gateway for mobile automation, trace persistence, artifact storage, browser event streaming, and AWS Device Farm integration.

## Highlights

- Full MCP `stdio` support for AI clients and desktop tools
- HTTP JSON-RPC gateway for service integrations
- Session lifecycle, element interaction, gesture control, screenshots, semantic snapshots, and health checks
- Live browser trace streaming through short-lived WebSocket subscription tokens
- AWS Device Farm support for scheduled runs and live remote-access Appium sessions
- PostgreSQL, Redis, and S3-backed persistence for traces, events, and artifacts
- Built-in validation commands for migrations and integration flows

## Public Interfaces

- `gateway --stdio`
- `POST /jsonrpc`
- `POST /api/ws/traces/{traceId}/subscription-token`
- `GET /ws/plan-events`
- `GET /healthz`
- `GET /metrics`

## Database Migrations

Apply these migrations in lexical order:

- `internal/storage/postgres/migrations/001_init.sql`
- `internal/storage/postgres/migrations/002_audit_logs.sql`
- `internal/storage/postgres/migrations/003_reserved_slot.sql`
- `internal/storage/postgres/migrations/004_pat_tokens.sql`
- `internal/storage/postgres/migrations/005_hmac_keys.sql`
- `internal/storage/postgres/migrations/006_trace_execution_state.sql`
- `internal/storage/postgres/migrations/007_trace_session_ownership.sql`

## Deployment Steps

1. Build the `gateway`, `orchestrator`, and `worker` binaries from the release commit.
2. Start from `config.production.example.yaml` for hosted deployments.
3. Apply all PostgreSQL migrations before starting runtime services.
4. Verify PostgreSQL, Redis, S3, and Appium reachability during startup.
5. Run the validation commands:
   - `go test ./... -count=1`
   - `go run ./cmd/migration_replay_check`

## AWS Device Farm

The release includes two Device Farm integration paths:

- `run_api` for uploads, scheduled runs, and run-status queries
- `remote_access` for live Appium sessions created through `startSession`

For live remote sessions, the following capability keys are supported in `w3cCapsJson`:

- `devicefarm:deviceArn`
- `devicefarm:appArn`
- `devicefarm:projectArn`
- `devicefarm:sessionName`

## Validation Summary

The release branch was validated with:

- full `go test ./...` execution
- migration replay verification
- monolith integration coverage
- distributed execution coverage
- artifact and MCP wait-flow integration coverage

## Related Documents

- [README](./README.md)
- [Third-Party Integration Guide](./THIRD_PARTY_INTEGRATION.md)
- [Usage Examples](./USAGE_EXAMPLES.md)
- [Security Policy](./SECURITY.md)
- [Changelog](./CHANGELOG.md)
