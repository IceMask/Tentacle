# Release Notes v1.0.0 Draft

This document is a draft release-notes package for the first stable release cut. It summarizes the expected operator-facing changes, deployment steps, migrations, upgrade notes, known limitations, and rollback expectations.

## Release Position

- Recommended current release posture before tagging: `v0.x`
- Recommended GA mode for the first stable release: `monolith`
- `distributed` should remain explicitly marked as `experimental` unless the remaining `v1.0.0_release_checklist.md` items are completed

## Highlights

- Standard MCP, JSON-RPC, REST, gRPC, WebSocket, and stdio gateway entry points remain available.
- Distributed execution now includes worker-to-orchestrator callbacks, execution-attempt tracking, Redis-backed ownership leases, worker recovery handling, and stale-result protection.
- Authentication and transport controls now include PAT mode selection, HMAC canonical request signing, WebSocket subscription tokens, and configurable internal RPC security modes.
- Repository release hygiene now includes a top-level changelog, a general CI workflow, migration replay verification, and an explicit release checklist.

## Breaking Or Behaviorally Significant Changes

- Distributed worker execution now reuses the orchestrator-created Appium session id instead of silently creating a fresh session in the worker.
- Accepted distributed traces now persist `current_attempt` and `terminal_reason` state on the `traces` table.
- Late distributed worker callbacks no longer overwrite an active or terminal trace when the owning attempt or lease has changed.
- Worker registration now depends on a routable advertise address instead of relying on a hard-coded `localhost` assumption.
- Internal RPC deployment guidance should move from `insecure` toward `tls`, with `mtls` as the later hardening target.

## Deployment Steps

1. Build or publish the current `gateway`, `orchestrator`, and `worker` binaries from the intended release commit.
2. Review `config.example.yaml` and update deployment configuration for any new or changed fields, especially `worker.advertise_addr`, PAT mode selection, and RPC security settings.
3. Apply every shipped PostgreSQL migration in lexical order before starting `gateway` or `orchestrator`.
4. Verify Redis, PostgreSQL, S3, and Appium reachability with the startup preflight checks enabled.
5. Run the repository validation commands before tagging:
   - `go test ./... -count=1`
   - `go run ./cmd/migration_replay_check`
6. For production-style environments, ensure external traffic is fronted by TLS and internal RPC transport is configured for `tls` or `mtls`.

## Required Migrations

Apply these migrations in order:

- `internal/storage/postgres/migrations/001_init.sql`
- `internal/storage/postgres/migrations/002_audit_logs.sql`
- `internal/storage/postgres/migrations/004_pat_tokens.sql`
- `internal/storage/postgres/migrations/005_hmac_keys.sql`
- `internal/storage/postgres/migrations/006_trace_execution_state.sql`

The latest migration adds:

- `traces.current_attempt`
- `traces.terminal_reason`
- `idx_traces_project_status_attempt_updated_at`

## Upgrade Notes

- Existing deployments must add the new `traces` execution-state columns before enabling the new distributed callback flow.
- Distributed worker deployments should set `worker.advertise_addr` to a routable address that the orchestrator can dial back.
- Review `rpc.security.mode` and avoid keeping production-style environments on `insecure`.
- Review PAT and HMAC storage expectations before enabling those paths in a shared environment.
- If you use WebSocket subscriptions from browsers, prefer the short-lived subscription-token flow instead of exposing long-lived credentials directly.

## Known Limitations

- `distributed` remains non-GA until a full end-to-end distributed integration flow and the remaining scope-declaration tasks are complete.
- WebSocket authorization still needs explicit trace ownership or tenant validation before that surface should be called GA.
- Some packages still have no direct tests and are covered only indirectly or not yet at all.
- The repository still needs final version-story cleanup between the `v4.3` and `v4.4` document sets before a clean `v1.0.0` tag.

## Rollback Guidance

- If the release must be rolled back, first stop new traffic to `gateway`, `orchestrator`, and `worker`.
- Roll back binaries and configuration as one unit so runtime expectations stay aligned.
- Rolling back code after `006_trace_execution_state.sql` has been applied is schema-compatible for older readers that ignore the new columns, but rollback verification must confirm that no code path depends on the new distributed callback behavior.
- If the rollback target predates `004_pat_tokens.sql` or `005_hmac_keys.sql`, disable the related auth modes before switching binaries.
- Preserve Redis and PostgreSQL data long enough to inspect trace state, leases, and audit history before deleting or recreating infrastructure.

## Release Manager Checklist

- Confirm `CHANGELOG.md` is updated for the exact release commit.
- Confirm `SECURITY.md` is present and linked from `README.md`.
- Confirm the general CI workflow and targeted workflows passed on the release candidate commit.
- Confirm release notes match the exact migrations and config fields shipped in the tag.
- Confirm docs clearly state whether `distributed` is GA or experimental for the tagged release.
