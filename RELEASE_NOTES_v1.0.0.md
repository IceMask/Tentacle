# Release Notes v1.0.0 Draft

This document is a draft release-notes package for the first stable release cut. It summarizes the expected operator-facing changes, deployment steps, migrations, upgrade notes, known limitations, and rollback expectations.

## Release Position

- Recommended current release posture before tagging: `v0.x`
- Recommended GA mode for the first stable release: `monolith`
- `distributed` is explicitly scoped as `experimental` for the first `v1.0.0` stable release, even though the callback, lease, recovery, and end-to-end coverage work is now in place

## Highlights

- Standard MCP, JSON-RPC, REST, gRPC, WebSocket, and stdio gateway entry points remain available.
- Distributed execution now includes worker-to-orchestrator callbacks, execution-attempt tracking, Redis-backed ownership leases, worker recovery handling, and stale-result protection.
- Authentication and transport controls now include PAT mode selection, HMAC canonical request signing, persisted session/trace ownership checks for WebSocket subscription issuance, and configurable internal RPC security modes.
- Repository release hygiene now includes a top-level changelog, a general CI workflow, migration replay verification, and an explicit release checklist.
- Repository examples now distinguish local-development defaults from production-oriented transport and auth defaults.

## Breaking Or Behaviorally Significant Changes

- Distributed worker execution now reuses the orchestrator-created Appium session id instead of silently creating a fresh session in the worker.
- Accepted distributed traces now persist `current_attempt` and `terminal_reason` state on the `traces` table.
- Sessions and traces now persist `tenant_id` and `subject_id` ownership metadata so authenticated resource access can fail closed when ownership cannot be proven.
- Late distributed worker callbacks no longer overwrite an active or terminal trace when the owning attempt or lease has changed.
- Worker registration now depends on a routable advertise address instead of relying on a hard-coded `localhost` assumption.
- Internal RPC deployment guidance should move from `insecure` toward `tls`, with `mtls` as the later hardening target.
- Distributed dispatch now resolves the worker-facing Appium session id before RPC dispatch so workers attach to the real Appium session.

## Deployment Steps

1. Build or publish the current `gateway`, `orchestrator`, and `worker` binaries from the intended release commit.
2. Review `config.example.yaml` and update deployment configuration for any new or changed fields, especially `worker.advertise_addr`, PAT mode selection, and RPC security settings.
3. For staging or production-style environments, prefer `config.production.example.yaml` as the starting point and only relax its transport defaults intentionally.
4. Apply every shipped PostgreSQL migration in lexical order before starting `gateway` or `orchestrator`.
5. Verify Redis, PostgreSQL, S3, and Appium reachability with the startup preflight checks enabled.
6. Run the repository validation commands before tagging:
   - `go test ./... -count=1`
   - `go run ./cmd/migration_replay_check`
7. For production-style environments, ensure external traffic is fronted by TLS and internal RPC transport is configured for `tls` or `mtls`.

## Required Migrations

Apply these migrations in order:

- `internal/storage/postgres/migrations/001_init.sql`
- `internal/storage/postgres/migrations/002_audit_logs.sql`
- `internal/storage/postgres/migrations/004_pat_tokens.sql`
- `internal/storage/postgres/migrations/005_hmac_keys.sql`
- `internal/storage/postgres/migrations/006_trace_execution_state.sql`
- `internal/storage/postgres/migrations/007_trace_session_ownership.sql`

The latest migrations add:

- `traces.current_attempt`
- `traces.terminal_reason`
- `sessions.tenant_id`
- `sessions.subject_id`
- `traces.tenant_id`
- `traces.subject_id`
- `idx_traces_project_status_attempt_updated_at`
- `idx_sessions_tenant_subject_created_at`
- `idx_traces_tenant_subject_created_at`

## Upgrade Notes

- Existing deployments must add the new `traces` execution-state columns before enabling the new distributed callback flow.
- Existing deployments that enable authenticated WebSocket subscriptions should also apply the new session/trace ownership columns before relying on the persisted ownership checks.
- Distributed worker deployments should set `worker.advertise_addr` to a routable address that the orchestrator can dial back.
- Production-style deployments should start from `config.production.example.yaml` instead of the local-development baseline in `config.example.yaml`.
- Review `rpc.security.mode` and avoid keeping production-style environments on `insecure`.
- Review PAT and HMAC storage expectations before enabling those paths in a shared environment.
- If you use WebSocket subscriptions from browsers, prefer the short-lived subscription-token flow instead of exposing long-lived credentials directly.

## Known Limitations

- `distributed` is intentionally shipped as `experimental` in the first stable release, even though the core correctness and end-to-end validation work is now in place.
- Repository-level branch protection still must be configured in GitHub so the general CI workflow becomes an enforced merge gate instead of a documented expectation.
- Some packages still have no direct tests and are covered only indirectly or not yet at all.
- The repository still needs final version-story cleanup between the `v4.3` and `v4.4` document sets before a clean `v1.0.0` tag.

## Rollback Guidance

- If the release must be rolled back, first stop new traffic to `gateway`, `orchestrator`, and `worker`.
- Roll back binaries and configuration as one unit so runtime expectations stay aligned.
- Rolling back code after `006_trace_execution_state.sql` and `007_trace_session_ownership.sql` have been applied is schema-compatible for older readers that ignore the new columns, but rollback verification must confirm that no code path depends on the new distributed callback or persisted ownership behavior.
- If the rollback target predates `004_pat_tokens.sql` or `005_hmac_keys.sql`, disable the related auth modes before switching binaries.
- Preserve Redis and PostgreSQL data long enough to inspect trace state, leases, and audit history before deleting or recreating infrastructure.

## Release Manager Checklist

- Confirm `CHANGELOG.md` is updated for the exact release commit.
- Confirm `SECURITY.md` is present and linked from `README.md`.
- Confirm the general CI workflow and targeted workflows passed on the release candidate commit.
- Confirm release notes match the exact migrations and config fields shipped in the tag.
- Confirm docs clearly state whether `distributed` is GA or experimental for the tagged release.
