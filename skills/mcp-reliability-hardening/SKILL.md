---
name: mcp-reliability-hardening
description: Use this skill when updating mcp_for_appium reliability-critical paths (dispatcher retry/ack, execution mode split, cancel propagation, session recovery, and release validation).
---

# MCP Reliability Hardening

Use this workflow for any change that touches job dispatch, execution mode routing, cancel behavior, or Appium session lifecycle.

## When to use

- Changes under `internal/orchestrator/`
- Changes that affect `cmd/gateway` or `cmd/orchestrator` runtime behavior
- Changes that can impact message loss, duplicate execution, or cancel reliability
- Changes that alter session ownership/recovery

## Workflow

1. Confirm the target runtime mode before coding.
2. Validate queue semantics first (`ACK` only after successful dispatch/execute).
3. Ensure cancel has two paths: local cancellation + distributed RPC cancellation.
4. Verify session recovery behavior for in-memory miss paths.
5. Run compile-level full-repo tests, then run core package tests.
6. Sync docs if tool counts, config fields, or execution behavior changed.

## Implementation rules

- Keep dispatcher transient failures retriable; never ACK terminally by mistake.
- Keep mode split explicit:
  - `monolith`: dispatcher executes locally.
  - `distributed`: dispatcher forwards to workers.
- Any distributed cancel change must include:
  - trace-to-worker assignment record
  - cancel RPC forwarding
  - assignment cleanup
- Session retrieval paths must not rely only on in-memory map.

## Required checks

- `GOCACHE=/tmp/go-build-cache go test -run TestDoesNotExist ./...`
- `GOCACHE=/tmp/go-build-cache go test ./internal/gateway/mcp ./internal/worker`

If environment blocks network/listener tests, mention the limitation explicitly in the report.

## References

- For validation checklist and expected outcomes, read `references/validation-checklist.md`.
