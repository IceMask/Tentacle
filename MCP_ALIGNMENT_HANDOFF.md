# MCP Alignment Handoff

This note captures the current MCP-alignment context so the next development session can continue on the `v1` worktree without rebuilding the analysis from scratch.

## Current Workspace State

- Worktree path: `/Users/stellajen/Documents/workspace/mcp_for_appium_v1`
- Active branch: `v1-work`
- Tracking branch: `origin/v1`
- Main worktree remains at `/Users/stellajen/Documents/workspace/mcp_for_appium`

## Documents Added In This Worktree

- `mcp_alignment_checklist.md`
- `requirements.mcp_alignment.md`
- `module_mcp_alignment_design.md`

These three documents define the agreed scope for bringing the MCP server closer to the latest official specification.

## Review Conclusions Already Reached

The current implementation is behind the latest MCP spec in several core areas:

1. `notifications/initialized` is not handled and currently falls through to method-not-found.
2. `ping` is not implemented.
3. `notifications/cancelled` is not handled and no in-flight request registry exists yet.
4. HTTP still uses a custom `/jsonrpc` request-response endpoint instead of standard Streamable HTTP.
5. HTTP does not validate `MCP-Protocol-Version`.
6. Tool metadata does not yet support `title` or `annotations`.
7. Tool responses do not yet support `structuredContent`.
8. The server still reports `protocolVersion: 2024-11-05`.

Items already considered okay or explicitly out of scope for this batch:

- `notifications/progress.message` is already implemented.
- JSON-RPC batching removal is not a problem because batching is not implemented.
- OAuth, Elicitation, Async Tasks, Tool icons, and sampling tool-calling are intentionally out of scope for this batch.

## Priority Order

### P0

1. Fix notification semantics so notifications are accepted silently and never generate responses.
2. Add `ping`.
3. Upgrade the server protocol version handling.
4. Implement Streamable HTTP.
5. Validate `MCP-Protocol-Version` for MCP over HTTP.

### P1

1. Add tool `title`.
2. Add tool `annotations`.
3. Add baseline `structuredContent`.

### P2

1. Decide whether to keep `/jsonrpc` as a documented compatibility endpoint.
2. Expand `structuredContent` coverage and later `outputSchema`.

## Primary Code Areas To Change

- `internal/gateway/jsonrpc/handler.go`
- `internal/gateway/stdio/transport.go`
- `internal/gateway/mcp/handler.go`
- `internal/gateway/mcp/tools.go`
- `internal/gateway/mcp/tools/*.json`
- `cmd/gateway/main.go`

Expected new code area:

- `internal/gateway/mcphttp` for Streamable HTTP transport

## Suggested Next Implementation Slice

The recommended first coding batch is:

1. Introduce request-vs-notification classification.
2. Ensure notifications never write responses in `stdio`.
3. Handle `notifications/initialized`.
4. Handle `ping`.
5. Add tests for the above before moving into HTTP transport work.

## Validation Expectations

Once coding starts, validation should include at minimum:

- targeted unit tests for handler + transport notification behavior
- full `go test ./... -count=1`
- no skipped tests counted as pass

## Notes

- No MCP alignment code changes have been made yet in this batch; only analysis and planning documents were produced.
- The public `main` worktree was intentionally kept free of these internal planning documents.
