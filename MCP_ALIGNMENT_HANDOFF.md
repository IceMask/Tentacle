# MCP Alignment Handoff

Updated: 2026-08-06

This handoff records the current MCP alignment state in `/Users/stellajen/Documents/workspace/mcp_for_appium_v1` on branch `v1-work`.

## Current Status

The original P0 legacy alignment is complete, including request/notification classification, silent notification responses, cancellation, legacy `ping`, and `/mcp` Streamable HTTP support.

The follow-up implementation now targets the official Current MCP revision `2026-07-28` while preserving dual-era compatibility:

- Modern `2026-07-28` requests use stateless per-request `_meta` and do not initialize.
- Legacy `2025-11-25` and `2025-06-18` clients continue to use initialize semantics.
- `server/discover` advertises all three supported versions and the implemented tools/resources capabilities.
- Successful modern results receive `resultType: complete` and namespaced server identity metadata.
- Modern Streamable HTTP validates `MCP-Protocol-Version`, `Mcp-Method`, and method-specific `Mcp-Name`, including Base64 sentinel decoding.
- Modern HTTP removes GET streams and session IDs, maps header mismatch to `400/-32020`, unsupported versions to `400/-32022`, and unknown methods to `404/-32601`.
- Modern HTTP rejects client notifications because cancellation is represented by closing the request response stream; stdio cancellation remains supported.
- Modern tool and resource results receive method-specific cache metadata while legacy result shapes remain unchanged; tool ordering is deterministic.
- Modern parameterized resources are exposed through `resources/templates/list`; `resources/list` reports the currently enumerable concrete set while legacy callers retain the historical catalog shape.
- Resource reads map missing or caller-invisible URIs to `-32602` and backend failures to `-32603`, replacing the old generic `-32000` path.
- Shared JSON-RPC decoding distinguishes omitted IDs from explicit JSON null and accepts only string or integer request IDs.
- Tool schemas are explicitly advertised and validated as JSON Schema 2020-12.
- Tool calls now expose native results through `structuredContent` in addition to text content.

The detailed protocol and compatibility summary is in `MCP_2026_07_28_ALIGNMENT.md`.

## Primary Implementation Areas

- `internal/gateway/mcp/protocol.go`
- `internal/gateway/mcp/handler.go`
- `internal/gateway/mcp/tools.go`
- `internal/gateway/jsonrpc/modern.go`
- `internal/gateway/jsonrpc/handler.go`
- `internal/gateway/mcphttp/handler.go`
- `README.md`

## Test Coverage

Targeted tests cover:

- mandatory modern discovery over shared JSON-RPC and Streamable HTTP
- required request metadata and unsupported-version retry data
- result decoration and server identity
- legacy initialize, ping, notifications, and client response compatibility
- modern HTTP method/name/version header consistency
- Base64 sentinel decoding
- modern HTTP client response and notification rejection
- modern method-not-found HTTP 404 behavior
- modern concrete-resource and resource-template separation
- current resource-not-found and internal error codes
- explicit-null and non-integral request ID rejection
- cache metadata and deterministic tool order
- JSON Schema 2020-12 `unevaluatedProperties` behavior

## Remaining Optional Work

The server does not declare optional Prompts, subscriptions/listen, sampling, elicitation, MCP Apps, or Tasks capabilities. Adding one later requires capability advertisement, request-local client capability checks where applicable, transport behavior, and conformance tests.

Tool `title`, annotations, icons, and outputSchema remain optional metadata improvements. They are not required for the implemented tools/resources core server surface.

## Validation Commands

```bash
GOCACHE=/tmp/mcp-for-appium-go-cache go test ./... -count=1
GOCACHE=/tmp/mcp-for-appium-go-cache go vet ./...
```
