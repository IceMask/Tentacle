# Changelog

All notable changes to this repository are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions are intended to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- MCP `2026-07-28` stateless request metadata, `server/discover`, modern result decoration, and Streamable HTTP routing headers
- JSON Schema 2020-12 tool validation, deterministic discovery, resource-template separation, and exact JSON-RPC request ID preservation
- Runtime audit recording for authentication, WebSocket credentials, worker lifecycle, and distributed ownership transitions
- Worker capacity admission, execution deduplication, atomic Redis lease operations, and race-focused distributed tests

### Changed

- `tools/call` business and downstream failures now return normal MCP results with sanitized `isError` payloads
- HTTP inputs now use bounded strict JSON decoding, server timeouts, and transport-safe status mappings
- Session cleanup, queue retry, trace terminalization, Appium retry policy, authentication validation, and configuration loading now fail closed
- Public documentation now describes MCP `2026-07-28`, current dependency preflights, and the gateway/distributed runtime boundary

### Fixed

- Prevented stdio logs from corrupting protocol stdout and removed raw backend diagnostics from client-visible errors
- Prevented stale callbacks, duplicate worker execution, capacity races, and non-atomic lease renewal or release
- Preserved retryable session cleanup, trace/session screenshot ownership, DAO not-found distinctions, and cancelled worker waits

## [1.0.0] - 2026-03-24

### Added

- Standard MCP `stdio` support for AI clients and desktop integrations
- HTTP JSON-RPC gateway for service integrations
- WebSocket subscription-token flow for browser event streaming
- AWS Device Farm support for scheduled runs and live remote-access Appium sessions
- Repository-wide CI, migration replay validation, and integration-test coverage
- Production-oriented configuration examples, release notes, and security documentation

### Changed

- Unified session, trace, event, and artifact handling across monolith and distributed execution flows
- Strengthened worker orchestration with result callbacks, ownership tracking, and worker recovery handling
- Hardened authentication, RPC security, browser subscription authorization, and `adbShell` operator controls
- Removed the A2A-style REST business surface and kept only the supported public interfaces

### Fixed

- Trace finalization, retry, timeout, and stale-result handling across local and distributed execution paths
- Migration replay validation and release-branch CI gating
- Tool and capability discovery alignment with the shipped runtime behavior
