# Changelog

All notable changes to this repository are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions are intended to follow [Semantic Versioning](https://semver.org/).

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
