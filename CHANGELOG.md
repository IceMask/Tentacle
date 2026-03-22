# Changelog

All notable changes to this repository will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions are intended to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Distributed worker-to-orchestrator callbacks for step events, lease renewals, and terminal results.
- Redis-backed distributed ownership leases with persisted `traces.current_attempt` and `traces.terminal_reason` state.
- Worker-side session attachment and stable distributed callback propagation for existing Appium sessions.
- Worker recovery-window handling in the registry so offline workers re-enter through `degraded` before returning to `healthy`.
- Real distributed callback and worker recovery tests.
- Persisted session and trace ownership metadata plus resource-level authorization checks for authenticated trace and session access.
- Repository-wide CI workflow that runs `go test ./... -count=1`.
- Release-tag verification workflow that runs the general suite plus migration replay and monolith integration checks.
- `v1.0.0` release checklist for the remaining stabilization work.
- Top-level `SECURITY.md` and draft `RELEASE_NOTES_v1.0.0.md` release documentation.
- Production-oriented example configuration and end-to-end distributed plus artifact integration coverage.

### Changed

- Distributed dispatch now reserves an execution attempt before RPC dispatch and promotes that reserved attempt to `running` only after worker acceptance.
- Watchdog handling now distinguishes plan timeout from ownership-lease expiry and prevents stale callbacks from overwriting active or terminal traces.
- Worker registration now uses a routable advertise address instead of hard-coded `localhost`.
- Migration docs and example configuration now include distributed execution-state and advertise-address fields.
- Distributed dispatch now resolves the worker-facing Appium session id before RPC dispatch so workers attach to the real Appium session.
- WebSocket subscription-token issuance now authorizes the requested trace against persisted ownership metadata before minting a browser credential.

### Fixed

- Distributed workers no longer execute plans without reusing the orchestrator-created Appium session id.
- Accepted distributed traces no longer rely solely on plan timeout for closure; stale ownership now expires independently.
- Late distributed callbacks can no longer append replay events or overwrite a trace once the owning lease or attempt has changed.
