# Security Policy

This document explains how to report security issues for this repository, which branches currently receive fixes, and which security assumptions operators should preserve in production deployments.

## Reporting A Vulnerability

- Please report suspected vulnerabilities privately to the repository maintainers before opening a public issue.
- Include the affected branch or commit, the deployment mode, the configuration choices involved, and the exact steps needed to reproduce the issue.
- If the issue affects authentication, transport security, or distributed execution ownership, include the relevant request or callback sequence and any sanitized logs that show the failure mode.
- Please avoid publishing proof-of-concept exploits, raw long-lived credentials, or live infrastructure addresses in public issues before the maintainers have had time to assess the report.

## Supported Versions

No stable `v1.0.0` release has been cut yet.

| Version / Branch | Support Status | Notes |
| --- | --- | --- |
| `v1` | Best-effort security fixes | Active pre-release branch used for the upcoming first stable release work. |
| Older branches and ad-hoc feature branches | Not supported | Please reproduce against `v1` before reporting a security issue. |

## Current Security Expectations

- `monolith` is the recommended production entry mode today.
- `distributed` remains non-GA and should be treated as an advanced or experimental deployment mode until the remaining release checklist items are complete.
- External `HTTP`, `JSON-RPC`, `REST`, and `WebSocket` traffic should be deployed behind TLS in production.
- Internal RPC links should use `tls` or `mtls` in production-style environments instead of `insecure`.
- Personal access tokens, OIDC configuration, HMAC keys, and WebSocket subscription tokens should be treated as secrets and must not be logged or committed.

## Security Scope Highlights

- Authentication surfaces currently include `PAT`, `OIDC`, and `HMAC` request signing.
- Distributed execution now depends on ownership leases, worker callbacks, and attempt-based stale-result protection.
- Production operators should review `config.example.yaml`, `README.md`, `RELEASE_NOTES_v1.0.0.md`, and `v1.0.0_release_checklist.md` together before promoting this repository to a stable release posture.

## Response Goals

- Triage acknowledgement target: within 5 business days.
- Best-effort status update target after triage: within 10 business days.
- Fix timing depends on severity, exploitability, and whether the issue affects the active `v1` branch.

## Disclosure Guidance

- Coordinated disclosure is preferred.
- Please wait for a maintainer acknowledgement before opening a public advisory or issue when the report could expose users to active risk.
