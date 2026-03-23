# Security Policy

This document explains how to report security issues, which release lines are supported, and which deployment expectations should be preserved in production.

## Reporting a Vulnerability

- Please report suspected vulnerabilities privately to the repository maintainers before opening a public issue.
- Include the affected version or commit, deployment mode, configuration choices, and the exact reproduction steps.
- If the issue affects authentication, transport security, trace ownership, or AWS Device Farm integration, include sanitized logs or request samples that demonstrate the problem.
- Do not publish proof-of-concept exploits, long-lived credentials, or live infrastructure addresses in public issues before maintainers have acknowledged the report.

## Supported Versions

| Version / Branch | Support Status | Notes |
| --- | --- | --- |
| `main` | Supported | Active stable release line |
| Latest `v1.x` release tag | Supported | Recommended release target for production use |
| Older release branches | Not supported | Please reproduce against the latest stable line first |

## Production Security Expectations

- Deploy external HTTP, JSON-RPC, and WebSocket traffic behind TLS.
- Use `tls` or `mtls` for internal RPC links in hosted environments.
- Treat PATs, OIDC settings, HMAC keys, AWS credentials, and WebSocket subscription tokens as secrets.
- Disable `adbShell` unless the environment explicitly requires device-shell diagnostics.
- Review the production configuration template before promoting a deployment:
  - [config.production.example.yaml](./config.production.example.yaml)

## Security Scope Highlights

- Public authentication surfaces include PAT, OIDC bearer validation, and HMAC request signing.
- Browser event subscriptions use short-lived WebSocket subscription tokens.
- Trace, session, and artifact state is persisted through PostgreSQL, Redis, and S3-backed workflows.
- AWS Device Farm integrations can create uploads, runs, and live remote-access Appium sessions.

## Response Goals

- Triage acknowledgement target: within 5 business days
- Best-effort status update target after triage: within 10 business days
- Fix timing depends on severity, exploitability, and the affected release line

## Disclosure Guidance

- Coordinated disclosure is preferred.
- Please wait for maintainer acknowledgement before opening a public advisory when the issue could expose active users to risk.
