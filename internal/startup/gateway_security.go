// gateway_security.go validates gateway security-related configuration such as auth sources and WebSocket origin policy.
package startup

import (
	"fmt"
	"strings"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
)

// validateGatewaySecurityConfig checks gateway auth and browser-origin settings before the HTTP listener starts.
func validateGatewaySecurityConfig(cfg *config.Config) error {
	if cfg == nil { // Reject a missing config before security-specific validation touches nested gateway and auth fields.
		return errors.New(errors.CodeConfigInvalid, "gateway config is nil") // Surface the nil config as a stable startup error instead of allowing a later panic.
	}
	if err := validateAllowedOrigins(cfg.Gateway.AllowedOrigins); err != nil { // Validate the optional WebSocket/browser origin allowlist before HTTP handlers start evaluating it.
		return err // Preserve the specific allowlist validation error for the caller.
	}
	if strings.TrimSpace(cfg.Auth.OIDC.IssuerURL) != "" { // Validate OIDC issuer fields only when the v4.4 config actually enables OIDC by providing an issuer URL.
		if _, err := parseHTTPURL(cfg.Auth.OIDC.IssuerURL, "auth.oidc.issuer_url"); err != nil { // Validate the effective OIDC issuer URL before the runtime attempts provider discovery against it.
			return err // Preserve the issuer URL validation failure for the caller.
		}
		if strings.TrimSpace(cfg.Auth.OIDC.JWKSURLOverride) != "" { // Validate the optional raw JWKS override only when the operator configures it explicitly.
			if _, err := parseHTTPURL(cfg.Auth.OIDC.JWKSURLOverride, "auth.oidc.jwks_url_override"); err != nil { // Validate the optional raw JWKS override as an HTTP(S) URL before runtime discovery starts.
				return err // Preserve the raw JWKS override validation failure for the caller.
			}
		}
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Auth.PAT.Mode), "static") { // Validate static PAT records only when the deployment explicitly selects the static PAT source.
		if _, err := auth.NewStaticPATLookup(cfg.Auth.PAT.StaticTokens); err != nil { // Parse the configured PAT hash records during startup so malformed auth config fails fast.
			return errors.Wrap(errors.CodeConfigInvalid, "auth.pat.static_tokens is invalid", err) // Surface the malformed static PAT source as a config error.
		}
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Auth.HMAC.KeySource), "postgres") && strings.TrimSpace(cfg.Storage.Postgres.DSN) == "" { // Reject postgres-backed HMAC auth when PostgreSQL is unavailable because the key registry cannot be queried without it.
		return errors.New(errors.CodeConfigMissing, "storage.postgres.dsn is required when auth.hmac.key_source=postgres") // Surface the missing PostgreSQL source before the gateway starts serving signed requests.
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Auth.PAT.Mode), "postgres") && strings.TrimSpace(cfg.Storage.Postgres.DSN) == "" { // Reject postgres-backed PAT auth when PostgreSQL is unavailable because the PAT source cannot be queried without it.
		return errors.New(errors.CodeConfigMissing, "storage.postgres.dsn is required when auth.pat.mode=postgres") // Surface the missing PostgreSQL source before the gateway starts serving PAT-authenticated requests.
	}

	return nil // Return success once every security-related gateway config field has passed validation.
}

// validateAllowedOrigins checks a comma-separated list of browser origins accepted by the WebSocket handshake.
func validateAllowedOrigins(rawOrigins string) error {
	trimmedOrigins := strings.TrimSpace(rawOrigins) // Normalize the raw allowlist string so empty and whitespace-only values behave consistently.
	if trimmedOrigins == "" {                       // Treat an empty allowlist as valid because the runtime falls back to strict same-origin behavior in that case.
		return nil // Return success because no explicit browser-origin allowlist was configured.
	}

	for index, rawOrigin := range strings.Split(trimmedOrigins, ",") { // Parse each configured origin individually so startup errors identify the broken entry.
		parsedOrigin, err := parseHTTPURL(rawOrigin, fmt.Sprintf("gateway.allowed_origins[%d]", index)) // Validate each origin as an absolute HTTP(S) URL because browser Origin headers use that shape.
		if err != nil {                                                                                 // Stop when any configured origin is malformed because partial allowlists are difficult to reason about safely.
			return err // Preserve the malformed-origin validation failure for the caller.
		}
		if parsedOrigin.Path != "" && parsedOrigin.Path != "/" { // Reject path-qualified origins because browser Origin headers never include application paths.
			return errors.New(errors.CodeConfigInvalid, fmt.Sprintf("gateway.allowed_origins[%d] must not include a path", index)) // Surface the invalid path-bearing origin explicitly.
		}
		if parsedOrigin.RawQuery != "" || parsedOrigin.Fragment != "" { // Reject query strings and fragments because Origin headers never include either component.
			return errors.New(errors.CodeConfigInvalid, fmt.Sprintf("gateway.allowed_origins[%d] must not include query or fragment", index)) // Surface the invalid decorated origin explicitly.
		}
	}

	return nil // Return success once every configured browser origin has the expected absolute origin shape.
}
