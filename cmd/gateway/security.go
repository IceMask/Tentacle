// security.go wires gateway runtime security helpers such as auth middleware construction and TLS-aware HTTP serving.
package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/config"
	gatewaymiddleware "mcp_for_appium/internal/gateway/middleware"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/storage/redis"
)

// buildGatewayAuthMiddleware constructs the active HTTP auth middleware from the current config, storage, and cache sources.
func buildGatewayAuthMiddleware(ctx context.Context, cfg *config.Config, pgDAO *postgres.DAO, redisCache *redis.Cache) (*gatewaymiddleware.AuthMiddleware, error) {
	hmacValidator, err := buildGatewayHMACValidator(cfg, pgDAO, redisCache) // Construct the optional HMAC validator first because signed requests bypass bearer-token routing entirely.
	if err != nil {                                                         // Stop immediately when the configured HMAC source cannot be initialized safely.
		return nil, err // Preserve the HMAC configuration failure so startup can stop with the precise root cause.
	}
	oidcValidator, err := buildGatewayOIDCValidator(ctx, cfg) // Construct the optional OIDC validator next because bearer JWTs route into it directly.
	if err != nil {                                           // Stop immediately when the configured OIDC provider cannot be initialized safely.
		return nil, err // Preserve the OIDC construction failure so startup can stop with the precise root cause.
	}
	patValidator, err := buildGatewayPATValidator(cfg, pgDAO) // Construct the optional PAT validator from the configured PAT source selected by auth.pat.mode.
	if err != nil {                                           // Stop immediately when the configured PAT source cannot be initialized safely.
		return nil, err // Preserve the PAT construction failure so startup can stop with the precise root cause.
	}

	authMiddleware := gatewaymiddleware.NewAuthMiddleware(hmacValidator, oidcValidator, patValidator) // Build the combined gateway auth middleware, or nil when no validator is active.
	if authMiddleware != nil {                                                                        // Attach audit persistence only when protected-route authentication is active.
		authMiddleware.SetAuditRecorder(postgres.NewAuditRecorder(pgDAO)) // Record every authentication success and failure through the shared append-only PostgreSQL sink.
	}
	return authMiddleware, nil // Return the fully wired middleware after validator and optional audit construction succeeds.
}

// buildGatewayHMACValidator constructs the optional HMAC validator from the configured static or PostgreSQL-backed key source.
func buildGatewayHMACValidator(cfg *config.Config, pgDAO *postgres.DAO, redisCache *redis.Cache) (*auth.HMACValidator, error) {
	if cfg == nil { // Skip HMAC auth entirely when the gateway config is missing because no source selection can be evaluated safely.
		return nil, nil // Return nil so the caller knows HMAC auth is inactive rather than partially initialized.
	}

	switch strings.ToLower(strings.TrimSpace(cfg.Auth.HMAC.KeySource)) { // Branch on the normalized HMAC source selector so each source can initialize its own lookup contract.
	case "", "disabled":
		return nil, nil // Return nil so the caller knows HMAC auth is inactive rather than broken.
	case "static":
		staticSecret := strings.TrimSpace(cfg.Auth.HMAC.StaticSecret) // Prefer the new config field for the static HMAC secret so startup behavior follows the v4.4 config model.
		if staticSecret == "" {                                       // Fall back to the legacy process environment only when the new config field is still empty.
			staticSecret = strings.TrimSpace(os.Getenv("HMAC_SECRET")) // Preserve compatibility with legacy HMAC_SECRET deployments while centralizing new config on auth.hmac.static_secret.
		}
		staticKeyID := strings.TrimSpace(cfg.Auth.HMAC.StaticKeyID) // Prefer the new config field for the static HMAC key ID so startup behavior follows the v4.4 config model.
		if staticKeyID == "" {                                      // Fall back to the legacy process environment only when the new config field is still empty.
			staticKeyID = strings.TrimSpace(os.Getenv("HMAC_KEY_ID")) // Preserve compatibility with legacy HMAC_KEY_ID deployments while centralizing new config on auth.hmac.static_key_id.
		}
		if staticKeyID == "" { // Provide a deterministic default key ID for the static source when the operator leaves it unset.
			staticKeyID = "static-hmac-key" // Keep the static HMAC key ID stable across requests and restarts without requiring extra configuration.
		}

		return auth.NewHMACValidator(func(_ context.Context, requestKeyID string) (*auth.HMACKeyMaterial, error) { // Build the static key lookup used by the HMAC validator at request time.
			if strings.TrimSpace(requestKeyID) != staticKeyID { // Reject mismatched key IDs so callers cannot authenticate with a key ID that was never configured.
				return nil, errors.New("unknown hmac key id") // Surface unknown key IDs so the validator can wrap them into an authentication failure.
			}

			return &auth.HMACKeyMaterial{KeyID: staticKeyID, SubjectID: staticKeyID, Secret: staticSecret, Status: "active"}, nil // Return the static key material in the canonical v4.4 lookup shape expected by the HMAC validator.
		}, redisCache, cfg.Auth.HMAC.Window, cfg.Auth.HMAC.NonceTTL), nil // Reuse Redis for nonce replay protection and apply the configured HMAC timing windows.
	case "postgres":
		if pgDAO == nil { // Reject postgres mode without a DAO because the gateway cannot load HMAC keys from PostgreSQL without it.
			return nil, errors.New("postgres-backed hmac auth requires a postgres DAO") // Surface the missing DAO as a startup wiring failure.
		}

		return auth.NewHMACValidator(func(requestCtx context.Context, keyID string) (*auth.HMACKeyMaterial, error) { // Build the PostgreSQL-backed HMAC key lookup used by the validator at request time.
			record, err := pgDAO.GetHMACKey(requestCtx, strings.TrimSpace(keyID)) // Resolve the authoritative HMAC key row by the caller-supplied key ID.
			if err != nil {                                                       // Stop immediately when the HMAC key row cannot be loaded from PostgreSQL.
				return nil, err // Preserve the DAO lookup failure so the validator can wrap it into an authentication failure.
			}

			material := &auth.HMACKeyMaterial{KeyID: record.KeyID, SubjectID: record.KeyID, TenantID: strings.TrimSpace(record.TenantID), Secret: record.SecretRef, Status: strings.TrimSpace(record.Status)} // Translate the PostgreSQL record into the canonical HMAC key lookup shape expected by the validator.
			if record.NotBefore.Valid {                                                                                                                                                                       // Copy the key validity start only when the database row actually stores one.
				notBefore := record.NotBefore.Time.UTC() // Normalize the HMAC key validity start into UTC so request-time comparisons stay consistent.
				material.NotBefore = &notBefore          // Attach the normalized validity start to the returned key material.
			}
			if record.NotAfter.Valid { // Copy the key validity end only when the database row actually stores one.
				notAfter := record.NotAfter.Time.UTC() // Normalize the HMAC key validity end into UTC so request-time comparisons stay consistent.
				material.NotAfter = &notAfter          // Attach the normalized validity end to the returned key material.
			}

			return material, nil // Return the authoritative PostgreSQL-backed HMAC key material for request-signature validation.
		}, redisCache, cfg.Auth.HMAC.Window, cfg.Auth.HMAC.NonceTTL), nil // Reuse Redis for nonce replay protection and apply the configured HMAC timing windows.
	default:
		return nil, errors.New("unsupported hmac key source") // Reject unexpected source selectors defensively even though config validation should already have normalized them.
	}
}

// buildGatewayOIDCValidator constructs the optional OIDC validator from the v4.4 issuer_url plus audience configuration.
func buildGatewayOIDCValidator(ctx context.Context, cfg *config.Config) (*auth.OIDCValidator, error) {
	if cfg == nil { // Skip OIDC auth entirely when the gateway config is missing because no issuer or audience can be evaluated safely.
		return nil, nil // Return nil so the caller knows OIDC auth is inactive rather than partially initialized.
	}

	issuerURL := strings.TrimSpace(cfg.Auth.OIDC.IssuerURL) // Read the primary v4.4 issuer_url because OIDC discovery should be anchored on the issuer endpoint.
	audience := strings.TrimSpace(cfg.Auth.OIDC.Audience)   // Read the v4.4 audience because OIDC validation must always enforce the intended client audience.
	if issuerURL == "" || audience == "" {                  // Treat incomplete OIDC config as inactive because config validation already rejects half-configured new layouts.
		return nil, nil // Return nil so the caller knows OIDC auth is inactive rather than partially initialized.
	}

	return auth.NewOIDCValidator(ctx, issuerURL, audience, strings.TrimSpace(cfg.Auth.OIDC.JWKSURLOverride)) // Build the OIDC validator with discovery or the configured explicit JWKS endpoint.
}

// buildGatewayPATValidator constructs the optional PAT validator from the configured static or PostgreSQL-backed token source.
func buildGatewayPATValidator(cfg *config.Config, pgDAO *postgres.DAO) (*auth.PATValidator, error) {
	if cfg == nil { // Skip PAT auth entirely when the gateway config is missing because no PAT source selection can be evaluated safely.
		return nil, nil // Return nil so the caller knows PAT auth is inactive rather than partially initialized.
	}

	switch strings.ToLower(strings.TrimSpace(cfg.Auth.PAT.Mode)) { // Branch on the normalized PAT source selector so each source can initialize its own lookup contract.
	case "", "disabled":
		return nil, nil // Return nil so the caller knows PAT auth is inactive rather than broken.
	case "static":
		lookup, err := auth.NewStaticPATLookup(cfg.Auth.PAT.StaticTokens) // Parse the configured static PAT records once during startup so runtime validation only performs in-memory lookups.
		if err != nil {                                                   // Stop immediately when the PAT static source is malformed because runtime auth would otherwise fail unpredictably.
			return nil, err // Preserve the static PAT source parse failure for the caller.
		}

		return auth.NewPATValidator(lookup.Lookup), nil // Build the PAT validator over the parsed static lookup using the v4.4 request-context-aware lookup signature.
	case "postgres":
		if pgDAO == nil { // Reject postgres mode without a DAO because the gateway cannot load PATs from PostgreSQL without it.
			return nil, errors.New("postgres-backed pat auth requires a postgres DAO") // Surface the missing DAO as a startup wiring failure.
		}

		return auth.NewPATValidator(func(requestCtx context.Context, tokenID string) (*auth.PATRecord, error) { // Build the PostgreSQL-backed PAT lookup used by the validator at request time.
			record, err := pgDAO.GetPATToken(requestCtx, strings.TrimSpace(tokenID)) // Resolve the authoritative PAT row by the parsed PAT token ID.
			if err != nil {                                                          // Stop immediately when the PAT row cannot be loaded from PostgreSQL.
				return nil, err // Preserve the DAO lookup failure so the validator can wrap it into an authentication failure.
			}

			patRecord := &auth.PATRecord{TokenID: record.TokenID, SubjectID: strings.TrimSpace(record.SubjectID), TenantID: strings.TrimSpace(record.TenantID), SecretHash: strings.TrimSpace(record.SecretHash), Status: strings.TrimSpace(record.Status), ScopesJSON: record.ScopesJSON} // Translate the PostgreSQL row into the canonical PAT lookup shape expected by the validator.
			if record.ExpiresAt.Valid {                                                                                                                                                                                                                                                    // Copy the PAT expiry only when the database row actually stores one.
				expiresAt := record.ExpiresAt.Time.UTC() // Normalize the PAT expiry into UTC so request-time comparisons stay consistent.
				patRecord.ExpiresAt = &expiresAt         // Attach the normalized expiry to the returned PAT record.
			}

			return patRecord, nil // Return the authoritative PostgreSQL-backed PAT record for token validation.
		}), nil // Build the PAT validator over the PostgreSQL-backed lookup closure.
	default:
		return nil, errors.New("unsupported pat mode") // Reject unexpected source selectors defensively even though config validation should already have normalized them.
	}
}

// newGatewayHTTPServer constructs the runtime HTTP server with TLS defaults aligned to the gateway config.
func newGatewayHTTPServer(cfg *config.Config, handler http.Handler) *http.Server {
	server := &http.Server{ // Construct the bounded server once so the caller can decide whether to run it in TLS or plaintext mode.
		Addr:              ":" + strconv.Itoa(cfg.Gateway.Port), // Bind the configured gateway port on every available interface as before.
		Handler:           handler,                              // Route accepted requests through tracing and the gateway mux supplied by the caller.
		ReadHeaderTimeout: 5 * time.Second,                      // Bound slow or incomplete request headers before any application handler runs.
		ReadTimeout:       30 * time.Second,                     // Bound request-body reads so slow clients cannot retain connections indefinitely.
		WriteTimeout:      10 * time.Minute,                     // Bound complete response handling while leaving room for the documented synchronous plan wait.
		IdleTimeout:       2 * time.Minute,                      // Reclaim inactive keep-alive connections after a predictable idle window.
		MaxHeaderBytes:    1 << 20,                              // Cap aggregate request headers at one mebibyte to constrain parsing memory.
	}
	if cfg.Gateway.EnableTLS { // Apply an explicit TLS floor only when HTTPS is enabled for the gateway listener.
		server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12} // Enforce TLS 1.2+ so the gateway does not negotiate deprecated protocol versions.
	}

	return server // Return the configured HTTP server so the caller can start and later shut it down.
}

// serveGatewayHTTP runs the gateway server in HTTP or HTTPS mode according to the current gateway config.
func serveGatewayHTTP(server *http.Server, cfg *config.Config) error {
	if cfg.Gateway.EnableTLS { // Start the listener in HTTPS mode when TLS is explicitly enabled in config.
		return server.ListenAndServeTLS(cfg.Gateway.TLSCertFile, cfg.Gateway.TLSKeyFile) // Use the configured certificate pair for the HTTPS listener.
	}

	return server.ListenAndServe() // Fall back to plaintext HTTP only when TLS has not been enabled.
}

// decodeScopesJSON converts one stored JSON scope payload into a slice of strings while tolerating empty records.
func decodeScopesJSON(raw []byte) []string {
	if len(raw) == 0 { // Treat empty scope payloads as an empty scope set because not every PAT source stores scopes yet.
		return nil // Return a nil scope slice so downstream callers can distinguish "no scopes recorded" from a non-empty scope set.
	}

	var scopes []string                                  // Allocate the destination slice lazily because many PAT rows may not store scopes at all.
	if err := json.Unmarshal(raw, &scopes); err != nil { // Ignore malformed stored scopes defensively so one bad row does not crash gateway startup.
		return nil // Return an empty scope set when stored PAT scopes are malformed because authentication should still remain deterministic.
	}

	return scopes // Return the decoded scope slice for downstream authorization logic.
}

// sqlNullTimeToPointer converts one nullable SQL timestamp into an optional UTC time pointer.
func sqlNullTimeToPointer(value sql.NullTime) *time.Time {
	if !value.Valid { // Return nil when the database column is null because there is no timestamp to attach to the auth record.
		return nil // Preserve null semantics so callers can distinguish missing validity windows from explicit timestamps.
	}

	normalizedTime := value.Time.UTC() // Normalize the nullable SQL timestamp into UTC so request-time comparisons stay consistent across environments.
	return &normalizedTime             // Return the normalized timestamp pointer so callers can attach it to auth records directly.
}
