// oidc.go validates OIDC ID tokens through issuer discovery or an explicitly configured JWKS endpoint.
package auth

import (
	"context"
	"strings"

	"mcp_for_appium/internal/errors"

	"github.com/coreos/go-oidc/v3/oidc"
)

type OIDCValidator struct {
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
}

// NewOIDCValidator constructs an issuer- and audience-validating token verifier, optionally bypassing discovery with one explicit JWKS URL.
func NewOIDCValidator(ctx context.Context, issuerURL string, audience string, jwksURLOverride ...string) (*OIDCValidator, error) {
	config := &oidc.Config{ // Build the verifier policy once so discovery and explicit-JWKS paths enforce the same audience.
		ClientID: strings.TrimSpace(audience), // Require every accepted token to target the configured gateway audience.
	}
	if len(jwksURLOverride) > 0 && strings.TrimSpace(jwksURLOverride[0]) != "" { // Select the explicit JWKS source only when the caller configured a non-empty override.
		keySet := oidc.NewRemoteKeySet(ctx, strings.TrimSpace(jwksURLOverride[0])) // Build the caching remote key set against the operator-selected JWKS endpoint.
		verifier := oidc.NewVerifier(strings.TrimSpace(issuerURL), keySet, config) // Continue enforcing the configured issuer even when discovery is bypassed.
		return &OIDCValidator{verifier: verifier}, nil                             // Return the explicit-JWKS validator without contacting the issuer discovery endpoint during startup.
	}

	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, err
	}
	verifier := provider.Verifier(config)

	return &OIDCValidator{
		provider: provider,
		verifier: verifier,
	}, nil
}

// Validate executes this operation.
func (v *OIDCValidator) Validate(ctx context.Context, rawToken string) (*Subject, error) {
	if v == nil || v.verifier == nil { // Reject validation through an uninitialized OIDC verifier instead of panicking.
		return nil, errors.New(errors.CodeUnauthenticated, "oidc validator is not configured") // Surface the startup wiring defect through the stable auth contract.
	}
	// Remove Bearer prefix if present
	if strings.HasPrefix(rawToken, "Bearer ") {
		rawToken = strings.TrimPrefix(rawToken, "Bearer ")
	}

	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return nil, errors.Wrap(errors.CodeUnauthenticated, "invalid OIDC token", err)
	}

	// Extract claims
	var claims struct {
		Sub    string   `json:"sub"`
		Tenant string   `json:"tenant_id"` // Custom claim example
		Scopes []string `json:"scopes"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return nil, errors.Wrap(errors.CodeUnauthenticated, "failed to parse claims", err)
	}
	if strings.TrimSpace(claims.Sub) == "" { // Require the standard subject claim because downstream ownership cannot authorize an anonymous token.
		return nil, errors.New(errors.CodeUnauthenticated, "oidc token subject is missing") // Reject otherwise valid tokens that cannot identify one principal.
	}

	return &Subject{
		ID:           strings.TrimSpace(claims.Sub),    // Preserve the validated OIDC subject as the application principal.
		Type:         "oidc",                           // Identify the authentication scheme for authorization and audit consumers.
		CredentialID: strings.TrimSpace(claims.Sub),    // Use the subject as the safe credential identifier because no token secret or raw JWT is persisted.
		TenantID:     strings.TrimSpace(claims.Tenant), // Preserve the optional tenant claim for ownership checks.
		Scopes:       claims.Scopes,                    // Preserve validated token scopes for downstream authorization.
	}, nil
}
