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

func NewOIDCValidator(ctx context.Context, issuerURL string, audience string) (*OIDCValidator, error) {
	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, err
	}

	config := &oidc.Config{
		ClientID: audience,
	}
	verifier := provider.Verifier(config)

	return &OIDCValidator{
		provider: provider,
		verifier: verifier,
	}, nil
}

func (v *OIDCValidator) Validate(ctx context.Context, rawToken string) (*Subject, error) {
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

	return &Subject{
		ID:       claims.Sub,
		Type:     "oidc",
		TenantID: claims.Tenant,
		Scopes:   claims.Scopes,
	}, nil
}
