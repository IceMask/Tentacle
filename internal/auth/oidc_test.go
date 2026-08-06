// oidc_test.go verifies OIDC validator construction for explicit JWKS overrides without issuer discovery.
package auth

import (
	"context"
	"testing"
)

// TestNewOIDCValidatorUsesJWKSOverrideWithoutDiscovery verifies that an explicit key endpoint avoids a startup request to issuer discovery.
func TestNewOIDCValidatorUsesJWKSOverrideWithoutDiscovery(t *testing.T) {
	validator, err := NewOIDCValidator(context.Background(), "https://issuer.invalid", "gateway-audience", "https://keys.invalid/jwks.json") // Construct against non-routable endpoints so any eager discovery request would fail the test.
	if err != nil {                                                                                                                          // Reject construction failures because remote JWKS retrieval should remain lazy until token verification.
		t.Fatalf("expected explicit JWKS validator construction to succeed, got %v", err) // Surface accidental issuer-discovery calls or invalid override wiring.
	}
	if validator == nil || validator.verifier == nil { // Require a usable verifier even though no provider discovery object is created.
		t.Fatalf("expected explicit JWKS verifier, got %#v", validator) // Surface incomplete construction directly.
	}
	if validator.provider != nil { // Require the override path to bypass discovery rather than silently ignoring the supplied endpoint.
		t.Fatal("expected explicit JWKS path to omit discovery provider") // Surface regressions that would make the config field ineffective.
	}
}
