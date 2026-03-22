// auth_test.go verifies gateway auth routing rules such as conflict rejection and strict bearer-token classification.
package middleware

import (
	"context"
	"net/http/httptest"
	"testing"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/errors"
)

// fakePATValidator implements the PAT validator test double used to verify strict PAT routing behavior.
type fakePATValidator struct {
	validateCalls int
	subject       *auth.Subject
	err           error
}

// Validate records the PAT validation attempt and returns the configured result.
func (f *fakePATValidator) Validate(_ context.Context, _ string) (*auth.Subject, error) {
	f.validateCalls++       // Record that the PAT path was selected so the test can assert strict bearer-token routing.
	return f.subject, f.err // Return the configured PAT validation result so the middleware test controls the downstream outcome.
}

// fakeOIDCValidator implements the OIDC validator test double used to verify strict JWT routing behavior.
type fakeOIDCValidator struct {
	validateCalls int
	subject       *auth.Subject
	err           error
}

// Validate records the OIDC validation attempt and returns the configured result.
func (f *fakeOIDCValidator) Validate(_ context.Context, _ string) (*auth.Subject, error) {
	f.validateCalls++       // Record that the OIDC path was selected so the test can assert strict bearer-token routing.
	return f.subject, f.err // Return the configured OIDC validation result so the middleware test controls the downstream outcome.
}

// TestAuthMiddlewareRejectsConflictingCredentialSets verifies that requests carrying HMAC material and a bearer credential together are rejected immediately.
func TestAuthMiddlewareRejectsConflictingCredentialSets(t *testing.T) {
	middleware := &AuthMiddleware{}                                   // Construct the middleware without validators because the conflict should be rejected before any validator is consulted.
	request := httptest.NewRequest("GET", "/jsonrpc", nil)            // Create one request against a protected route so the middleware can inspect its auth headers.
	request.Header.Set("Authorization", "Bearer mcp_v1_token_secret") // Attach one PAT-shaped bearer token so the request carries a bearer credential set.
	request.Header.Set("X-MCP-Signature", "sig")                      // Attach the required HMAC headers so the request simultaneously carries HMAC auth material.
	request.Header.Set("X-MCP-Timestamp", "1")                        // Attach the HMAC timestamp header expected by the request classifier.
	request.Header.Set("X-MCP-Nonce", "nonce")                        // Attach the HMAC nonce header expected by the request classifier.
	request.Header.Set("X-MCP-Key-Id", "key")                         // Attach the HMAC key ID header expected by the request classifier.

	_, err := middleware.authenticate(request)            // Authenticate the conflicting request so the multi-scheme conflict logic is exercised directly.
	if !errors.IsCode(err, errors.CodePermissionDenied) { // Fail the test when the request is not classified as a credential-conflict error.
		t.Fatalf("expected conflicting credentials to return %q, got %v", errors.CodePermissionDenied, err) // Surface the unexpected error classification together with the returned error.
	}
}

// TestAuthMiddlewareRoutesPATByPrefix verifies that PAT-shaped bearer tokens route only into the PAT validator and never fall back to OIDC.
func TestAuthMiddlewareRoutesPATByPrefix(t *testing.T) {
	patValidator := &fakePATValidator{subject: &auth.Subject{ID: "pat-subject", Type: "pat", TenantID: "tenant-1"}}                 // Configure the PAT validator to succeed so the middleware can return its subject directly.
	oidcValidator := &fakeOIDCValidator{err: errors.New(errors.CodeInternal, "oidc validator should not be called for PAT tokens")} // Configure the OIDC validator with a sentinel error so the test would fail clearly if PAT routing leaked into OIDC.
	middleware := &AuthMiddleware{patValidator: patValidator, oidcValidator: oidcValidator}                                         // Build the middleware with both validators so bearer routing must choose one path explicitly.
	request := httptest.NewRequest("GET", "/jsonrpc", nil)                                                                          // Create one bearer-authenticated request against a protected route.
	request.Header.Set("Authorization", "Bearer mcp_v1_token_secret")                                                               // Attach one PAT-shaped bearer token so the middleware should select the PAT validator exclusively.

	subject, err := middleware.authenticate(request) // Authenticate the PAT-shaped bearer token through the middleware route classifier.
	if err != nil {                                  // Fail the test when strict PAT routing unexpectedly rejects the request.
		t.Fatalf("expected PAT bearer token to authenticate successfully, got error: %v", err) // Surface the unexpected PAT authentication failure together with the returned error.
	}
	if patValidator.validateCalls != 1 { // Fail the test when the PAT validator was not called exactly once for the PAT-shaped bearer token.
		t.Fatalf("expected PAT validator to be called once, got %d", patValidator.validateCalls) // Surface the unexpected PAT validator call count so routing drift is obvious.
	}
	if oidcValidator.validateCalls != 0 { // Fail the test when the OIDC validator was called for a PAT-shaped bearer token.
		t.Fatalf("expected OIDC validator to remain unused for PAT bearer token, got %d calls", oidcValidator.validateCalls) // Surface the unexpected OIDC validator call count so routing drift is obvious.
	}
	if subject == nil || subject.ID != "pat-subject" { // Fail the test when the middleware does not return the PAT validator's authenticated subject.
		t.Fatalf("expected PAT subject pat-subject, got %#v", subject) // Surface the unexpected returned subject so routing drift is easy to diagnose.
	}
}

// TestAuthMiddlewareRoutesJWTToOIDC verifies that JWT-shaped bearer tokens route only into the OIDC validator and never fall back to PAT.
func TestAuthMiddlewareRoutesJWTToOIDC(t *testing.T) {
	patValidator := &fakePATValidator{err: errors.New(errors.CodeInternal, "pat validator should not be called for JWT tokens")} // Configure the PAT validator with a sentinel error so the test would fail clearly if JWT routing leaked into PAT.
	oidcValidator := &fakeOIDCValidator{subject: &auth.Subject{ID: "oidc-subject", Type: "oidc", TenantID: "tenant-2"}}          // Configure the OIDC validator to succeed so the middleware can return its subject directly.
	middleware := &AuthMiddleware{patValidator: patValidator, oidcValidator: oidcValidator}                                      // Build the middleware with both validators so bearer routing must choose one path explicitly.
	request := httptest.NewRequest("GET", "/jsonrpc", nil)                                                                       // Create one bearer-authenticated request against a protected route.
	request.Header.Set("Authorization", "Bearer header.payload.signature")                                                       // Attach one JWT-shaped bearer token so the middleware should select the OIDC validator exclusively.

	subject, err := middleware.authenticate(request) // Authenticate the JWT-shaped bearer token through the middleware route classifier.
	if err != nil {                                  // Fail the test when strict JWT routing unexpectedly rejects the request.
		t.Fatalf("expected JWT bearer token to authenticate successfully, got error: %v", err) // Surface the unexpected OIDC authentication failure together with the returned error.
	}
	if oidcValidator.validateCalls != 1 { // Fail the test when the OIDC validator was not called exactly once for the JWT-shaped bearer token.
		t.Fatalf("expected OIDC validator to be called once, got %d", oidcValidator.validateCalls) // Surface the unexpected OIDC validator call count so routing drift is obvious.
	}
	if patValidator.validateCalls != 0 { // Fail the test when the PAT validator was called for a JWT-shaped bearer token.
		t.Fatalf("expected PAT validator to remain unused for JWT bearer token, got %d calls", patValidator.validateCalls) // Surface the unexpected PAT validator call count so routing drift is obvious.
	}
	if subject == nil || subject.ID != "oidc-subject" { // Fail the test when the middleware does not return the OIDC validator's authenticated subject.
		t.Fatalf("expected OIDC subject oidc-subject, got %#v", subject) // Surface the unexpected returned subject so routing drift is easy to diagnose.
	}
}
