// auth_test.go verifies gateway auth routing rules such as conflict rejection and strict bearer-token classification.
package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mcp_for_appium/internal/audit"
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

// TestAuthMiddlewareRecordsSanitizedSuccess verifies that successful protected requests emit attributable audit metadata without bearer secrets.
func TestAuthMiddlewareRecordsSanitizedSuccess(t *testing.T) {
	const rawToken = "mcp_v1_token-safe_super-secret-value"                             // Build one PAT whose safe identifier and secret can be distinguished in audit assertions.
	var recordedEvents []audit.Event                                                    // Collect middleware audit records through an in-memory recorder.
	recorder := audit.RecorderFunc(func(ctx context.Context, event audit.Event) error { // Adapt one deterministic callback into the production recorder contract.
		recordedEvents = append(recordedEvents, event) // Preserve the exact event emitted before downstream request dispatch.
		return nil                                     // Keep audit persistence successful so authentication behavior remains the focus.
	})
	patValidator := &fakePATValidator{subject: &auth.Subject{ID: "subject-audit", Type: "pat", TenantID: "tenant-audit", CredentialID: "token-safe"}} // Return one fully attributable authenticated subject.
	middleware := &AuthMiddleware{patValidator: patValidator, auditRecorder: recorder}                                                                // Construct the middleware with strict PAT routing and the in-memory audit sink.
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {                                                                           // Verify authenticated and correlated context reaches the protected handler.
		if subject, ok := auth.SubjectFrom(r.Context()); !ok || subject.ID != "subject-audit" { // Require the validator-produced subject downstream.
			t.Fatalf("expected authenticated subject in downstream context, got %#v", subject) // Surface context propagation regression.
		}
		if requestContext, ok := audit.RequestContextFrom(r.Context()); !ok || requestContext.RequestID != "request-audit-success" { // Require audit correlation metadata downstream.
			t.Fatalf("expected request audit context downstream, got %#v", requestContext) // Surface missing request correlation.
		}
		w.WriteHeader(http.StatusNoContent) // Mark successful protected-handler execution without a response body.
	})
	request := httptest.NewRequest(http.MethodGet, "/jsonrpc?ignored=secret", nil) // Build one protected request whose query must not appear as the audited resource id.
	request.Header.Set("Authorization", "Bearer "+rawToken)                        // Present the PAT through the standard bearer transport.
	request.Header.Set("X-Request-ID", "request-audit-success")                    // Supply one deterministic correlation id for assertions.
	request.RemoteAddr = "192.0.2.10:4321"                                         // Supply one deterministic directly observed peer address.
	response := httptest.NewRecorder()                                             // Capture the protected response and effective request id.
	middleware.Handle(next).ServeHTTP(response, request)                           // Execute authentication, audit recording, and downstream context propagation.
	if response.Code != http.StatusNoContent {                                     // Require successful authentication to reach the protected handler.
		t.Fatalf("expected HTTP 204, got %d with body %s", response.Code, response.Body.String()) // Surface unexpected auth rejection.
	}
	if len(recordedEvents) != 1 { // Require exactly one authentication outcome per protected request.
		t.Fatalf("expected one authentication audit event, got %d", len(recordedEvents)) // Surface missing or duplicated audit writes.
	}
	event := recordedEvents[0]                                                                                                                 // Read the sole success event for field-level assertions.
	if event.Result != "success" || event.ActorID != "subject-audit" || event.CredentialID != "token-safe" || event.ResourceID != "/jsonrpc" { // Require principal, safe credential id, and query-free route attribution.
		t.Fatalf("unexpected authentication success audit event: %#v", event) // Surface malformed attribution without hiding fields.
	}
	serializedEvent, err := json.Marshal(event) // Serialize the complete event so every string field can be checked for secret leakage.
	if err != nil {                             // Fail when the audit event cannot be serialized for persistence.
		t.Fatalf("failed to serialize authentication audit event: %v", err) // Surface incompatible metadata immediately.
	}
	if strings.Contains(string(serializedEvent), "super-secret-value") || strings.Contains(string(serializedEvent), rawToken) { // Reject full or suffix credential disclosure anywhere in the event.
		t.Fatalf("authentication audit event leaked PAT secret: %s", serializedEvent) // Surface the unsafe event payload for diagnosis.
	}
}

// TestAuthMiddlewareRecordsSanitizedFailure verifies that rejected credentials emit only a stable code and generic public response.
func TestAuthMiddlewareRecordsSanitizedFailure(t *testing.T) {
	const backendSecret = "postgres-password=do-not-expose"                             // Embed a sentinel secret in the validator error to detect unsafe propagation.
	var recordedEvent audit.Event                                                       // Capture the sole rejected authentication event.
	recorder := audit.RecorderFunc(func(ctx context.Context, event audit.Event) error { // Adapt one deterministic callback into the recorder contract.
		recordedEvent = event // Preserve the exact failure event emitted by middleware.
		return nil            // Keep audit persistence successful so sanitization remains the focus.
	})
	patValidator := &fakePATValidator{err: errors.New(errors.CodeUnauthenticated, backendSecret)} // Return one typed failure whose raw message must stay private.
	middleware := &AuthMiddleware{patValidator: patValidator, auditRecorder: recorder}            // Construct the protected middleware with the failing validator and audit sink.
	request := httptest.NewRequest(http.MethodGet, "/mcp", nil)                                   // Build one protected request for rejected PAT authentication.
	request.Header.Set("Authorization", "Bearer mcp_v1_token-failure_secret-material")            // Present a PAT with a safe token identifier and private secret.
	response := httptest.NewRecorder()                                                            // Capture the sanitized public authentication rejection.
	middleware.Handle(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {             // Install a handler that must never run after authentication failure.
		t.Fatal("expected rejected authentication not to reach protected handler") // Fail immediately if middleware bypasses the validator error.
	})).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || strings.Contains(response.Body.String(), backendSecret) { // Require stable status and no backend diagnostic disclosure.
		t.Fatalf("expected sanitized HTTP 401, got %d with body %s", response.Code, response.Body.String()) // Surface status or leakage regression.
	}
	if recordedEvent.Result != "failure" || recordedEvent.Reason != string(errors.CodeUnauthenticated) || recordedEvent.CredentialID != "token-failure" { // Require stable failure classification and safe PAT id extraction.
		t.Fatalf("unexpected authentication failure audit event: %#v", recordedEvent) // Surface malformed failure attribution.
	}
	serializedEvent, err := json.Marshal(recordedEvent) // Serialize every audit field for sentinel leakage detection.
	if err != nil {                                     // Fail when the event cannot be serialized for persistence.
		t.Fatalf("failed to serialize authentication failure audit event: %v", err) // Surface incompatible event metadata.
	}
	if strings.Contains(string(serializedEvent), backendSecret) || strings.Contains(string(serializedEvent), "secret-material") { // Reject validator diagnostics and PAT secrets anywhere in the audit row.
		t.Fatalf("authentication failure audit event leaked secret data: %s", serializedEvent) // Surface the unsafe payload directly.
	}
}
