// auth.go implements the gateway HTTP authentication middleware that routes requests by credential type and rejects ambiguous auth attempts.
package middleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/errors"
)

// patValidator defines the PAT validation contract consumed by the HTTP auth middleware.
type patValidator interface {
	// Validate authenticates one PAT bearer token and returns the resulting subject.
	Validate(ctx context.Context, token string) (*auth.Subject, error)
}

// oidcValidator defines the OIDC validation contract consumed by the HTTP auth middleware.
type oidcValidator interface {
	// Validate authenticates one OIDC bearer token and returns the resulting subject.
	Validate(ctx context.Context, rawToken string) (*auth.Subject, error)
}

// hmacValidator defines the HMAC validation contract consumed by the HTTP auth middleware.
type hmacValidator interface {
	// Verify authenticates one HMAC-signed HTTP request and returns the resulting subject.
	Verify(ctx context.Context, sig string, method string, path string, rawQuery string, body string, ts string, nonce string, keyID string) (*auth.Subject, error)
}

// AuthMiddleware authenticates protected gateway HTTP requests by first classifying the credential type and then delegating to the matching validator.
type AuthMiddleware struct {
	patValidator  patValidator
	oidcValidator oidcValidator
	hmacValidator hmacValidator
}

// NewAuthMiddleware constructs the gateway auth middleware when at least one validator has been configured.
func NewAuthMiddleware(hmac *auth.HMACValidator, oidc *auth.OIDCValidator, pat *auth.PATValidator) *AuthMiddleware {
	if hmac == nil && oidc == nil && pat == nil { // Return nil when no validators are configured so callers can keep unauthenticated routes unchanged by default.
		return nil // Signal to the caller that no auth wrapper is needed because there is no active credential source.
	}

	return &AuthMiddleware{hmacValidator: hmac, oidcValidator: oidc, patValidator: pat} // Store the configured validators once so every request can authenticate against the same startup-built set.
}

// Handle wraps an HTTP handler with gateway request authentication.
func (m *AuthMiddleware) Handle(next http.Handler) http.Handler {
	if m == nil { // Preserve the original handler when auth is not configured because callers may choose not to protect a route.
		return next // Return the unwrapped handler so no unnecessary allocation or auth check occurs on each request.
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Build one auth-enforcing wrapper around the protected handler.
		subject, err := m.authenticate(r) // Resolve the caller identity before the protected handler is allowed to run.
		if err != nil {                   // Stop request processing when none of the configured auth schemes accept the request.
			http.Error(w, err.Error(), errors.MapToHTTP(err)) // Return a transport-appropriate auth status so clients can distinguish missing credentials from permission failures.
			return                                            // Stop before the protected handler runs because the request has not been authenticated successfully.
		}

		ctx := auth.WithSubject(r.Context(), subject) // Attach the authenticated subject to the request context so downstream handlers can read tenant and principal metadata.
		next.ServeHTTP(w, r.WithContext(ctx))         // Forward the authenticated request to the protected handler with the injected subject context.
	})
}

// authenticate resolves one request by classifying the presented credentials and delegating to the single matching validator.
func (m *AuthMiddleware) authenticate(r *http.Request) (*auth.Subject, error) {
	if m == nil { // Defend against accidental direct calls on a nil middleware even though Handle already guards this case.
		return nil, errors.New(errors.CodeInternal, "auth middleware is nil") // Surface the programmer error explicitly instead of panicking on nil field access.
	}
	if hasAnyHMACHeaders(r) && strings.TrimSpace(r.Header.Get("Authorization")) != "" { // Reject requests that present HMAC material and a bearer credential together because v4.4 forbids ambiguous multi-scheme auth.
		return nil, errors.New(errors.CodePermissionDenied, "multiple authentication schemes are not allowed") // Surface the conflicting credentials explicitly so callers remove the extra credential set.
	}
	if hasAnyHMACHeaders(r) { // Route every HMAC-marked request into the HMAC path so partial or malformed HMAC attempts fail deterministically without falling back to bearer auth.
		if m.hmacValidator == nil { // Reject HMAC-marked requests when the gateway has no active HMAC validator because silently ignoring the signature headers would be unsafe.
			return nil, errors.New(errors.CodeUnauthenticated, "hmac authentication is not enabled") // Surface the disabled HMAC path as an authentication failure instead of falling back to another scheme.
		}
		return m.authenticateHMAC(r) // Authenticate the signed HTTP request through the HMAC validator because the request shape selected that path already.
	}

	authHeader := strings.TrimSpace(r.Header.Get("Authorization")) // Read the Authorization header once because both OIDC and PAT share the same bearer token transport.
	if authHeader == "" {                                          // Reject missing credentials after skipping HMAC because none of the remaining auth schemes can proceed without a bearer token.
		return nil, errors.New(errors.CodeUnauthenticated, "missing authentication credentials") // Surface the absence of credentials as a standard auth failure.
	}
	rawToken, err := parseBearerToken(authHeader) // Parse the bearer token once so OIDC and PAT can both validate the same normalized credential value.
	if err != nil {                               // Stop immediately when the Authorization header does not use the expected Bearer scheme.
		return nil, err // Preserve the bearer-token parse error so the client can correct the header format.
	}

	if looksLikePAT(rawToken) { // Route PAT-shaped bearer tokens only into the PAT validator so OIDC and PAT never blind-fallback across the same credential.
		if m.patValidator == nil { // Reject PAT-shaped bearer tokens when PAT auth is not enabled because another validator must not reinterpret them.
			return nil, errors.New(errors.CodeUnauthenticated, "pat authentication is not enabled") // Surface the disabled PAT path as an authentication failure instead of attempting another validator.
		}
		return m.patValidator.Validate(r.Context(), rawToken) // Authenticate the PAT through the single matching PAT validator.
	}
	if looksLikeJWT(rawToken) { // Route JWT-shaped bearer tokens only into the OIDC validator so PAT and OIDC never blind-fallback across the same credential.
		if m.oidcValidator == nil { // Reject JWT-shaped bearer tokens when OIDC auth is not enabled because PAT must not reinterpret them.
			return nil, errors.New(errors.CodeUnauthenticated, "oidc authentication is not enabled") // Surface the disabled OIDC path as an authentication failure instead of attempting another validator.
		}
		return m.oidcValidator.Validate(r.Context(), rawToken) // Authenticate the JWT through the single matching OIDC validator.
	}

	return nil, errors.New(errors.CodeUnauthenticated, "unsupported bearer token format") // Reject unknown bearer token shapes because v4.4 routes bearer auth strictly by credential type.
}

// authenticateHMAC validates one HMAC-signed HTTP request while restoring the request body for downstream handlers.
func (m *AuthMiddleware) authenticateHMAC(r *http.Request) (*auth.Subject, error) {
	body, err := cloneRequestBody(r) // Read and restore the request body so HMAC validation can hash it without consuming it for downstream handlers.
	if err != nil {                  // Stop immediately when the request body cannot be read because signature validation would become unreliable.
		return nil, errors.Wrap(errors.CodeUnauthenticated, "failed to read request body for HMAC validation", err) // Surface the body-read failure as an authentication error because the request cannot be verified safely.
	}

	signature := strings.TrimSpace(r.Header.Get("X-MCP-Signature"))       // Read the signature header because the HMAC validator needs the caller-supplied digest.
	timestamp := strings.TrimSpace(r.Header.Get("X-MCP-Timestamp"))       // Read the timestamp header because replay protection depends on the signed request time.
	nonce := strings.TrimSpace(r.Header.Get("X-MCP-Nonce"))               // Read the nonce header because replay protection depends on one-time nonces.
	keyID := strings.TrimSpace(r.Header.Get("X-MCP-Key-Id"))              // Read the key ID header because the validator needs to resolve the correct shared secret.
	if signature == "" || timestamp == "" || nonce == "" || keyID == "" { // Reject partially supplied HMAC credentials because the request cannot be verified safely without every signed field.
		return nil, errors.New(errors.CodeUnauthenticated, "missing HMAC headers") // Surface the incomplete HMAC credential set as a standard auth failure.
	}

	return m.hmacValidator.Verify(r.Context(), signature, r.Method, r.URL.Path, r.URL.RawQuery, string(body), timestamp, nonce, keyID) // Delegate the actual signature, timestamp, nonce, and canonical-query validation to the configured HMAC validator.
}

// parseBearerToken extracts the opaque bearer token from an Authorization header value.
func parseBearerToken(authHeader string) (string, error) {
	if !strings.HasPrefix(authHeader, "Bearer ") { // Accept only the Bearer scheme because PAT and OIDC both rely on bearer tokens in this gateway.
		return "", errors.New(errors.CodeUnauthenticated, "authorization header must use Bearer") // Surface the unsupported scheme explicitly so clients can correct the header.
	}

	token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer ")) // Normalize the token payload after removing the bearer scheme prefix.
	if token == "" {                                                      // Reject empty bearer payloads because neither OIDC nor PAT can validate a missing token.
		return "", errors.New(errors.CodeUnauthenticated, "bearer token is empty") // Surface the missing token payload as a standard auth failure.
	}

	return token, nil // Return the normalized bearer token for OIDC and PAT validation.
}

// hasAnyHMACHeaders reports whether the request includes any HMAC authentication material.
func hasAnyHMACHeaders(r *http.Request) bool {
	return strings.TrimSpace(r.Header.Get("X-MCP-Signature")) != "" || strings.TrimSpace(r.Header.Get("X-MCP-Timestamp")) != "" || strings.TrimSpace(r.Header.Get("X-MCP-Nonce")) != "" || strings.TrimSpace(r.Header.Get("X-MCP-Key-Id")) != "" // Detect any HMAC credential material so HMAC retains priority whenever a signed request is attempted.
}

// looksLikePAT reports whether one bearer token matches the repository PAT wire format.
func looksLikePAT(rawToken string) bool {
	return strings.HasPrefix(strings.TrimSpace(rawToken), "mcp_v1_") // Detect PAT bearer tokens by their stable repository-managed prefix so PAT and OIDC routing never overlap.
}

// looksLikeJWT reports whether one bearer token resembles a compact JWT with three dot-delimited segments.
func looksLikeJWT(rawToken string) bool {
	parts := strings.Split(strings.TrimSpace(rawToken), ".") // Split the token on compact-JWT segment delimiters so the route classifier can inspect its basic wire shape.
	if len(parts) != 3 {                                     // Reject non-three-part tokens because compact JWTs always contain header, payload, and signature segments.
		return false // Report non-JWT shapes directly so PAT or unsupported-token routing can handle them instead.
	}
	for _, part := range parts { // Validate every JWT segment so obviously invalid tokens do not accidentally route into OIDC validation.
		if strings.TrimSpace(part) == "" { // Reject empty compact-JWT segments because valid JWTs must contain data in every segment.
			return false // Report malformed JWT shapes directly so the caller receives the correct unsupported-token error.
		}
	}

	return true // Report the three-segment non-empty token as JWT-shaped so the request routes into OIDC validation only.
}

// cloneRequestBody reads the current request body and replaces it with a fresh reader over the same bytes.
func cloneRequestBody(r *http.Request) ([]byte, error) {
	if r.Body == nil { // Treat nil bodies as empty so GET and body-less requests can still participate in HMAC verification deterministically.
		return nil, nil // Return an empty byte slice equivalent without mutating the request because there is no body reader to restore.
	}

	body, err := io.ReadAll(r.Body) // Read the original request body bytes so the HMAC validator can hash the exact payload sent by the client.
	if err != nil {                 // Stop when the body cannot be read because the signed payload would be ambiguous and unsafe to verify.
		return nil, err // Preserve the low-level body read failure for the caller so it can be wrapped appropriately.
	}
	r.Body = io.NopCloser(bytes.NewReader(body)) // Restore the request body so downstream handlers can parse the same bytes after HMAC verification succeeds.

	return body, nil // Return the cloned request body bytes to the caller for signature validation.
}
