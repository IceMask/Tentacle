// auth.go implements the gateway HTTP authentication middleware that routes requests by credential type and rejects ambiguous auth attempts.
package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"mcp_for_appium/internal/audit"
	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/gateway/httpinput"
	"mcp_for_appium/internal/telemetry"

	"github.com/google/uuid"
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
	auditRecorder audit.Recorder
	logger        *slog.Logger
}

// NewAuthMiddleware constructs the gateway auth middleware when at least one validator has been configured.
func NewAuthMiddleware(hmac *auth.HMACValidator, oidc *auth.OIDCValidator, pat *auth.PATValidator) *AuthMiddleware {
	if hmac == nil && oidc == nil && pat == nil { // Return nil when no validators are configured so callers can keep unauthenticated routes unchanged by default.
		return nil // Signal to the caller that no auth wrapper is needed because there is no active credential source.
	}

	return &AuthMiddleware{hmacValidator: hmac, oidcValidator: oidc, patValidator: pat, logger: telemetry.Logger()} // Store the configured validators and logger once so every request shares the same authentication and audit diagnostics.
}

// SetAuditRecorder installs the append-only recorder used for authentication success and failure events.
func (m *AuthMiddleware) SetAuditRecorder(recorder audit.Recorder) {
	if m == nil { // Ignore optional wiring when authentication itself is disabled and no middleware was constructed.
		return // Keep startup wiring concise without dereferencing a nil middleware.
	}
	m.auditRecorder = recorder // Store the shared recorder so every protected request emits one best-effort authentication audit event.
}

// Handle wraps an HTTP handler with gateway request authentication.
func (m *AuthMiddleware) Handle(next http.Handler) http.Handler {
	if m == nil { // Preserve the original handler when auth is not configured because callers may choose not to protect a route.
		return next // Return the unwrapped handler so no unnecessary allocation or auth check occurs on each request.
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Build one auth-enforcing wrapper around the protected handler.
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID")) // Reuse a caller or upstream request identifier when supplied for end-to-end correlation.
		if requestID == "" {                                         // Generate a trustworthy local correlation identifier when the request omitted one.
			requestID = uuid.NewString() // Assign one random request identifier without deriving it from credentials or body content.
		}
		w.Header().Set("X-Request-ID", requestID) // Return the effective request identifier so callers can correlate responses, logs, and audit records.
		subject, err := m.authenticate(r)         // Resolve the caller identity before the protected handler is allowed to run.
		if err != nil {                           // Stop request processing when none of the configured auth schemes accept the request.
			m.recordAuthentication(r, requestID, nil, err) // Record the rejected attempt without storing bearer tokens, signatures, or raw backend errors.
			status := errors.MapToHTTP(err)                // Map typed authentication failures without exposing their wrapped backend causes.
			message := "authentication failed"             // Return one stable public diagnostic for ordinary credential failures.
			if httpinput.IsBodyTooLarge(err) {             // Preserve the request-size contract when HMAC body hashing encounters the byte ceiling.
				status = http.StatusRequestEntityTooLarge // Return HTTP 413 so clients can distinguish size rejection from invalid credentials.
				message = "request body too large"        // Explain the actionable transport constraint without reflecting request content.
			}
			http.Error(w, message, status) // Write only the sanitized public diagnostic and mapped transport status.
			return                         // Stop before the protected handler runs because the request has not been authenticated successfully.
		}
		m.recordAuthentication(r, requestID, subject, nil) // Record successful authentication before application dispatch under the resolved principal.

		auditContext := audit.WithRequestContext(r.Context(), audit.RequestContext{RequestID: requestID, SourceIP: audit.RequestSourceIP(r)}) // Attach sanitized request correlation for downstream audit producers.
		ctx := auth.WithSubject(auditContext, subject)                                                                                        // Attach the authenticated subject to the correlated request context so downstream handlers can read tenant and principal metadata.
		next.ServeHTTP(w, r.WithContext(ctx))                                                                                                 // Forward the authenticated request to the protected handler with the injected subject context.
	})
}

// recordAuthentication appends one sanitized authentication outcome without changing request success when the audit sink is unavailable.
func (m *AuthMiddleware) recordAuthentication(r *http.Request, requestID string, subject *auth.Subject, authErr error) {
	if m == nil || m.auditRecorder == nil { // Skip optional audit work when startup did not configure a recorder.
		return // Preserve authentication behavior in protocol-only tests and deployments without PostgreSQL wiring.
	}
	event := audit.Event{ // Build the complete sanitized authentication audit record before determining the outcome.
		ActorID:      "anonymous",                  // Attribute rejected pre-authentication attempts without inventing a principal.
		ActorType:    "anonymous",                  // Classify the pre-authentication actor explicitly for security queries.
		AuthScheme:   requestAuthScheme(r),         // Record only the classified credential scheme, never the credential value.
		CredentialID: requestCredentialID(r),       // Record a safe key or PAT identifier when it can be extracted without a secret.
		Action:       "auth.authenticate",          // Use one stable action for both successful and rejected protected requests.
		ResourceType: "http_route",                 // Classify the protected route as the resource under authentication.
		ResourceID:   r.URL.Path,                   // Record the requested path without query parameters that may carry subscription tokens.
		Result:       "failure",                    // Default to failure until a validated subject proves success below.
		Reason:       auditErrorReason(authErr),    // Record a stable internal code or generic marker without wrapped error text.
		SourceIP:     audit.RequestSourceIP(r),     // Record the directly observed peer address for investigation.
		RequestID:    strings.TrimSpace(requestID), // Correlate this audit row with response and telemetry identifiers.
	}
	if subject != nil && authErr == nil { // Populate authoritative principal fields only after one validator succeeds.
		event.ActorID = strings.TrimSpace(subject.ID)                // Record the authenticated principal identifier.
		event.ActorType = "subject"                                  // Classify authenticated callers uniformly while AuthScheme preserves credential type.
		event.AuthScheme = strings.TrimSpace(subject.Type)           // Prefer the validator-produced scheme over request-shape inference.
		event.CredentialID = strings.TrimSpace(subject.CredentialID) // Record the safe token or key identifier supplied by the validator.
		event.TenantID = strings.TrimSpace(subject.TenantID)         // Preserve the authenticated tenant scope for audit filtering.
		event.Result = "success"                                     // Mark the request as authenticated successfully.
		event.Reason = ""                                            // Keep successful authentication records free of failure reason text.
	}
	if err := m.auditRecorder.Record(r.Context(), event); err != nil { // Append the audit event without allowing recorder availability to bypass or reject authentication.
		logger := m.logger // Reuse the middleware logger when constructor wiring supplied one.
		if logger == nil { // Fall back to the current global logger for zero-value middleware instances used in tests.
			logger = telemetry.Logger() // Preserve observability for audit persistence failures without panicking.
		}
		logger.WarnContext(r.Context(), "failed to record authentication audit event", "request_id", requestID, "result", event.Result, "error", err) // Log the recorder failure without credential or raw request data.
	}
}

// requestAuthScheme classifies one request credential shape without validating or persisting the credential value.
func requestAuthScheme(r *http.Request) string {
	if r == nil { // Return unknown when no request metadata exists.
		return "unknown" // Avoid dereferencing nil in failure-path audit handling.
	}
	if hasAnyHMACHeaders(r) { // Classify every complete or partial HMAC attempt consistently with authentication routing.
		return "hmac" // Record only the scheme name without any signature material.
	}
	authorization := strings.TrimSpace(r.Header.Get("Authorization")) // Read the bearer transport only for shape classification.
	rawToken, err := parseBearerToken(authorization)                  // Reuse strict bearer parsing while keeping errors private.
	if err != nil {                                                   // Classify missing or malformed Authorization headers generically.
		return "unknown" // Avoid reflecting malformed header values into audit metadata.
	}
	if looksLikePAT(rawToken) { // Match the stable repository PAT prefix.
		return "pat" // Record the PAT scheme without persisting its secret.
	}
	if looksLikeJWT(rawToken) { // Match the compact three-segment JWT shape.
		return "oidc" // Record the OIDC scheme without parsing or persisting the raw JWT.
	}
	return "bearer" // Classify unsupported bearer formats without exposing their contents.
}

// requestCredentialID extracts only a safe HMAC key ID or PAT token ID for audit correlation.
func requestCredentialID(r *http.Request) string {
	if r == nil { // Return no credential identifier when request metadata is unavailable.
		return "" // Avoid dereferencing nil in failure-path audit handling.
	}
	if hasAnyHMACHeaders(r) { // Use the dedicated non-secret key identifier from HMAC attempts.
		return strings.TrimSpace(r.Header.Get("X-MCP-Key-Id")) // Never record the HMAC signature, nonce, or shared secret.
	}
	rawToken, err := parseBearerToken(strings.TrimSpace(r.Header.Get("Authorization"))) // Parse the bearer envelope without validating its secret.
	if err != nil || !looksLikePAT(rawToken) {                                          // Return no identifier for malformed bearer values and opaque JWTs.
		return "" // Avoid decoding or persisting any part of unsupported credentials.
	}
	tokenBody := strings.TrimPrefix(strings.TrimSpace(rawToken), "mcp_v1_") // Remove the fixed PAT prefix before locating the ID/secret separator.
	separatorIndex := strings.Index(tokenBody, "_")                         // Locate the first separator after the safe token identifier.
	if separatorIndex <= 0 {                                                // Reject malformed PAT bodies without a complete identifier.
		return "" // Avoid persisting an ambiguous or secret-bearing fragment.
	}
	return strings.TrimSpace(tokenBody[:separatorIndex]) // Return only the token ID segment before the secret.
}

// auditErrorReason returns one stable repository error code or a generic failure marker without exposing wrapped backend details.
func auditErrorReason(err error) string {
	if code, ok := errors.CodeOf(err); ok { // Prefer the repository's machine-readable error classification when available.
		return string(code) // Store only the stable code in the append-only audit record.
	}
	return "unclassified_failure" // Classify untyped failures without copying their error text.
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
	body, err := httpinput.ReadAndRestoreBody(r, httpinput.MaxRequestBodyBytes) // Read and restore a bounded request body so HMAC validation cannot consume unbounded memory.
	if err != nil {                                                             // Stop immediately when the request body cannot be read because signature validation would become unreliable.
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
