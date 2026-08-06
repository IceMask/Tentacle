// subscription_token.go implements short-lived opaque WebSocket subscription tokens backed by Redis.
package websocket

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	stdErrors "errors"
	"log/slog"
	"strings"
	"time"

	"mcp_for_appium/internal/audit"
	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/telemetry"

	goredis "github.com/redis/go-redis/v9"
)

const defaultSubscriptionTokenTTL = 60 * time.Second // defaultSubscriptionTokenTTL bounds browser subscription-token lifetime when the caller does not configure a custom TTL.

// SubscriptionTokenClaims stores the authoritative WebSocket subscription metadata associated with one opaque token.
type SubscriptionTokenClaims struct {
	SubjectID string    `json:"sub"`
	TenantID  string    `json:"tenant_id"`
	TraceID   string    `json:"trace_id"`
	ExpiresAt time.Time `json:"exp"`
	JTI       string    `json:"jti"`
	Scope     string    `json:"scope"`
}

// IssuedSubscriptionToken stores the response payload returned to callers after issuing one short-lived WebSocket subscription token.
type IssuedSubscriptionToken struct {
	Token     string
	TraceID   string
	ExpiresAt time.Time
	Scope     string
}

// SubscriptionTokenStore issues and resolves short-lived opaque WebSocket subscription tokens backed by Redis.
type SubscriptionTokenStore struct {
	cache         *redis.Cache
	ttl           time.Duration
	auditRecorder audit.Recorder
	logger        *slog.Logger
}

// NewSubscriptionTokenStore constructs one Redis-backed subscription-token store.
func NewSubscriptionTokenStore(cache *redis.Cache, ttl time.Duration) *SubscriptionTokenStore {
	if ttl <= 0 { // Fill in the default token TTL when the caller leaves it unset so issued tokens always expire promptly.
		ttl = defaultSubscriptionTokenTTL // Preserve the repository default browser subscription-token lifetime for callers that do not configure a custom value.
	}

	return &SubscriptionTokenStore{cache: cache, ttl: ttl, logger: telemetry.Logger()} // Store cache, TTL, and logger once so token operations and audit failures share one runtime contract.
}

// SetAuditRecorder installs the append-only recorder used for subscription-token lifecycle and WebSocket authorization events.
func (s *SubscriptionTokenStore) SetAuditRecorder(recorder audit.Recorder) {
	if s == nil { // Ignore optional wiring when no token store was constructed.
		return // Avoid dereferencing nil during gateway startup or protocol-only tests.
	}
	s.auditRecorder = recorder // Share the gateway's PostgreSQL audit sink with token issuance and handshake handling.
}

// Issue creates one short-lived opaque token bound to the supplied authenticated subject and trace identifier.
func (s *SubscriptionTokenStore) Issue(ctx context.Context, subject *auth.Subject, traceID string) (*IssuedSubscriptionToken, error) {
	if s == nil || s.cache == nil { // Reject calls on an unconfigured store because token issuance requires a shared authoritative backing store.
		return nil, errors.New(errors.CodeInternal, "websocket subscription token store is not configured") // Surface the missing token store as a startup wiring failure rather than panicking.
	}
	if subject == nil { // Reject missing authenticated subjects because subscription tokens must always be bound to a caller identity.
		return nil, errors.New(errors.CodeUnauthenticated, "authenticated subject is required") // Surface the missing subject as an authentication failure instead of issuing an anonymous token.
	}
	if strings.TrimSpace(subject.ID) == "" { // Reject subjects without a stable identifier because later audit and ownership checks must be able to attribute the issued token to one principal.
		return nil, errors.New(errors.CodeUnauthenticated, "authenticated subject id is required") // Surface the missing subject identifier as an authentication failure instead of issuing an unauditable token.
	}

	normalizedTraceID := strings.TrimSpace(traceID) // Normalize the requested trace ID so empty and whitespace-only values behave consistently.
	if normalizedTraceID == "" {                    // Reject missing trace IDs because every subscription token must bind to exactly one trace.
		return nil, errors.New(errors.CodePlanInvalid, "traceId is required") // Surface the missing trace ID as a request-validation failure.
	}

	token, err := generateOpaqueToken() // Generate one random opaque token so the browser never sees a long-lived or semantically meaningful credential.
	if err != nil {                     // Stop immediately when secure random token generation fails because the subscription token would otherwise be predictable.
		return nil, errors.Wrap(errors.CodeInternal, "failed to generate websocket subscription token", err) // Surface secure random failures as internal server errors.
	}
	jti, err := generateOpaqueToken() // Generate one independent token identifier so audits and logs can reference the issued subscription without exposing the token value itself.
	if err != nil {                   // Stop immediately when secure random token generation fails because the subscription token metadata would otherwise be incomplete.
		return nil, errors.Wrap(errors.CodeInternal, "failed to generate websocket subscription token id", err) // Surface secure random failures as internal server errors.
	}

	expiresAt := time.Now().UTC().Add(s.ttl)                                                                                                                                                                           // Compute the token expiry once so the Redis TTL and returned response payload stay consistent.
	claims := SubscriptionTokenClaims{SubjectID: strings.TrimSpace(subject.ID), TenantID: strings.TrimSpace(subject.TenantID), TraceID: normalizedTraceID, ExpiresAt: expiresAt, JTI: jti, Scope: "trace.events.read"} // Build the authoritative claim set stored behind the opaque token.
	serializedClaims, err := json.Marshal(claims)                                                                                                                                                                      // Serialize the claims once so the Redis-backed token store can return them deterministically during the WebSocket handshake.
	if err != nil {                                                                                                                                                                                                    // Stop immediately when the claims cannot be serialized because the subscription token would otherwise be unreadable at handshake time.
		return nil, errors.Wrap(errors.CodeInternal, "failed to serialize websocket subscription token", err) // Surface JSON serialization failures as internal server errors.
	}
	if err := s.cache.Set(ctx, subscriptionTokenKey(token), string(serializedClaims), s.ttl); err != nil { // Persist the opaque token claims with the configured TTL so every gateway instance that shares Redis can validate the token during the handshake.
		s.recordTokenAudit(ctx, subject, claims, "failure", string(errors.CodeStoreWrite))                  // Record the failed creation without persisting the opaque token or wrapped Redis diagnostic.
		return nil, errors.Wrap(errors.CodeStoreWrite, "failed to store websocket subscription token", err) // Surface Redis persistence failures as storage write errors because the authoritative token store is unavailable.
	}

	s.recordTokenAudit(ctx, subject, claims, "success", "")                                                                   // Record successful token creation using only the safe JTI, trace binding, scope, and expiry.
	return &IssuedSubscriptionToken{Token: token, TraceID: normalizedTraceID, ExpiresAt: expiresAt, Scope: claims.Scope}, nil // Return the opaque token payload so the caller can establish the WebSocket subscription before it expires.
}

// recordTokenAudit appends one sanitized subscription-token creation event without changing issuance when audit persistence is unavailable.
func (s *SubscriptionTokenStore) recordTokenAudit(ctx context.Context, subject *auth.Subject, claims SubscriptionTokenClaims, result string, reason string) {
	if s == nil || s.auditRecorder == nil { // Skip optional audit work when the gateway has no recorder wiring.
		return // Preserve token behavior in isolated tests and deployments without PostgreSQL audit persistence.
	}
	requestContext, _ := audit.RequestContextFrom(ctx) // Reuse source IP and request ID attached by authentication middleware when available.
	event := audit.Event{                              // Build one sanitized append-only token lifecycle record.
		ActorID:      strings.TrimSpace(subject.ID),               // Attribute token creation to the authenticated subject.
		ActorType:    "subject",                                   // Classify the actor as an authenticated gateway principal.
		AuthScheme:   strings.TrimSpace(subject.Type),             // Preserve PAT, OIDC, or HMAC authentication provenance.
		CredentialID: strings.TrimSpace(subject.CredentialID),     // Record only the safe parent credential identifier.
		TenantID:     strings.TrimSpace(subject.TenantID),         // Preserve tenant scope for audit filtering.
		TraceID:      strings.TrimSpace(claims.TraceID),           // Link the record to the authorized trace foreign key.
		Action:       "ws.subscription_token.issue",               // Use the stable WebSocket token creation action.
		ResourceType: "trace",                                     // Identify the trace as the resource receiving subscription authority.
		ResourceID:   strings.TrimSpace(claims.TraceID),           // Preserve the exact authorized trace identifier.
		Result:       strings.TrimSpace(result),                   // Record success or failure without backend details.
		Reason:       strings.TrimSpace(reason),                   // Record a stable error code only on failure.
		SourceIP:     strings.TrimSpace(requestContext.SourceIP),  // Correlate the creation with the directly observed gateway peer.
		RequestID:    strings.TrimSpace(requestContext.RequestID), // Correlate the creation with the authenticated HTTP request.
		Metadata: map[string]interface{}{ // Preserve safe token metadata without storing the opaque credential itself.
			"jti":       strings.TrimSpace(claims.JTI),   // Record the independent token identifier for investigations and revocation analysis.
			"scope":     strings.TrimSpace(claims.Scope), // Record the granted WebSocket subscription scope.
			"expiresAt": claims.ExpiresAt.UTC(),          // Record the short-lived credential expiry.
		},
	}
	if err := s.auditRecorder.Record(ctx, event); err != nil { // Append best-effort without turning an audit sink outage into token behavior changes.
		logger := s.logger // Reuse the store logger initialized by the production constructor.
		if logger == nil { // Fall back for zero-value test stores that inject only a recorder.
			logger = telemetry.Logger() // Preserve audit failure observability without panicking.
		}
		logger.WarnContext(ctx, "failed to record websocket token audit event", "trace_id", claims.TraceID, "result", result, "error", err) // Log no opaque token or request secret.
	}
}

// Lookup resolves one opaque subscription token to its authoritative claims and rejects expired or malformed entries.
func (s *SubscriptionTokenStore) Lookup(ctx context.Context, rawToken string) (*SubscriptionTokenClaims, error) {
	if s == nil || s.cache == nil { // Reject calls on an unconfigured store because handshake validation requires a shared authoritative backing store.
		return nil, errors.New(errors.CodeInternal, "websocket subscription token store is not configured") // Surface the missing token store as a startup wiring failure rather than panicking.
	}

	normalizedToken := strings.TrimSpace(rawToken) // Normalize surrounding whitespace so empty and whitespace-only token values behave consistently.
	if normalizedToken == "" {                     // Reject missing token values because the handshake cannot authorize a trace subscription without one.
		return nil, errors.New(errors.CodeUnauthenticated, "subscription token is required") // Surface the missing token as a standard authentication failure.
	}

	serializedClaims, err := s.cache.Get(ctx, subscriptionTokenKey(normalizedToken)) // Resolve the opaque token against the authoritative Redis-backed token store.
	if err != nil {                                                                  // Stop immediately when the token cannot be found or the token store is unavailable.
		if stdErrors.Is(err, goredis.Nil) { // Distinguish an unknown credential from an unavailable authoritative token store.
			return nil, errors.New(errors.CodeUnauthenticated, "subscription token not found") // Reject unknown tokens without reflecting Redis sentinel text.
		}
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to read websocket subscription token", err) // Preserve infrastructure failures for HTTP 5xx mapping and operator diagnosis.
	}

	var claims SubscriptionTokenClaims                                        // Allocate the destination claims struct once so the stored JSON can be decoded deterministically.
	if err := json.Unmarshal([]byte(serializedClaims), &claims); err != nil { // Reject malformed stored claims because the token store entry is unreadable or corrupted.
		return nil, errors.Wrap(errors.CodeStoreRead, "stored websocket subscription token is invalid", err) // Classify corrupted authoritative state as a server storage failure rather than a caller authentication mistake.
	}
	if time.Now().UTC().After(claims.ExpiresAt.UTC()) { // Reject expired subscription tokens even if Redis has not yet evicted the record.
		return &claims, errors.New(errors.CodeTokenExpired, "subscription token expired") // Return safe stored claims for audit attribution while still rejecting the expired credential.
	}

	return &claims, nil // Return the authoritative claims so the handshake can bind the WebSocket connection to the authorized trace subscription.
}

// subscriptionTokenKey builds the Redis key used to store one opaque WebSocket subscription token.
func subscriptionTokenKey(token string) string {
	return "ws_subscription_token:" + token // Namespace WebSocket subscription tokens so they cannot collide with other Redis-backed gateway data.
}

// generateOpaqueToken returns one randomly generated lowercase-hex token suitable for use as an opaque subscription token or token identifier.
func generateOpaqueToken() (string, error) {
	randomBytes := make([]byte, 32)                   // Allocate 32 random bytes so the opaque token has 256 bits of entropy before hex encoding.
	if _, err := rand.Read(randomBytes); err != nil { // Fill the token buffer with cryptographically secure random bytes so the resulting token cannot be predicted.
		return "", err // Preserve the secure-random failure so the caller can wrap it into an internal error.
	}

	return hex.EncodeToString(randomBytes), nil // Return the lowercase-hex token so the browser can transport it safely in a query parameter.
}
