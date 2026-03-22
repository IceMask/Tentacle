// subscription_token.go implements short-lived opaque WebSocket subscription tokens backed by Redis.
package websocket

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/redis"
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
	cache *redis.Cache
	ttl   time.Duration
}

// NewSubscriptionTokenStore constructs one Redis-backed subscription-token store.
func NewSubscriptionTokenStore(cache *redis.Cache, ttl time.Duration) *SubscriptionTokenStore {
	if ttl <= 0 { // Fill in the default token TTL when the caller leaves it unset so issued tokens always expire promptly.
		ttl = defaultSubscriptionTokenTTL // Preserve the repository default browser subscription-token lifetime for callers that do not configure a custom value.
	}

	return &SubscriptionTokenStore{cache: cache, ttl: ttl} // Store the cache dependency and TTL once so every issue or lookup operation follows the same runtime contract.
}

// Issue creates one short-lived opaque token bound to the supplied authenticated subject and trace identifier.
func (s *SubscriptionTokenStore) Issue(ctx context.Context, subject *auth.Subject, traceID string) (*IssuedSubscriptionToken, error) {
	if s == nil || s.cache == nil { // Reject calls on an unconfigured store because token issuance requires a shared authoritative backing store.
		return nil, errors.New(errors.CodeInternal, "websocket subscription token store is not configured") // Surface the missing token store as a startup wiring failure rather than panicking.
	}
	if subject == nil { // Reject missing authenticated subjects because subscription tokens must always be bound to a caller identity.
		return nil, errors.New(errors.CodeUnauthenticated, "authenticated subject is required") // Surface the missing subject as an authentication failure instead of issuing an anonymous token.
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
		return nil, errors.Wrap(errors.CodeStoreWrite, "failed to store websocket subscription token", err) // Surface Redis persistence failures as storage write errors because the authoritative token store is unavailable.
	}

	return &IssuedSubscriptionToken{Token: token, TraceID: normalizedTraceID, ExpiresAt: expiresAt, Scope: claims.Scope}, nil // Return the opaque token payload so the caller can establish the WebSocket subscription before it expires.
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
		return nil, errors.Wrap(errors.CodeUnauthenticated, "subscription token not found", err) // Surface unknown tokens as authentication failures without revealing whether a trace exists.
	}

	var claims SubscriptionTokenClaims                                        // Allocate the destination claims struct once so the stored JSON can be decoded deterministically.
	if err := json.Unmarshal([]byte(serializedClaims), &claims); err != nil { // Reject malformed stored claims because the token store entry is unreadable or corrupted.
		return nil, errors.Wrap(errors.CodeUnauthenticated, "subscription token is invalid", err) // Surface malformed token-store entries as authentication failures.
	}
	if time.Now().UTC().After(claims.ExpiresAt.UTC()) { // Reject expired subscription tokens even if Redis has not yet evicted the record.
		return nil, errors.New(errors.CodeTokenExpired, "subscription token expired") // Surface token expiry distinctly so callers can request a fresh token.
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
