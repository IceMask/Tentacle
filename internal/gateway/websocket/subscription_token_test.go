// subscription_token_test.go verifies WebSocket subscription-token issuance, lookup, and trace-binding validation.
package websocket

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mcp_for_appium/internal/audit"
	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	storageredis "mcp_for_appium/internal/storage/redis"

	"github.com/alicebob/miniredis/v2"
)

// newSubscriptionTokenStoreForTest constructs one Redis-backed subscription-token store over a disposable in-memory Redis server.
func newSubscriptionTokenStoreForTest(t *testing.T) (*SubscriptionTokenStore, func()) {
	t.Helper()                                                                        // Mark this helper so any failure points at the calling test rather than the helper body.
	redisServer := miniredis.RunT(t)                                                  // Start one in-memory Redis server so the token store uses real Redis semantics without external dependencies.
	cache, err := storageredis.NewCache(config.RedisConfig{Addr: redisServer.Addr()}) // Construct the production Redis cache wrapper against the in-memory Redis server.
	if err != nil {                                                                   // Stop immediately when the production cache wrapper cannot connect to the in-memory Redis server.
		t.Fatalf("failed to construct redis cache for websocket subscription-token test: %v", err) // Surface the unexpected cache construction failure together with the returned error.
	}
	store := NewSubscriptionTokenStore(cache, 60*time.Second) // Construct the production subscription-token store over the disposable Redis cache.
	return store, func() {                                    // Return the store together with a cleanup closure that releases every disposable dependency.
		_ = cache.Close()   // Close the Redis client so no background connections remain after the test.
		redisServer.Close() // Stop the in-memory Redis server so the disposable TCP listener is released immediately.
	}
}

// TestSubscriptionTokenStoreIssueAndLookup verifies that issued opaque tokens resolve back to the authoritative subject and trace claims stored in Redis.
func TestSubscriptionTokenStoreIssueAndLookup(t *testing.T) {
	store, cleanup := newSubscriptionTokenStoreForTest(t) // Construct one disposable Redis-backed token store for the round-trip issuance and lookup test.
	defer cleanup()                                       // Release the Redis client and in-memory Redis server after the test completes.

	subject := &auth.Subject{ID: "subject-1", TenantID: "tenant-1", Type: "pat"} // Build one authenticated subject whose identity should be preserved inside the issued token claims.
	issuedToken, err := store.Issue(context.Background(), subject, "trace-1")    // Issue one short-lived opaque token bound to the authenticated subject and target trace.
	if err != nil {                                                              // Fail the test when token issuance unexpectedly fails.
		t.Fatalf("expected subscription token issuance to succeed, got error: %v", err) // Surface the unexpected issuance failure together with the returned error.
	}
	claims, err := store.Lookup(context.Background(), issuedToken.Token) // Resolve the issued token back through the production Redis-backed token lookup path.
	if err != nil {                                                      // Fail the test when token lookup unexpectedly fails for a freshly issued token.
		t.Fatalf("expected subscription token lookup to succeed, got error: %v", err) // Surface the unexpected lookup failure together with the returned error.
	}
	if claims.SubjectID != "subject-1" || claims.TenantID != "tenant-1" || claims.TraceID != "trace-1" { // Fail the test when the lookup does not preserve the issued subject and trace binding.
		t.Fatalf("expected claims subject-1/tenant-1/trace-1, got %#v", claims) // Surface the unexpected claim set so token-store drift is easy to diagnose.
	}
}

// TestSubscriptionTokenStoreAuditOmitsOpaqueToken verifies that issuance audit records use JTI and never persist the browser credential itself.
func TestSubscriptionTokenStoreAuditOmitsOpaqueToken(t *testing.T) {
	store, cleanup := newSubscriptionTokenStoreForTest(t)                                          // Construct one disposable Redis-backed token store for audited issuance.
	defer cleanup()                                                                                // Release Redis resources after the audit assertions complete.
	var recordedEvents []audit.Event                                                               // Collect append-only token events through an in-memory recorder.
	store.SetAuditRecorder(audit.RecorderFunc(func(ctx context.Context, event audit.Event) error { // Install one deterministic recorder under the production token store API.
		recordedEvents = append(recordedEvents, event) // Preserve the exact token lifecycle event emitted after Redis persistence.
		return nil                                     // Keep audit persistence successful so issuance behavior remains unchanged.
	}))
	requestContext := audit.WithRequestContext(context.Background(), audit.RequestContext{RequestID: "request-ws-token", SourceIP: "192.0.2.20"}) // Attach the safe gateway correlation metadata used by runtime issuance.
	subject := &auth.Subject{ID: "subject-token-audit", TenantID: "tenant-token-audit", Type: "pat", CredentialID: "pat-safe-id"}                 // Build one fully attributable authenticated issuer.
	issuedToken, err := store.Issue(requestContext, subject, "trace-token-audit")                                                                 // Issue one real opaque token through Redis and audit recording.
	if err != nil {                                                                                                                               // Fail when audited issuance unexpectedly rejects the valid request.
		t.Fatalf("expected audited subscription token issuance to succeed, got error: %v", err) // Surface setup or audit integration failures.
	}
	if len(recordedEvents) != 1 { // Require exactly one token issuance event.
		t.Fatalf("expected one subscription token audit event, got %d", len(recordedEvents)) // Surface missing or duplicated token audit writes.
	}
	event := recordedEvents[0]                                                                                                                             // Read the sole issuance event for safe identifier assertions.
	if event.Result != "success" || event.CredentialID != "pat-safe-id" || event.TraceID != "trace-token-audit" || event.RequestID != "request-ws-token" { // Require complete issuer, trace, and request attribution.
		t.Fatalf("unexpected subscription token audit event: %#v", event) // Surface malformed lifecycle metadata.
	}
	if strings.TrimSpace(event.Metadata["jti"].(string)) == "" { // Require one independent token identifier for audit correlation.
		t.Fatal("expected subscription token audit event to include a non-empty JTI") // Surface missing safe token identity.
	}
	serializedEvent, err := json.Marshal(event) // Serialize every event field for opaque-token leakage detection.
	if err != nil {                             // Fail when the event cannot be represented by the PostgreSQL recorder.
		t.Fatalf("failed to serialize subscription token audit event: %v", err) // Surface incompatible metadata.
	}
	if strings.Contains(string(serializedEvent), issuedToken.Token) { // Reject the actual browser credential anywhere in the audit event.
		t.Fatalf("subscription token audit event leaked opaque token: %s", serializedEvent) // Surface unsafe token persistence immediately.
	}
}

// TestSubscriptionTokenStoreLookupClassifiesRedisFailure verifies that token-store outages remain server errors instead of invalid-credential responses.
func TestSubscriptionTokenStoreLookupClassifiesRedisFailure(t *testing.T) {
	store, cleanup := newSubscriptionTokenStoreForTest(t) // Construct one real cache wrapper before forcing its client closed.
	defer cleanup()                                       // Release the already closed client and in-memory Redis listener idempotently after the assertion.
	if err := store.cache.Close(); err != nil {           // Close the production Redis client so Lookup encounters an infrastructure failure.
		t.Fatalf("failed to close websocket token cache: %v", err) // Surface fixture failure because the classification scenario was not created.
	}
	_, err := store.Lookup(context.Background(), "unknown-opaque-token") // Resolve one token while the authoritative store is unavailable.
	if !errors.IsCode(err, errors.CodeStoreRead) {                       // Require infrastructure classification rather than unauthenticated.
		t.Fatalf("expected closed token store lookup to return %q, got %v", errors.CodeStoreRead, err) // Surface fail-open or misclassified outage behavior.
	}
}

// TestHandlerResolveSubscriptionRejectsMissingToken verifies that the WebSocket handler rejects missing subscription tokens before any upgrade attempt.
func TestHandlerResolveSubscriptionRejectsMissingToken(t *testing.T) {
	store, cleanup := newSubscriptionTokenStoreForTest(t) // Construct one disposable Redis-backed token store for the handler validation test.
	defer cleanup()                                       // Release the Redis client and in-memory Redis server after the test completes.

	handler := NewHandler(nil, store, nil)                        // Construct the production WebSocket handler so the test exercises the same token-resolution path used at runtime.
	request := httptest.NewRequest("GET", "/ws/plan-events", nil) // Build one WebSocket handshake request that intentionally omits the subscription token query parameter.
	_, err := handler.resolveSubscription(request)                // Resolve the missing-token request directly so the handler's pre-upgrade validation path is exercised.
	if !errors.IsCode(err, errors.CodeUnauthenticated) {          // Fail the test when missing tokens do not map to the standard unauthenticated code.
		t.Fatalf("expected missing subscription token to return %q, got %v", errors.CodeUnauthenticated, err) // Surface the unexpected error classification together with the returned error.
	}
}

// TestHandlerResolveSubscriptionRejectsTraceMismatch verifies that a mismatched traceId query parameter cannot override the trace binding carried by the opaque token.
func TestHandlerResolveSubscriptionRejectsTraceMismatch(t *testing.T) {
	store, cleanup := newSubscriptionTokenStoreForTest(t) // Construct one disposable Redis-backed token store for the handler validation test.
	defer cleanup()                                       // Release the Redis client and in-memory Redis server after the test completes.

	issuedToken, err := store.Issue(context.Background(), &auth.Subject{ID: "subject-1", TenantID: "tenant-1", Type: "pat"}, "trace-authorized") // Issue one token bound to the authorized trace so the mismatch check has a trusted reference value.
	if err != nil {                                                                                                                              // Fail the test when token issuance unexpectedly fails before the mismatch check can run.
		t.Fatalf("expected subscription token issuance to succeed, got error: %v", err) // Surface the unexpected issuance failure together with the returned error.
	}
	handler := NewHandler(nil, store, nil)                                                                                    // Construct the production WebSocket handler so the test exercises the same token-resolution path used at runtime.
	request := httptest.NewRequest("GET", "/ws/plan-events?subscriptionToken="+issuedToken.Token+"&traceId=trace-other", nil) // Build one handshake request whose traceId query parameter intentionally conflicts with the trace authorized by the issued token.
	_, err = handler.resolveSubscription(request)                                                                             // Resolve the mismatched request directly so the handler's trace-binding guard runs before any upgrade attempt.
	if !errors.IsCode(err, errors.CodePermissionDenied) {                                                                     // Fail the test when mismatched trace bindings do not map to the standard permission-denied code.
		t.Fatalf("expected mismatched trace binding to return %q, got %v", errors.CodePermissionDenied, err) // Surface the unexpected error classification together with the returned error.
	}
}
