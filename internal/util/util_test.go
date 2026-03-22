// util_test.go verifies the small pure utility helpers used across gateway, orchestration, and logging paths.
package util

import (
	"context"
	"testing"
	"time"
)

// fakeIdempotencyStore implements IdempotencyStore for direct utility tests.
type fakeIdempotencyStore struct {
	acquired bool
	value    string
	err      error
}

// CheckAndSet returns the preconfigured fake idempotency result so CheckIdempotency can be exercised without any external store.
func (f *fakeIdempotencyStore) CheckAndSet(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	_ = ctx                           // Ignore the context because this fake store returns one preconfigured in-memory result immediately.
	_ = key                           // Ignore the key because this fake store returns one preconfigured in-memory result immediately.
	_ = value                         // Ignore the value because this fake store returns one preconfigured in-memory result immediately.
	_ = ttl                           // Ignore the TTL because this fake store returns one preconfigured in-memory result immediately.
	return f.acquired, f.value, f.err // Return the preconfigured result so the utility helper's pass-through contract can be asserted directly.
}

// TestBackoffRespectsCap verifies that exponential backoff does not exceed the configured cap when jitter is disabled.
func TestBackoffRespectsCap(t *testing.T) {
	delay := Backoff(10, time.Second, 5*time.Second, 0) // Calculate one large-attempt backoff with no jitter so the cap behavior can be asserted deterministically.
	if delay != 5*time.Second {                         // Fail the test when the backoff helper does not clamp the computed delay to the configured cap.
		t.Fatalf("expected capped delay 5s, got %s", delay) // Surface the unexpected delay so retry-timing regressions are obvious.
	}
}

// TestCheckIdempotencyDelegatesToStore verifies that the helper preserves the underlying store result without modification.
func TestCheckIdempotencyDelegatesToStore(t *testing.T) {
	acquired, value, err := CheckIdempotency(context.Background(), &fakeIdempotencyStore{acquired: true, value: "cached"}, "idempotency-key", "payload", time.Minute) // Delegate one idempotency check through the production helper so the pass-through contract can be asserted directly.
	if err != nil {                                                                                                                                                   // Fail the test when the helper introduces an unexpected error on a successful store result.
		t.Fatalf("expected idempotency check to succeed, got error: %v", err) // Surface the unexpected error so helper regressions are obvious.
	}
	if !acquired || value != "cached" { // Fail the test when the helper does not preserve the store's acquired flag or cached payload.
		t.Fatalf("expected acquired=true and cached value, got acquired=%t value=%q", acquired, value) // Surface the unexpected result so helper regressions are obvious.
	}
}

// TestMaskSecretsRedactsSensitiveJSON verifies that common sensitive JSON fields are replaced with a redacted marker.
func TestMaskSecretsRedactsSensitiveJSON(t *testing.T) {
	masked := MaskSecrets(`{"token":"secret-value","safe":"keep"}`) // Mask one JSON payload containing a sensitive token field so redaction behavior can be asserted directly.
	if masked != `{"token": "***","safe":"keep"}` {                 // Fail the test when the masking helper does not redact the sensitive field as expected.
		t.Fatalf("expected masked token payload, got %s", masked) // Surface the unexpected masked string so redaction regressions are obvious.
	}
}

// TestMergeTimeoutsPrefersStepOverride verifies that step-level timeout overrides take precedence over plan defaults.
func TestMergeTimeoutsPrefersStepOverride(t *testing.T) {
	if got := MergeTimeouts(10*time.Second, 3*time.Second); got != 3*time.Second { // Merge one plan default and one positive step override so the override-precedence rule can be asserted directly.
		t.Fatalf("expected 3s merged timeout, got %s", got) // Surface the unexpected timeout so scheduling regressions are obvious.
	}
}
