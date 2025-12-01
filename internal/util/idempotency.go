package util

import (
	"context"
	"time"
)

// IdempotencyStore defines the storage requirement for idempotency checks.
type IdempotencyStore interface {
	// CheckAndSet checks if key exists. If not, sets it with ttl and returns true (acquired).
	// If exists, returns false and the stored value (if any).
	CheckAndSet(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error)
}

// CheckIdempotency is a helper to check and set idempotency key.
func CheckIdempotency(ctx context.Context, store IdempotencyStore, key string, val string, ttl time.Duration) (bool, string, error) {
	return store.CheckAndSet(ctx, key, val, ttl)
}
