package util

import (
	"math"
	"math/rand"
	"time"
)

// Backoff calculates the delay for the n-th attempt (0-indexed) using exponential backoff with jitter.
func Backoff(attempt int, base time.Duration, cap time.Duration, jitter time.Duration) time.Duration {
	// base * 2^attempt
	exp := math.Pow(2, float64(attempt))
	delay := float64(base) * exp

	// Apply cap
	if delay > float64(cap) {
		delay = float64(cap)
	}

	// Apply jitter: +/- jitter/2
	// Or just random between 0 and jitter?
	// Requirements say "200-1200ms jitter" implies a range or additive.
	// Let's assume additive jitter: delay + rand(0, jitter)
	if jitter > 0 {
		j := rand.Int63n(int64(jitter))
		delay += float64(j)
	}

	return time.Duration(delay)
}
