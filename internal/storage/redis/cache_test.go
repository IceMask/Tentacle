// cache_test.go verifies atomic Redis ownership primitives used by distributed dispatch locks and execution leases.
package redis

import (
	"context"
	stdErrors "errors"
	"testing"
	"time"

	"mcp_for_appium/internal/config"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// TestCompareAndSetAndDeletePreserveOwnership verifies that stale values cannot replace or remove a newer Redis owner.
func TestCompareAndSetAndDeletePreserveOwnership(t *testing.T) {
	redisServer := miniredis.RunT(t)                                     // Start one disposable Redis-compatible server for real Lua-script behavior.
	cache, err := NewCache(config.RedisConfig{Addr: redisServer.Addr()}) // Construct the production cache wrapper against the isolated server.
	if err != nil {                                                      // Stop when the cache wrapper cannot connect to the fixture.
		t.Fatalf("failed to construct redis cache: %v", err) // Surface fixture setup failure explicitly.
	}
	defer func() { // Release both the production client and disposable server after all ownership assertions.
		_ = cache.Close()   // Close pooled Redis connections before stopping their endpoint.
		redisServer.Close() // Release the loopback listener owned by miniredis.
	}()
	ctx := context.Background()                                                  // Use one synchronous context for the short ownership script calls.
	if err := cache.Set(ctx, "lease:test", "owner-1", time.Minute); err != nil { // Seed the first exact lease owner.
		t.Fatalf("failed to seed redis owner: %v", err) // Surface setup failure because comparison assertions require the key.
	}

	replaced, err := cache.CompareAndSet(ctx, "lease:test", "stale-owner", "owner-2", time.Minute) // Attempt replacement with a stale expected value.
	if err != nil {                                                                                // Fail when a normal compare mismatch returns an infrastructure error.
		t.Fatalf("stale compare-and-set returned error: %v", err) // Surface unexpected Lua execution failure.
	}
	if replaced { // Reject stale ownership replacement.
		t.Fatal("expected stale compare-and-set not to replace current owner") // Surface lost lease isolation.
	}
	value, err := cache.Get(ctx, "lease:test") // Read the value after the rejected stale replacement.
	if err != nil || value != "owner-1" {      // Require the original owner to remain unchanged.
		t.Fatalf("expected owner-1 after stale replacement, got %q with error %v", value, err) // Surface unexpected value mutation.
	}

	replaced, err = cache.CompareAndSet(ctx, "lease:test", "owner-1", "owner-2", time.Minute) // Replace through the current exact owner.
	if err != nil || !replaced {                                                              // Require the matching owner to update atomically.
		t.Fatalf("expected matching compare-and-set to succeed, replaced=%v error=%v", replaced, err) // Surface script or comparison regression.
	}
	removed, err := cache.CompareAndDelete(ctx, "lease:test", "owner-1") // Attempt cleanup with the now-stale former owner.
	if err != nil {                                                      // Fail when a normal compare mismatch returns an infrastructure error.
		t.Fatalf("stale compare-and-delete returned error: %v", err) // Surface unexpected Lua execution failure.
	}
	if removed { // Reject stale cleanup that would delete the successor's lease.
		t.Fatal("expected stale compare-and-delete not to remove current owner") // Surface unsafe ownership cleanup.
	}
	removed, err = cache.CompareAndDelete(ctx, "lease:test", "owner-2") // Delete through the current exact owner.
	if err != nil || !removed {                                         // Require matching ownership cleanup to succeed atomically.
		t.Fatalf("expected matching compare-and-delete to succeed, removed=%v error=%v", removed, err) // Surface script or comparison regression.
	}
	if _, err := cache.Get(ctx, "lease:test"); !stdErrors.Is(err, goredis.Nil) { // Require the key to be absent after matching cleanup.
		t.Fatalf("expected lease key to be deleted, got error %v", err) // Surface leaked ownership state.
	}
}
