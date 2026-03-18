// cache_timeout_test.go verifies that the Redis cache constructor fails instead of hanging indefinitely when the configured server accepts TCP connections but never speaks the Redis protocol.
package redis

import (
	"net"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
)

// startHangingTCPServer starts one IPv4 loopback TCP server that accepts connections and then leaves them open without responding.
func startHangingTCPServer(t *testing.T) (string, func()) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind an IPv4 loopback listener explicitly so the hanging test server works in restricted sandbox environments too.
	if err != nil {                                    // Stop immediately when the test environment cannot provide a loopback TCP listener.
		t.Skipf("skipping redis timeout test because listen failed: %v", err) // Surface the environment limitation without flagging the production timeout path as broken.
		return "", func() {}                                                  // Return a no-op server handle after the skip to satisfy the compiler even though execution will not continue.
	}

	stopCh := make(chan struct{}) // Coordinate graceful shutdown of the accept loop and every accepted connection.
	go func() {
		for { // Continue accepting connections until the test stops the hanging server explicitly.
			conn, acceptErr := listener.Accept() // Accept one incoming TCP connection from the Redis client under test.
			if acceptErr != nil {                // Exit the accept loop when the listener is closed during test cleanup.
				return // Stop serving because the test is tearing the hanging TCP server down.
			}
			go func(c net.Conn) {
				defer c.Close() // Ensure each accepted connection is closed once the test shuts the hanging server down.
				<-stopCh        // Keep the TCP connection open and silent so the client blocks waiting for a Redis protocol response.
			}(conn)
		}
	}()

	return listener.Addr().String(), func() { // Return the loopback address together with a stop closure so tests can clean up deterministically.
		close(stopCh)        // Release every goroutine that is intentionally hanging accepted connections open.
		_ = listener.Close() // Close the listener explicitly so the accept loop exits immediately.
	}
}

// TestNewCacheFailsWhenRedisDoesNotRespond verifies that cache construction fails with the storage-connectivity error code when Redis never answers the initial ping.
func TestNewCacheFailsWhenRedisDoesNotRespond(t *testing.T) {
	address, stopServer := startHangingTCPServer(t) // Start one hanging TCP server that accepts Redis connections but never answers the initial ping.
	defer stopServer()                              // Ensure the temporary hanging TCP server is always stopped after the test completes.

	startedAt := time.Now()                               // Capture the constructor start time so the test can verify the built-in timeout is eventually enforced.
	_, err := NewCache(config.RedisConfig{Addr: address}) // Invoke the production Redis cache constructor against the hanging loopback TCP server.
	elapsed := time.Since(startedAt)                      // Measure the wall-clock duration so the test can verify the constructor does not hang indefinitely.
	if err == nil {                                       // Fail the test when cache construction unexpectedly succeeds against a non-speaking TCP peer.
		t.Fatal("expected redis cache timeout error") // Surface the missing expected timeout failure clearly.
	}
	if !errors.IsCode(err, errors.CodeStoreConn) { // Fail the test when the returned error code does not classify the timeout as a storage connectivity failure.
		t.Fatalf("expected E.STORE.CONN for redis timeout, got: %v", err) // Surface the unexpected error classification because startup callers rely on that code.
	}
	if elapsed >= 8*time.Second { // Fail the test when cache construction hangs far beyond the built-in timeout budget instead of eventually failing.
		t.Fatalf("expected redis cache construction to fail within timeout budget, took %v", elapsed) // Surface the slow failure because startup must not stall indefinitely on an unresponsive Redis endpoint.
	}
}
