// dao_timeout_test.go verifies that the PostgreSQL DAO constructor respects caller deadlines when the configured server accepts TCP connections but never completes the protocol handshake.
package postgres

import (
	"context"
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
		t.Skipf("skipping postgres timeout test because listen failed: %v", err) // Surface the environment limitation without flagging the production timeout path as broken.
		return "", func() {}                                                     // Return a no-op server handle after the skip to satisfy the compiler even though execution will not continue.
	}

	stopCh := make(chan struct{}) // Coordinate graceful shutdown of the accept loop and every accepted connection.
	go func() {
		for { // Continue accepting connections until the test stops the hanging server explicitly.
			conn, acceptErr := listener.Accept() // Accept one incoming TCP connection from the DAO under test.
			if acceptErr != nil {                // Exit the accept loop when the listener is closed during test cleanup.
				return // Stop serving because the test is tearing the hanging TCP server down.
			}
			go func(c net.Conn) {
				defer c.Close() // Ensure each accepted connection is closed once the test shuts the hanging server down.
				<-stopCh        // Keep the TCP connection open and silent so the client blocks waiting for the PostgreSQL protocol handshake.
			}(conn)
		}
	}()

	return listener.Addr().String(), func() { // Return the loopback address together with a stop closure so tests can clean up deterministically.
		close(stopCh)        // Release every goroutine that is intentionally hanging accepted connections open.
		_ = listener.Close() // Close the listener explicitly so the accept loop exits immediately.
	}
}

// TestNewDAORespectsContextDeadlineWhenServerDoesNotRespond verifies that DAO construction fails quickly when PostgreSQL never speaks the wire protocol.
func TestNewDAORespectsContextDeadlineWhenServerDoesNotRespond(t *testing.T) {
	address, stopServer := startHangingTCPServer(t) // Start one hanging TCP server that accepts PostgreSQL connections but never completes the protocol handshake.
	defer stopServer()                              // Ensure the temporary hanging TCP server is always stopped after the test completes.

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond) // Bound DAO construction with a short timeout so the hanging-server test completes quickly.
	defer cancel()                                                                 // Release the timeout resources once DAO construction returns.
	startedAt := time.Now()                                                        // Capture the constructor start time so the test can verify fail-fast deadline handling.
	_, err := NewDAO(ctx, config.PostgresConfig{                                   // Invoke the production DAO constructor against the hanging PostgreSQL endpoint.
		DSN:             "postgres://postgres:postgres@" + address + "/mcp_mobile_worker?sslmode=disable", // Point the DSN at the hanging loopback TCP server instead of a real PostgreSQL instance.
		MaxOpenConns:    1,                                                                                // Use the smallest positive pool size because the timeout path needs only one blocked connection.
		MaxIdleConns:    1,                                                                                // Use the smallest positive idle pool size because the timeout path needs only one blocked connection.
		ConnMaxLifetime: time.Minute,                                                                      // Use a valid positive lifetime because the timeout test focuses on connectivity, not config validation.
	})
	elapsed := time.Since(startedAt) // Measure the wall-clock duration so the test can verify the constructor honors the caller deadline.
	if err == nil {                  // Fail the test when DAO construction unexpectedly succeeds against a non-speaking TCP peer.
		t.Fatal("expected postgres dao timeout error") // Surface the missing expected timeout failure clearly.
	}
	if !errors.IsCode(err, errors.CodeStoreConn) { // Fail the test when the returned error code does not classify the timeout as a storage connectivity failure.
		t.Fatalf("expected E.STORE.CONN for postgres timeout, got: %v", err) // Surface the unexpected error classification because startup callers rely on that code.
	}
	if elapsed >= time.Second { // Fail the test when DAO construction hangs far beyond its timeout budget instead of failing promptly.
		t.Fatalf("expected postgres dao construction to fail fast, took %v", elapsed) // Surface the slow failure because startup must not stall indefinitely on an unresponsive database.
	}
}
