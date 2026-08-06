// main_test.go verifies small gateway main-package helpers that shape process-level HTTP behavior.
package main

import (
	"net/http"
	"testing"

	"mcp_for_appium/internal/config"
)

// TestHealthHTTPStatusReturnsServiceUnavailableForDown verifies that /healthz uses HTTP 503 when the orchestrator reports a down state.
func TestHealthHTTPStatusReturnsServiceUnavailableForDown(t *testing.T) {
	status := healthHTTPStatus(map[string]interface{}{"status": "down"}) // Map one explicit down health payload through the gateway helper used by /healthz.
	if status != http.StatusServiceUnavailable {                         // Fail the test when the helper does not surface down health as HTTP 503.
		t.Fatalf("expected health HTTP status %d, got %d", http.StatusServiceUnavailable, status) // Surface the unexpected status code because readiness probes depend on it.
	}
}

// TestNewGatewayHTTPServerConfiguresDefensiveTimeouts verifies that the gateway listener cannot retain slow or idle connections indefinitely.
func TestNewGatewayHTTPServerConfiguresDefensiveTimeouts(t *testing.T) {
	cfg := &config.Config{Gateway: config.GatewayConfig{Port: 8080}}                                                     // Build the minimal gateway configuration needed by the HTTP server constructor.
	server := newGatewayHTTPServer(cfg, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))                   // Construct the production server around one inert handler.
	if server.ReadHeaderTimeout <= 0 || server.ReadTimeout <= 0 || server.WriteTimeout <= 0 || server.IdleTimeout <= 0 { // Require every slow-client lifecycle phase to have a finite deadline.
		t.Fatalf("expected defensive HTTP timeouts, got header=%v read=%v write=%v idle=%v", server.ReadHeaderTimeout, server.ReadTimeout, server.WriteTimeout, server.IdleTimeout) // Surface every configured deadline for quick diagnosis.
	}
	if server.MaxHeaderBytes <= 0 { // Require an explicit header-memory ceiling in addition to connection deadlines.
		t.Fatalf("expected positive MaxHeaderBytes, got %d", server.MaxHeaderBytes) // Surface a missing or invalid header limit.
	}
}

// TestHealthHTTPStatusReturnsOKForNonDownStates verifies that healthy and degraded health payloads remain HTTP 200 so operators can still inspect degraded nodes.
func TestHealthHTTPStatusReturnsOKForNonDownStates(t *testing.T) {
	healthyStatus := healthHTTPStatus(map[string]interface{}{"status": "healthy"}) // Map one healthy payload through the gateway helper used by /healthz.
	if healthyStatus != http.StatusOK {                                            // Fail the test when healthy status no longer maps to HTTP 200.
		t.Fatalf("expected healthy health HTTP status %d, got %d", http.StatusOK, healthyStatus) // Surface the unexpected status code because healthy nodes must remain probe-compatible.
	}
	degradedStatus := healthHTTPStatus(map[string]interface{}{"status": "degraded"}) // Map one degraded payload through the gateway helper used by /healthz.
	if degradedStatus != http.StatusOK {                                             // Fail the test when degraded status no longer maps to HTTP 200.
		t.Fatalf("expected degraded health HTTP status %d, got %d", http.StatusOK, degradedStatus) // Surface the unexpected status code because degraded nodes should stay inspectable.
	}
}
