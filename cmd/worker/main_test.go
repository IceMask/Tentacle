// main_test.go verifies the small pure worker startup helpers that determine advertise address and lease-renewal cadence.
package main

import (
	"strings"
	"testing"
	"time"
)

// TestBuildWorkerAdvertiseAddressPrefersConfiguredValue verifies that an explicit advertise address wins over any hostname-derived fallback.
func TestBuildWorkerAdvertiseAddressPrefersConfiguredValue(t *testing.T) {
	address := buildWorkerAdvertiseAddress("worker.example.internal:9092", 9092) // Resolve the worker advertise address using one explicit operator-supplied value.
	if address != "worker.example.internal:9092" {                               // Fail the test when the helper ignores the configured advertise address.
		t.Fatalf("expected configured advertise address, got %q", address) // Surface the unexpected address so worker callback-routing regressions are obvious.
	}
}

// TestBuildWorkerAdvertiseAddressFallsBackToHostAndPort verifies that the fallback advertise address still includes the worker gRPC port.
func TestBuildWorkerAdvertiseAddressFallsBackToHostAndPort(t *testing.T) {
	address := buildWorkerAdvertiseAddress("", 19092) // Resolve the worker advertise address without any configured override so the hostname fallback path runs.
	if !strings.HasSuffix(address, ":19092") {        // Fail the test when the fallback address does not include the worker gRPC port expected by the orchestrator dial path.
		t.Fatalf("expected fallback address to end with :19092, got %q", address) // Surface the unexpected address so callback-routing regressions are obvious.
	}
}

// TestLeaseRenewEveryUsesSafeDefaults verifies that lease renewal falls back to a conservative cadence for missing or tiny heartbeat intervals.
func TestLeaseRenewEveryUsesSafeDefaults(t *testing.T) {
	if got := leaseRenewEvery(0); got != 5*time.Second { // Exercise the missing-heartbeat path so lease renewal remains active even when startup config is incomplete.
		t.Fatalf("expected 5s fallback renewal cadence, got %s", got) // Surface the unexpected cadence so distributed lease-expiry regressions are obvious.
	}
	if got := leaseRenewEvery(1500 * time.Millisecond); got != time.Second { // Exercise the tiny-heartbeat clamp so the worker does not spin on sub-second renewals.
		t.Fatalf("expected 1s clamped renewal cadence, got %s", got) // Surface the unexpected cadence so renewal-rate regressions are obvious.
	}
	if got := leaseRenewEvery(10 * time.Second); got != 5*time.Second { // Exercise the normal half-heartbeat path so renewal timing stays aligned with ownership lease expectations.
		t.Fatalf("expected 5s renewal cadence, got %s", got) // Surface the unexpected cadence so renewal-rate regressions are obvious.
	}
}
