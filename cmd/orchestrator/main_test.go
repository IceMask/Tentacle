// main_test.go verifies the standalone orchestrator mode-normalization helper used during startup.
package main

import (
	"testing"

	"mcp_for_appium/internal/orchestrator"
)

// TestNormalizeStandaloneExecutionModePreservesDistributed verifies that the standalone orchestrator keeps an already-distributed mode unchanged.
func TestNormalizeStandaloneExecutionModePreservesDistributed(t *testing.T) {
	mode, forced := normalizeStandaloneExecutionMode(orchestrator.ExecutionModeDistributed) // Normalize the already-supported distributed mode so the helper can report whether an override was required.
	if mode != orchestrator.ExecutionModeDistributed {                                      // Fail the test when the helper changes a mode that is already valid for the standalone orchestrator.
		t.Fatalf("expected distributed mode to be preserved, got %q", mode) // Surface the unexpected effective mode so the startup normalization regression is obvious.
	}
	if forced { // Fail the test when the helper reports an override for the already-supported distributed mode.
		t.Fatal("expected distributed mode to avoid override") // Surface the unexpected override flag because startup logs and behavior depend on it.
	}
}

// TestNormalizeStandaloneExecutionModeForcesNonDistributed verifies that the standalone orchestrator forces unsupported modes back to distributed.
func TestNormalizeStandaloneExecutionModeForcesNonDistributed(t *testing.T) {
	mode, forced := normalizeStandaloneExecutionMode(orchestrator.ExecutionModeMonolith) // Normalize the unsupported monolith mode so the helper can force the standalone process into its supported runtime posture.
	if mode != orchestrator.ExecutionModeDistributed {                                   // Fail the test when the helper does not force unsupported standalone modes to distributed.
		t.Fatalf("expected distributed mode after normalization, got %q", mode) // Surface the unexpected effective mode so the startup normalization regression is obvious.
	}
	if !forced { // Fail the test when the helper does not report that it overrode the unsupported configured mode.
		t.Fatal("expected unsupported standalone mode to be forced") // Surface the missing override flag because startup logs and behavior depend on it.
	}
}
