package orchestrator

import "strings"

const (
	ExecutionModeMonolith    = "monolith"
	ExecutionModeDistributed = "distributed"
)

// normalizeExecutionMode executes this operation.
func normalizeExecutionMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case ExecutionModeDistributed:
		return ExecutionModeDistributed
	default:
		return ExecutionModeMonolith
	}
}
