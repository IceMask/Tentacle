package util

import "time"

// MergeTimeouts merges a plan-level default with a step-level override.
// If stepOverride is > 0, it takes precedence.
// Otherwise, planDefault is used.
// If both are 0, a global default is returned (optional, here we assume caller handles global).
func MergeTimeouts(planDefault, stepOverride time.Duration) time.Duration {
	if stepOverride > 0 {
		return stepOverride
	}
	return planDefault
}
