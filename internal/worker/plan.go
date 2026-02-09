package worker

import (
	"encoding/json"

	"mcp_for_appium/internal/errors"
)

type planEnvelope struct {
	Steps []PlanStep `json:"steps"`
}

// ParsePlan parses a plan JSON into steps, supporting array or {steps:[...]}.
func ParsePlan(plan json.RawMessage) ([]PlanStep, error) {
	var steps []PlanStep
	if len(plan) == 0 {
		return nil, errors.New(errors.CodePlanInvalid, "empty plan")
	}

	if err := json.Unmarshal(plan, &steps); err == nil && len(steps) > 0 {
		return steps, nil
	}

	var env planEnvelope
	if err := json.Unmarshal(plan, &env); err != nil {
		return nil, errors.New(errors.CodePlanInvalid, "invalid plan format")
	}
	if len(env.Steps) == 0 {
		return nil, errors.New(errors.CodePlanInvalid, "plan has no steps")
	}
	return env.Steps, nil
}
