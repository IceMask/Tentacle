package orchestrator

import (
	"context"
	"encoding/json"
)

// PlanExecutor executes a plan for a trace and session.
type PlanExecutor interface {
	ExecuteDispatchedPlan(ctx context.Context, traceID string, sessionID string, plan json.RawMessage) error
}
