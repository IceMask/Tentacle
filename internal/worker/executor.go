package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"mcp_for_appium/internal/worker/appium"
)

type Executor struct {
	appium *appium.Client
}

func NewExecutor(appium *appium.Client) *Executor {
	return &Executor{appium: appium}
}

type PlanStep struct {
	Type     string          `json:"type"`
	Selector string          `json:"selector,omitempty"`
	Params   json.RawMessage `json:"params,omitempty"`
}

func (e *Executor) Execute(ctx context.Context, plan []PlanStep) error {
	for i, step := range plan {
		fmt.Printf("Executing step %d: %s\n", i, step.Type)

		switch step.Type {
		case "click":
			// 1. Find
			el, err := e.appium.FindElement(ctx, "xpath", step.Selector) // Default to xpath for now
			if err != nil {
				return fmt.Errorf("step %d failed: %w", i, err)
			}
			// 2. Click
			if err := e.appium.Click(ctx, el); err != nil {
				return fmt.Errorf("step %d click failed: %w", i, err)
			}
		case "wait":
			time.Sleep(1 * time.Second)
		default:
			return fmt.Errorf("unknown step type: %s", step.Type)
		}
	}
	return nil
}
