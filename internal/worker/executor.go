package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"mcp_for_appium/internal/errors"
)

type Executor struct {
	appium         AppiumClient
	stepTimeout    time.Duration
	autoWaitMax    time.Duration
	retryMax       int
	retryMinJitter time.Duration
	retryMaxJitter time.Duration
	onEvent        func(StepEvent)
}

type AppiumClient interface {
	FindElement(ctx context.Context, strategy, selector string) (string, error)
	Click(ctx context.Context, elementID string) error
	SendKeys(ctx context.Context, elementID, text string) error
	Clear(ctx context.Context, elementID string) error
	Screenshot(ctx context.Context) ([]byte, error)
	PageSource(ctx context.Context) (string, error)
	Tap(ctx context.Context, x, y int) error
	Swipe(ctx context.Context, x1, y1, x2, y2, durationMs int) error
	LongPress(ctx context.Context, elementID string, durationMs int) error
	Back(ctx context.Context) error
	HideKeyboard(ctx context.Context) error
}

// NewExecutor executes this operation.
func NewExecutor(appium AppiumClient, stepTimeout time.Duration, autoWaitMax time.Duration, onEvent func(StepEvent)) *Executor {
	if stepTimeout <= 0 {
		stepTimeout = 30 * time.Second
	}
	if autoWaitMax <= 0 {
		autoWaitMax = 5 * time.Second
	}
	return &Executor{
		appium:         appium,
		stepTimeout:    stepTimeout,
		autoWaitMax:    autoWaitMax,
		retryMax:       3,
		retryMinJitter: 200 * time.Millisecond,
		retryMaxJitter: 1200 * time.Millisecond,
		onEvent:        onEvent,
	}
}

type PlanStep struct {
	Type     string          `json:"type"`
	Selector string          `json:"selector,omitempty"`
	Params   json.RawMessage `json:"params,omitempty"`
}

type StepMetrics struct {
	Attempt   int   `json:"attempt"`
	WDCalls   int   `json:"wdCalls"`
	ElapsedMs int64 `json:"elapsedMs"`
}

type StepEvent struct {
	StepIndex    int         `json:"stepIndex"`
	Status       string      `json:"status"`
	Message      string      `json:"message"`
	Metrics      StepMetrics `json:"metrics"`
	ArtifactRefs []string    `json:"artifactRefs,omitempty"`
	Phase        string      `json:"phase,omitempty"`
}

// Execute executes this operation.
func (e *Executor) Execute(ctx context.Context, plan []PlanStep) error {
	for i, step := range plan {
		start := time.Now()
		stepCtx, cancel := context.WithTimeout(ctx, e.stepTimeout)

		e.emit(StepEvent{
			StepIndex: i,
			Status:    "running",
			Message:   fmt.Sprintf("step %d starting", i),
			Metrics:   StepMetrics{Attempt: 0, WDCalls: 0},
		})

		err := func() error {
			switch step.Type {
			case "click":
				if err := e.executeClick(stepCtx, i, step, start); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
			case "clearElement":
				if err := e.executeClear(stepCtx, i, step, start); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
			case "wait":
				waitMs := 1000
				if len(step.Params) > 0 {
					var params struct {
						Ms int `json:"ms"`
					}
					if err := json.Unmarshal(step.Params, &params); err == nil && params.Ms > 0 {
						waitMs = params.Ms
					}
				}
				time.Sleep(time.Duration(waitMs) * time.Millisecond)
				e.emitSuccess(i, "wait completed", start, StepMetrics{Attempt: 1})
			case "sendKeys":
				if err := e.executeSendKeys(stepCtx, i, step, start); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
			case "tap":
				if err := e.executeTap(stepCtx, i, step, start); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
			case "swipe":
				if err := e.executeSwipe(stepCtx, i, step, start); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
			case "longPress":
				if err := e.executeLongPress(stepCtx, i, step, start); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
			case "pressBack":
				if err := e.appium.Back(stepCtx); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
				e.emitSuccess(i, "pressBack succeeded", start, StepMetrics{Attempt: 1, WDCalls: 1})
			case "hideKeyboard":
				if err := e.appium.HideKeyboard(stepCtx); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
				e.emitSuccess(i, "hideKeyboard succeeded", start, StepMetrics{Attempt: 1, WDCalls: 1})
			case "screenshot":
				if _, err := e.appium.Screenshot(stepCtx); err != nil {
					e.emitFailure(stepCtx, i, err, start)
					return err
				}
				e.emitSuccess(i, "screenshot captured", start, StepMetrics{Attempt: 1})
			default:
				err := errors.New(errors.CodeStepUnsupported, "unknown step type: "+step.Type)
				e.emitFailure(stepCtx, i, err, start)
				return err
			}
			return nil
		}()
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// executeClick executes this operation.
func (e *Executor) executeClick(ctx context.Context, stepIndex int, step PlanStep, start time.Time) error {
	var lastErr error
	for attempt := 1; attempt <= e.retryMax; attempt++ {
		elementID, attempts, wdCalls, err := e.findWithAutoWait(ctx, step.Selector)
		if err != nil {
			lastErr = err
			if !e.isTransient(err) {
				return err
			}
			e.backoff(attempt)
			continue
		}

		if err := e.appium.Click(ctx, elementID); err != nil {
			lastErr = err
			if !e.isTransient(err) {
				return err
			}
			e.backoff(attempt)
			continue
		}

		e.emitSuccess(stepIndex, "click succeeded", start, StepMetrics{
			Attempt: attempt + attempts - 1,
			WDCalls: wdCalls + 1,
		})
		return nil
	}
	return lastErr
}

// executeClear executes this operation.
func (e *Executor) executeClear(ctx context.Context, stepIndex int, step PlanStep, start time.Time) error {
	var lastErr error
	for attempt := 1; attempt <= e.retryMax; attempt++ {
		elementID, attempts, wdCalls, err := e.findWithAutoWait(ctx, step.Selector)
		if err != nil {
			lastErr = err
			if !e.isTransient(err) {
				return err
			}
			e.backoff(attempt)
			continue
		}

		if err := e.appium.Clear(ctx, elementID); err != nil {
			lastErr = err
			if !e.isTransient(err) {
				return err
			}
			e.backoff(attempt)
			continue
		}

		e.emitSuccess(stepIndex, "clearElement succeeded", start, StepMetrics{
			Attempt: attempt + attempts - 1,
			WDCalls: wdCalls + 1,
		})
		return nil
	}
	return lastErr
}

// executeSendKeys executes this operation.
func (e *Executor) executeSendKeys(ctx context.Context, stepIndex int, step PlanStep, start time.Time) error {
	var params struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(step.Params, &params); err != nil || params.Text == "" {
		return errors.New(errors.CodePlanInvalid, "sendKeys requires params.text")
	}

	var lastErr error
	for attempt := 1; attempt <= e.retryMax; attempt++ {
		elementID, attempts, wdCalls, err := e.findWithAutoWait(ctx, step.Selector)
		if err != nil {
			lastErr = err
			if !e.isTransient(err) {
				return err
			}
			e.backoff(attempt)
			continue
		}

		if err := e.appium.SendKeys(ctx, elementID, params.Text); err != nil {
			lastErr = err
			if !e.isTransient(err) {
				return err
			}
			e.backoff(attempt)
			continue
		}

		e.emitSuccess(stepIndex, "sendKeys succeeded", start, StepMetrics{
			Attempt: attempt + attempts - 1,
			WDCalls: wdCalls + 1,
		})
		return nil
	}
	return lastErr
}

// executeTap executes this operation.
func (e *Executor) executeTap(ctx context.Context, stepIndex int, step PlanStep, start time.Time) error {
	var params struct {
		X *int `json:"x"`
		Y *int `json:"y"`
	}
	if err := json.Unmarshal(step.Params, &params); err != nil || params.X == nil || params.Y == nil {
		return errors.New(errors.CodePlanInvalid, "tap requires params.x and params.y")
	}
	if err := e.appium.Tap(ctx, *params.X, *params.Y); err != nil {
		return err
	}
	e.emitSuccess(stepIndex, "tap succeeded", start, StepMetrics{Attempt: 1, WDCalls: 1})
	return nil
}

// executeSwipe executes this operation.
func (e *Executor) executeSwipe(ctx context.Context, stepIndex int, step PlanStep, start time.Time) error {
	var params struct {
		StartX     *int `json:"startX"`
		StartY     *int `json:"startY"`
		EndX       *int `json:"endX"`
		EndY       *int `json:"endY"`
		DurationMs int  `json:"durationMs"`
	}
	if err := json.Unmarshal(step.Params, &params); err != nil || params.StartX == nil || params.StartY == nil || params.EndX == nil || params.EndY == nil {
		return errors.New(errors.CodePlanInvalid, "swipe requires params.startX,startY,endX,endY")
	}
	if params.DurationMs <= 0 {
		params.DurationMs = 200
	}
	if err := e.appium.Swipe(ctx, *params.StartX, *params.StartY, *params.EndX, *params.EndY, params.DurationMs); err != nil {
		return err
	}
	e.emitSuccess(stepIndex, "swipe succeeded", start, StepMetrics{Attempt: 1, WDCalls: 1})
	return nil
}

// executeLongPress executes this operation.
func (e *Executor) executeLongPress(ctx context.Context, stepIndex int, step PlanStep, start time.Time) error {
	var params struct {
		DurationMs int `json:"durationMs"`
	}
	if len(step.Params) > 0 {
		if err := json.Unmarshal(step.Params, &params); err != nil {
			return errors.New(errors.CodePlanInvalid, "longPress params must be valid json")
		}
	}
	if params.DurationMs <= 0 {
		params.DurationMs = 1000
	}

	var lastErr error
	for attempt := 1; attempt <= e.retryMax; attempt++ {
		elementID, attempts, wdCalls, err := e.findWithAutoWait(ctx, step.Selector)
		if err != nil {
			lastErr = err
			if !e.isTransient(err) {
				return err
			}
			e.backoff(attempt)
			continue
		}

		if err := e.appium.LongPress(ctx, elementID, params.DurationMs); err != nil {
			lastErr = err
			if !e.isTransient(err) {
				return err
			}
			e.backoff(attempt)
			continue
		}

		e.emitSuccess(stepIndex, "longPress succeeded", start, StepMetrics{
			Attempt: attempt + attempts - 1,
			WDCalls: wdCalls + 1,
		})
		return nil
	}
	return lastErr
}

// findWithAutoWait executes this operation.
func (e *Executor) findWithAutoWait(ctx context.Context, selector string) (string, int, int, error) {
	if selector == "" {
		return "", 0, 0, errors.New(errors.CodePlanInvalid, "selector is required")
	}

	strategies := selectorStrategies(selector)
	start := time.Now()
	attempt := 0
	wdCalls := 0

	for {
		attempt++
		for _, strategy := range strategies {
			wdCalls++
			el, err := e.appium.FindElement(ctx, strategy, selectorValue(selector))
			if err == nil {
				return el, attempt, wdCalls, nil
			}
		}

		if time.Since(start) >= e.autoWaitMax {
			return "", attempt, wdCalls, errors.New(errors.CodeAppElemNotFound, "element not found after auto-wait")
		}

		select {
		case <-ctx.Done():
			return "", attempt, wdCalls, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// selectorStrategies executes this operation.
func selectorStrategies(selector string) []string {
	lower := strings.ToLower(selector)
	switch {
	case strings.HasPrefix(lower, "xpath=") || strings.HasPrefix(lower, "//"):
		return []string{"xpath"}
	case strings.HasPrefix(lower, "id="):
		return []string{"id", "accessibility id", "xpath"}
	case strings.HasPrefix(lower, "accessibility="):
		return []string{"accessibility id", "id", "xpath"}
	case strings.HasPrefix(lower, "css="):
		return []string{"css selector", "xpath"}
	default:
		return []string{"accessibility id", "id", "xpath"}
	}
}

// selectorValue executes this operation.
func selectorValue(selector string) string {
	lower := strings.ToLower(selector)
	switch {
	case strings.HasPrefix(lower, "xpath="):
		return selector[6:]
	case strings.HasPrefix(lower, "id="):
		return selector[3:]
	case strings.HasPrefix(lower, "accessibility="):
		return selector[len("accessibility="):]
	case strings.HasPrefix(lower, "css="):
		return selector[4:]
	default:
		return selector
	}
}

// emit executes this operation.
func (e *Executor) emit(ev StepEvent) {
	if e.onEvent != nil {
		e.onEvent(ev)
	}
}

// emitSuccess executes this operation.
func (e *Executor) emitSuccess(stepIndex int, msg string, start time.Time, metrics StepMetrics) {
	if metrics.ElapsedMs == 0 {
		metrics.ElapsedMs = time.Since(start).Milliseconds()
	}
	e.emit(StepEvent{
		StepIndex: stepIndex,
		Status:    "passed",
		Message:   msg,
		Metrics:   metrics,
	})
}

// emitFailure executes this operation.
func (e *Executor) emitFailure(ctx context.Context, stepIndex int, err error, start time.Time) {
	artifactRefs := []string{}
	if e.appium != nil {
		if _, shotErr := e.appium.Screenshot(ctx); shotErr == nil {
			artifactRefs = append(artifactRefs, "screenshot:captured")
		}
	}
	e.emit(StepEvent{
		StepIndex: stepIndex,
		Status:    "failed",
		Message:   err.Error(),
		Metrics: StepMetrics{
			Attempt:   1,
			ElapsedMs: time.Since(start).Milliseconds(),
		},
		ArtifactRefs: artifactRefs,
	})
}

// isTransient executes this operation.
func (e *Executor) isTransient(err error) bool {
	return errors.IsCode(err, errors.CodeAppTimeout) || errors.IsCode(err, errors.CodeStoreConn) || errors.IsCode(err, errors.CodeAppElemNotFound)
}

// backoff executes this operation.
func (e *Executor) backoff(attempt int) {
	if attempt <= 0 {
		return
	}
	jitterRange := int64(e.retryMaxJitter - e.retryMinJitter)
	if jitterRange <= 0 {
		time.Sleep(e.retryMinJitter)
		return
	}
	jitter := e.retryMinJitter + time.Duration(rand.Int63n(jitterRange))
	time.Sleep(jitter)
}
