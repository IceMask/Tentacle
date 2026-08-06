// executor_test.go verifies that the worker executor performs real step execution, emits terminal events, and propagates failures through the AppiumClient interface.
package worker

import (
	"context"
	"encoding/json"
	stdErrors "errors"
	"testing"
	"time"

	apperrors "mcp_for_appium/internal/errors"
)

// recordedFindCall captures one FindElement invocation made by the executor under test.
type recordedFindCall struct {
	strategy string
	selector string
}

// recordedSendKeysCall captures one SendKeys invocation made by the executor under test.
type recordedSendKeysCall struct {
	elementID string
	text      string
}

// recordedTapCall captures one Tap invocation made by the executor under test.
type recordedTapCall struct {
	x int
	y int
}

// recordedSwipeCall captures one Swipe invocation made by the executor under test.
type recordedSwipeCall struct {
	x1         int
	y1         int
	x2         int
	y2         int
	durationMs int
}

// recordedLongPressCall captures one LongPress invocation made by the executor under test.
type recordedLongPressCall struct {
	elementID  string
	durationMs int
}

// recordingAppiumClient records every Appium call and allows tests to override specific behaviors.
type recordingAppiumClient struct {
	findCalls         []recordedFindCall
	clickCalls        []string
	sendKeysCalls     []recordedSendKeysCall
	clearCalls        []string
	tapCalls          []recordedTapCall
	swipeCalls        []recordedSwipeCall
	longPressCalls    []recordedLongPressCall
	backCalls         int
	hideKeyboardCalls int
	screenshotCalls   int
	pageSourceCalls   int
	findElementFunc   func(ctx context.Context, strategy string, selector string) (string, error)
	clickFunc         func(ctx context.Context, elementID string) error
	sendKeysFunc      func(ctx context.Context, elementID string, text string) error
	clearFunc         func(ctx context.Context, elementID string) error
	tapFunc           func(ctx context.Context, x int, y int) error
	swipeFunc         func(ctx context.Context, x1 int, y1 int, x2 int, y2 int, durationMs int) error
	longPressFunc     func(ctx context.Context, elementID string, durationMs int) error
	backFunc          func(ctx context.Context) error
	hideKeyboardFunc  func(ctx context.Context) error
	screenshotFunc    func(ctx context.Context) ([]byte, error)
	pageSourceFunc    func(ctx context.Context) (string, error)
}

// FindElement records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) FindElement(ctx context.Context, strategy string, selector string) (string, error) {
	m.findCalls = append(m.findCalls, recordedFindCall{strategy: strategy, selector: selector}) // Record the exact strategy and selector so tests can assert the executor's lookup behavior.
	if m.findElementFunc != nil {                                                               // Delegate to the test override when the current scenario needs custom behavior.
		return m.findElementFunc(ctx, strategy, selector) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return "element-123", nil // Return one stable default element identifier for success-path tests that do not need custom lookup behavior.
}

// Click records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) Click(ctx context.Context, elementID string) error {
	m.clickCalls = append(m.clickCalls, elementID) // Record the clicked element identifier so tests can assert the executor's action targeting.
	if m.clickFunc != nil {                        // Delegate to the test override when the current scenario needs custom click behavior.
		return m.clickFunc(ctx, elementID) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return nil // Return success by default so success-path tests do not need to stub click behavior explicitly.
}

// SendKeys records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) SendKeys(ctx context.Context, elementID string, text string) error {
	m.sendKeysCalls = append(m.sendKeysCalls, recordedSendKeysCall{elementID: elementID, text: text}) // Record the send-keys target and payload so tests can assert input behavior precisely.
	if m.sendKeysFunc != nil {                                                                        // Delegate to the test override when the current scenario needs custom send-keys behavior.
		return m.sendKeysFunc(ctx, elementID, text) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return nil // Return success by default so success-path tests do not need to stub send-keys behavior explicitly.
}

// Clear records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) Clear(ctx context.Context, elementID string) error {
	m.clearCalls = append(m.clearCalls, elementID) // Record the cleared element identifier so tests can assert clear-element targeting precisely.
	if m.clearFunc != nil {                        // Delegate to the test override when the current scenario needs custom clear behavior.
		return m.clearFunc(ctx, elementID) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return nil // Return success by default so success-path tests do not need to stub clear behavior explicitly.
}

// Screenshot records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) Screenshot(ctx context.Context) ([]byte, error) {
	m.screenshotCalls++          // Count screenshot attempts so failure-path tests can assert artifact-capture behavior.
	if m.screenshotFunc != nil { // Delegate to the test override when the current scenario needs custom screenshot behavior.
		return m.screenshotFunc(ctx) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return []byte("fake-screenshot"), nil // Return one stable fake screenshot payload so failure-path artifact capture succeeds by default.
}

// PageSource records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) PageSource(ctx context.Context) (string, error) {
	m.pageSourceCalls++          // Count page-source calls so future tests can assert source fetch behavior when needed.
	if m.pageSourceFunc != nil { // Delegate to the test override when the current scenario needs custom page-source behavior.
		return m.pageSourceFunc(ctx) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return "<xml/>", nil // Return one stable fake page-source payload so default tests can use the mock without extra setup.
}

// Tap records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) Tap(ctx context.Context, x int, y int) error {
	m.tapCalls = append(m.tapCalls, recordedTapCall{x: x, y: y}) // Record the tap coordinates so tests can assert gesture dispatch precisely.
	if m.tapFunc != nil {                                        // Delegate to the test override when the current scenario needs custom tap behavior.
		return m.tapFunc(ctx, x, y) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return nil // Return success by default so success-path tests do not need to stub tap behavior explicitly.
}

// Swipe records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) Swipe(ctx context.Context, x1 int, y1 int, x2 int, y2 int, durationMs int) error {
	m.swipeCalls = append(m.swipeCalls, recordedSwipeCall{x1: x1, y1: y1, x2: x2, y2: y2, durationMs: durationMs}) // Record the swipe parameters so tests can assert gesture dispatch precisely.
	if m.swipeFunc != nil {                                                                                        // Delegate to the test override when the current scenario needs custom swipe behavior.
		return m.swipeFunc(ctx, x1, y1, x2, y2, durationMs) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return nil // Return success by default so success-path tests do not need to stub swipe behavior explicitly.
}

// LongPress records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) LongPress(ctx context.Context, elementID string, durationMs int) error {
	m.longPressCalls = append(m.longPressCalls, recordedLongPressCall{elementID: elementID, durationMs: durationMs}) // Record the long-press target and duration so tests can assert gesture dispatch precisely.
	if m.longPressFunc != nil {                                                                                      // Delegate to the test override when the current scenario needs custom long-press behavior.
		return m.longPressFunc(ctx, elementID, durationMs) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return nil // Return success by default so success-path tests do not need to stub long-press behavior explicitly.
}

// Back records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) Back(ctx context.Context) error {
	m.backCalls++          // Count back-button invocations so tests can assert navigation dispatch precisely.
	if m.backFunc != nil { // Delegate to the test override when the current scenario needs custom back behavior.
		return m.backFunc(ctx) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return nil // Return success by default so success-path tests do not need to stub back behavior explicitly.
}

// HideKeyboard records the call and then executes the configured test behavior.
func (m *recordingAppiumClient) HideKeyboard(ctx context.Context) error {
	m.hideKeyboardCalls++          // Count hide-keyboard invocations so tests can assert keyboard-dismiss behavior precisely.
	if m.hideKeyboardFunc != nil { // Delegate to the test override when the current scenario needs custom hide-keyboard behavior.
		return m.hideKeyboardFunc(ctx) // Return the override result so the executor observes the scenario-specific Appium response.
	}

	return nil // Return success by default so success-path tests do not need to stub hide-keyboard behavior explicitly.
}

// newTestExecutor constructs one executor together with the event sink slice used by assertions.
func newTestExecutor(client AppiumClient, stepTimeout time.Duration, autoWaitMax time.Duration) (*Executor, *[]StepEvent) {
	recordedEvents := make([]StepEvent, 0, 4) // Allocate a small reusable event slice because each unit test only emits a few executor events.
	executor := NewExecutor(client, stepTimeout, autoWaitMax, func(event StepEvent) {
		recordedEvents = append(recordedEvents, event) // Append every emitted event so tests can assert running, passed, and failed transitions exactly.
	})

	return executor, &recordedEvents // Return both the executor and the shared event slice so tests can assert side effects after execution completes.
}

// TestNewExecutorAppliesDefaults verifies that the constructor fills in default timing values when callers pass non-positive durations.
func TestNewExecutorAppliesDefaults(t *testing.T) {
	executor := NewExecutor(&recordingAppiumClient{}, 0, 0, nil) // Construct one executor with zero durations so the constructor must apply defaults.
	if executor == nil {                                         // Reject a nil executor because every later test depends on a live executor instance.
		t.Fatal("expected non-nil executor") // Stop immediately because no constructor defaults can be asserted on a nil executor.
	}
	if executor.stepTimeout != 30*time.Second { // Assert the documented default step timeout so later execution contexts remain bounded.
		t.Fatalf("expected default step timeout 30s, got %v", executor.stepTimeout) // Surface the actual timeout value so constructor regressions are easy to diagnose.
	}
	if executor.autoWaitMax != 5*time.Second { // Assert the documented default auto-wait duration so element lookup behavior stays predictable.
		t.Fatalf("expected default auto wait 5s, got %v", executor.autoWaitMax) // Surface the actual auto-wait value so constructor regressions are easy to diagnose.
	}
}

// TestExecuteClickStepRecordsCallsAndEvents verifies that a successful click step performs one lookup, one click, and emits running/passed events.
func TestExecuteClickStepRecordsCallsAndEvents(t *testing.T) {
	client := &recordingAppiumClient{}                                            // Construct one recording Appium client so the test can assert real executor side effects.
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Second) // Construct one executor with bounded timings so the unit test remains fast and deterministic.
	plan := []PlanStep{{Type: "click", Selector: "xpath=//button[@id='submit']"}} // Build one concrete click plan that exercises selector normalization and click dispatch.

	err := executor.Execute(context.Background(), plan) // Execute the real worker plan against the recording Appium client.
	if err != nil {                                     // Fail immediately when the success-path executor unexpectedly returns an error.
		t.Fatalf("expected click step to succeed, got error: %v", err) // Surface the unexpected executor error so regressions are easy to diagnose.
	}
	if len(client.findCalls) != 1 { // Assert that exactly one lookup was required for the success path.
		t.Fatalf("expected one find call, got %d", len(client.findCalls)) // Surface the actual lookup count so selector or retry regressions are visible.
	}
	if client.findCalls[0].strategy != "xpath" { // Assert that xpath-prefixed selectors route through the xpath lookup strategy.
		t.Fatalf("expected xpath strategy, got %q", client.findCalls[0].strategy) // Surface the actual strategy so selector normalization regressions are easy to diagnose.
	}
	if client.findCalls[0].selector != "//button[@id='submit']" { // Assert that selectorValue strips the explicit xpath= prefix before the Appium call.
		t.Fatalf("expected stripped xpath selector, got %q", client.findCalls[0].selector) // Surface the actual selector so normalization regressions are easy to diagnose.
	}
	if len(client.clickCalls) != 1 || client.clickCalls[0] != "element-123" { // Assert that the click step targets the element returned by FindElement.
		t.Fatalf("expected one click against element-123, got %#v", client.clickCalls) // Surface the actual click targets so execution regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 { // Assert that the executor emitted one running event and one passed event for the successful step.
		t.Fatalf("expected two executor events, got %d", len(*recordedEvents)) // Surface the actual event count so event-emission regressions are easy to diagnose.
	}
	if (*recordedEvents)[0].Status != "running" || (*recordedEvents)[1].Status != "passed" { // Assert the expected event-state transition for a successful click.
		t.Fatalf("expected running then passed events, got %#v", *recordedEvents) // Surface the actual event sequence so event-order regressions are easy to diagnose.
	}
	if (*recordedEvents)[1].Message != "click succeeded" { // Assert the success message so downstream trace consumers keep a stable success marker.
		t.Fatalf("expected click success message, got %q", (*recordedEvents)[1].Message) // Surface the actual message so event-message regressions are easy to diagnose.
	}
}

// TestExecuteSendKeysStepRecordsCalls verifies that a successful sendKeys step resolves one element and forwards the configured text payload.
func TestExecuteSendKeysStepRecordsCalls(t *testing.T) {
	client := &recordingAppiumClient{}                                                                               // Construct one recording Appium client so the test can assert real send-keys side effects.
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Second)                                    // Construct one executor with bounded timings so the unit test remains fast and deterministic.
	plan := []PlanStep{{Type: "sendKeys", Selector: "id=username", Params: json.RawMessage(`{"text":"demo-user"}`)}} // Build one concrete sendKeys plan that exercises selector normalization and param decoding.

	err := executor.Execute(context.Background(), plan) // Execute the real worker plan against the recording Appium client.
	if err != nil {                                     // Fail immediately when the success-path executor unexpectedly returns an error.
		t.Fatalf("expected sendKeys step to succeed, got error: %v", err) // Surface the unexpected executor error so regressions are easy to diagnose.
	}
	if len(client.findCalls) != 1 || client.findCalls[0].strategy != "id" || client.findCalls[0].selector != "username" { // Assert the selector normalization used for id= prefixes.
		t.Fatalf("expected one id lookup for username, got %#v", client.findCalls) // Surface the actual lookup data so selector regressions are easy to diagnose.
	}
	if len(client.sendKeysCalls) != 1 { // Assert that the executor issued exactly one send-keys action for the success path.
		t.Fatalf("expected one sendKeys call, got %d", len(client.sendKeysCalls)) // Surface the actual send-keys count so execution regressions are easy to diagnose.
	}
	if client.sendKeysCalls[0].elementID != "element-123" || client.sendKeysCalls[0].text != "demo-user" { // Assert that the resolved element and decoded text reach Appium unchanged.
		t.Fatalf("expected sendKeys payload element-123/demo-user, got %#v", client.sendKeysCalls[0]) // Surface the actual payload so parameter propagation regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Status != "passed" || (*recordedEvents)[1].Message != "sendKeys succeeded" { // Assert that sendKeys emits the expected terminal success event.
		t.Fatalf("expected sendKeys success events, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestExecuteClearElementStepRecordsCalls verifies that a successful clearElement step resolves one element and forwards the clear action.
func TestExecuteClearElementStepRecordsCalls(t *testing.T) {
	client := &recordingAppiumClient{}                                            // Construct one recording Appium client so the test can assert clear-element side effects precisely.
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Second) // Construct one executor with bounded timings so the unit test remains fast and deterministic.
	plan := []PlanStep{{Type: "clearElement", Selector: "id=username"}}           // Build one concrete clearElement plan that exercises selector normalization and clear dispatch.

	err := executor.Execute(context.Background(), plan) // Execute the real worker plan against the recording Appium client.
	if err != nil {                                     // Fail immediately when the success-path executor unexpectedly returns an error.
		t.Fatalf("expected clearElement step to succeed, got error: %v", err) // Surface the unexpected executor error so regressions are easy to diagnose.
	}
	if len(client.clearCalls) != 1 || client.clearCalls[0] != "element-123" { // Assert that the clear action targets the element returned by FindElement.
		t.Fatalf("expected one clear call against element-123, got %#v", client.clearCalls) // Surface the actual clear targets so execution regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Status != "passed" || (*recordedEvents)[1].Message != "clearElement succeeded" { // Assert that clearElement emits the expected terminal success event.
		t.Fatalf("expected clearElement success events, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestExecuteWaitStepUsesConfiguredDuration verifies that a wait step honors the configured millisecond duration and emits a passed event.
func TestExecuteWaitStepUsesConfiguredDuration(t *testing.T) {
	executor, recordedEvents := newTestExecutor(&recordingAppiumClient{}, time.Second, time.Second) // Construct one executor with no special Appium behavior because wait steps never call Appium.
	plan := []PlanStep{{Type: "wait", Params: json.RawMessage(`{"ms":25}`)}}                        // Build one wait step with a short explicit duration so the test remains quick.
	startedAt := time.Now()                                                                         // Capture the start time so the test can assert that the wait duration was applied.

	err := executor.Execute(context.Background(), plan) // Execute the wait plan through the real executor.
	if err != nil {                                     // Fail immediately when the wait step unexpectedly returns an error.
		t.Fatalf("expected wait step to succeed, got error: %v", err) // Surface the unexpected executor error so regressions are easy to diagnose.
	}
	elapsed := time.Since(startedAt)   // Measure the wall-clock duration so the test can assert that the configured wait was actually honored.
	if elapsed < 20*time.Millisecond { // Allow a small scheduling buffer while still proving that the configured wait happened.
		t.Fatalf("expected wait step to sleep for about 25ms, took %v", elapsed) // Surface the actual elapsed duration so timing regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Status != "passed" || (*recordedEvents)[1].Message != "wait completed" { // Assert the expected wait-step event transition and success message.
		t.Fatalf("expected wait completion events, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestExecuteWaitStepStopsOnCallerCancellation verifies that a long wait does not outlive a cancelled plan context.
func TestExecuteWaitStepStopsOnCallerCancellation(t *testing.T) {
	executor, recordedEvents := newTestExecutor(&recordingAppiumClient{}, 5*time.Second, time.Second) // Construct an executor whose step timeout is much longer than the cancellation window.
	ctx, cancel := context.WithCancel(context.Background())                                           // Create one caller-controlled plan context for deterministic cancellation.
	go func() {                                                                                       // Cancel shortly after execution starts so the wait branch has entered its timer select.
		time.Sleep(25 * time.Millisecond) // Allow the executor to emit its running event before cancellation.
		cancel()                          // Cancel the plan so the wait must stop without sleeping for its requested duration.
	}()
	startedAt := time.Now()                                                                          // Capture wall-clock start time to assert prompt cancellation.
	err := executor.Execute(ctx, []PlanStep{{Type: "wait", Params: json.RawMessage(`{"ms":5000}`)}}) // Execute one five-second wait under the shortly cancelled context.
	if !stdErrors.Is(err, context.Canceled) {                                                        // Require the caller cancellation to propagate unchanged.
		t.Fatalf("expected context cancellation, got %v", err) // Surface masking or delayed completion regressions.
	}
	if elapsed := time.Since(startedAt); elapsed >= time.Second { // Require termination far before the requested five-second delay.
		t.Fatalf("expected cancelled wait to stop promptly, took %v", elapsed) // Surface any reintroduction of uncancellable sleep behavior.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Status != "failed" { // Require replayable running and failed events for the interrupted step.
		t.Fatalf("expected running/failed events, got %#v", *recordedEvents) // Surface missing cancellation observability.
	}
}

// TestExecuteWaitStepStopsAtStepDeadline verifies that the configured step timeout bounds a longer requested wait duration.
func TestExecuteWaitStepStopsAtStepDeadline(t *testing.T) {
	executor, _ := newTestExecutor(&recordingAppiumClient{}, 30*time.Millisecond, time.Second)                        // Construct an executor with a short per-step deadline.
	startedAt := time.Now()                                                                                           // Capture wall-clock start time to assert timeout enforcement.
	err := executor.Execute(context.Background(), []PlanStep{{Type: "wait", Params: json.RawMessage(`{"ms":5000}`)}}) // Execute one five-second wait under the short step timeout.
	if !stdErrors.Is(err, context.DeadlineExceeded) {                                                                 // Require the step deadline to propagate unchanged.
		t.Fatalf("expected step deadline error, got %v", err) // Surface timeout masking regressions.
	}
	if elapsed := time.Since(startedAt); elapsed >= time.Second { // Require termination far before the requested five-second delay.
		t.Fatalf("expected timed-out wait to stop promptly, took %v", elapsed) // Surface any reintroduction of uncancellable wait behavior.
	}
}

// TestExecuteTapStepUsesConfiguredCoordinates verifies that a successful tap step forwards the configured coordinates directly to Appium.
func TestExecuteTapStepUsesConfiguredCoordinates(t *testing.T) {
	client := &recordingAppiumClient{}                                            // Construct one recording Appium client so the test can assert tap gesture dispatch precisely.
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Second) // Construct one executor with bounded timings so the unit test remains fast and deterministic.
	plan := []PlanStep{{Type: "tap", Params: json.RawMessage(`{"x":12,"y":24}`)}} // Build one tap step with explicit coordinates so the gesture payload can be asserted directly.

	err := executor.Execute(context.Background(), plan) // Execute the real worker plan against the recording Appium client.
	if err != nil {                                     // Fail immediately when the success-path executor unexpectedly returns an error.
		t.Fatalf("expected tap step to succeed, got error: %v", err) // Surface the unexpected executor error so regressions are easy to diagnose.
	}
	if len(client.tapCalls) != 1 || client.tapCalls[0].x != 12 || client.tapCalls[0].y != 24 { // Assert that the tap gesture forwarded the configured coordinates unchanged.
		t.Fatalf("expected one tap call at 12,24, got %#v", client.tapCalls) // Surface the actual tap payload so gesture regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Message != "tap succeeded" { // Assert that tap emits the expected terminal success event.
		t.Fatalf("expected tap success events, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestExecuteSwipeStepUsesConfiguredCoordinates verifies that a successful swipe step forwards the configured gesture coordinates and duration.
func TestExecuteSwipeStepUsesConfiguredCoordinates(t *testing.T) {
	client := &recordingAppiumClient{}                                                                                           // Construct one recording Appium client so the test can assert swipe gesture dispatch precisely.
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Second)                                                // Construct one executor with bounded timings so the unit test remains fast and deterministic.
	plan := []PlanStep{{Type: "swipe", Params: json.RawMessage(`{"startX":1,"startY":2,"endX":11,"endY":22,"durationMs":333}`)}} // Build one swipe step with explicit coordinates and duration so the gesture payload can be asserted directly.

	err := executor.Execute(context.Background(), plan) // Execute the real worker plan against the recording Appium client.
	if err != nil {                                     // Fail immediately when the success-path executor unexpectedly returns an error.
		t.Fatalf("expected swipe step to succeed, got error: %v", err) // Surface the unexpected executor error so regressions are easy to diagnose.
	}
	if len(client.swipeCalls) != 1 || client.swipeCalls[0].x1 != 1 || client.swipeCalls[0].y1 != 2 || client.swipeCalls[0].x2 != 11 || client.swipeCalls[0].y2 != 22 || client.swipeCalls[0].durationMs != 333 { // Assert that the swipe gesture forwarded the configured coordinates and duration unchanged.
		t.Fatalf("expected one swipe call with configured payload, got %#v", client.swipeCalls) // Surface the actual swipe payload so gesture regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Message != "swipe succeeded" { // Assert that swipe emits the expected terminal success event.
		t.Fatalf("expected swipe success events, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestExecuteLongPressResolvesElementAndUsesDefaultDuration verifies that a successful longPress step resolves one element and applies the default hold duration when none is supplied.
func TestExecuteLongPressResolvesElementAndUsesDefaultDuration(t *testing.T) {
	client := &recordingAppiumClient{}                                            // Construct one recording Appium client so the test can assert long-press dispatch precisely.
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Second) // Construct one executor with bounded timings so the unit test remains fast and deterministic.
	plan := []PlanStep{{Type: "longPress", Selector: "accessibility=Submit"}}     // Build one longPress step without explicit duration so the executor must apply its default duration.

	err := executor.Execute(context.Background(), plan) // Execute the real worker plan against the recording Appium client.
	if err != nil {                                     // Fail immediately when the success-path executor unexpectedly returns an error.
		t.Fatalf("expected longPress step to succeed, got error: %v", err) // Surface the unexpected executor error so regressions are easy to diagnose.
	}
	if len(client.longPressCalls) != 1 || client.longPressCalls[0].elementID != "element-123" || client.longPressCalls[0].durationMs != 1000 { // Assert that longPress resolved the element and applied the default duration.
		t.Fatalf("expected one longPress call against element-123 with 1000ms, got %#v", client.longPressCalls) // Surface the actual long-press payload so gesture regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Message != "longPress succeeded" { // Assert that longPress emits the expected terminal success event.
		t.Fatalf("expected longPress success events, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestExecutePressBackAndHideKeyboard verifies that non-element device actions execute directly and emit terminal success events.
func TestExecutePressBackAndHideKeyboard(t *testing.T) {
	client := &recordingAppiumClient{}                                            // Construct one recording Appium client so the test can assert direct device action dispatch precisely.
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Second) // Construct one executor with bounded timings so the unit test remains fast and deterministic.
	plan := []PlanStep{{Type: "pressBack"}, {Type: "hideKeyboard"}}               // Build one two-step plan that exercises both direct device-action branches.

	err := executor.Execute(context.Background(), plan) // Execute the real worker plan against the recording Appium client.
	if err != nil {                                     // Fail immediately when the success-path executor unexpectedly returns an error.
		t.Fatalf("expected pressBack/hideKeyboard plan to succeed, got error: %v", err) // Surface the unexpected executor error so regressions are easy to diagnose.
	}
	if client.backCalls != 1 || client.hideKeyboardCalls != 1 { // Assert that both direct device actions reached the Appium client exactly once.
		t.Fatalf("expected one back call and one hideKeyboard call, got back=%d hideKeyboard=%d", client.backCalls, client.hideKeyboardCalls) // Surface the actual call counts so device-action regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 4 || (*recordedEvents)[1].Message != "pressBack succeeded" || (*recordedEvents)[3].Message != "hideKeyboard succeeded" { // Assert that both direct device actions emitted the expected success events in order.
		t.Fatalf("expected pressBack/hideKeyboard success events, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestExecuteClickFailureCapturesFailureEvent verifies that non-transient Appium action failures return immediately and emit a failed event with a screenshot artifact marker.
func TestExecuteClickFailureCapturesFailureEvent(t *testing.T) {
	clickError := stdErrors.New("click failed") // Define one deterministic click failure so the executor's returned error and event message can be asserted precisely.
	client := &recordingAppiumClient{
		clickFunc: func(ctx context.Context, elementID string) error {
			return clickError // Return the deterministic click failure so the executor enters its terminal failure path without retries.
		},
	}
	executor, recordedEvents := newTestExecutor(client, time.Second, 100*time.Millisecond) // Construct one executor with short waits so the failure-path test stays fast.
	plan := []PlanStep{{Type: "click", Selector: "//button[@id='submit']"}}                // Build one click plan that reaches the failing click call immediately.

	err := executor.Execute(context.Background(), plan) // Execute the real worker plan against the failing Appium client.
	if !stdErrors.Is(err, clickError) {                 // Assert that the executor returns the original non-transient click error to the caller.
		t.Fatalf("expected click failure %v, got %v", clickError, err) // Surface the actual error so failure-path regressions are easy to diagnose.
	}
	if client.screenshotCalls != 1 { // Assert that the executor captured one screenshot artifact during failure emission.
		t.Fatalf("expected one screenshot capture on failure, got %d", client.screenshotCalls) // Surface the actual screenshot count so failure-artifact regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Status != "failed" { // Assert the expected running->failed event transition for a terminal click failure.
		t.Fatalf("expected failure events, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
	if len((*recordedEvents)[1].ArtifactRefs) != 1 || (*recordedEvents)[1].ArtifactRefs[0] != "screenshot:captured" { // Assert that failure events expose the screenshot artifact marker expected by downstream consumers.
		t.Fatalf("expected screenshot artifact ref, got %#v", (*recordedEvents)[1].ArtifactRefs) // Surface the actual artifact refs so failure-artifact regressions are easy to diagnose.
	}
}

// TestExecuteUnknownStepTypeReturnsStructuredError verifies that unsupported steps fail with the repository's structured step error code.
func TestExecuteUnknownStepTypeReturnsStructuredError(t *testing.T) {
	executor, recordedEvents := newTestExecutor(&recordingAppiumClient{}, time.Second, time.Second) // Construct one executor because unsupported-step validation happens inside Execute.
	plan := []PlanStep{{Type: "unknown_action", Selector: "//something"}}                           // Build one plan with an unsupported type so the executor enters the structured failure path.

	err := executor.Execute(context.Background(), plan) // Execute the invalid plan through the real executor.
	if err == nil {                                     // Fail immediately when the executor unexpectedly accepts an unsupported step type.
		t.Fatal("expected unknown step type error") // Surface the missing error because unsupported-step handling is the behavior under test.
	}
	if !apperrors.IsCode(err, apperrors.CodeStepUnsupported) { // Assert the repository-standard error code so callers can classify unsupported steps consistently.
		t.Fatalf("expected E.STEP.UNSUPPORTED, got %v", err) // Surface the actual error so error-classification regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Status != "failed" { // Assert that unsupported steps still emit a terminal failed event for traces.
		t.Fatalf("expected failed terminal event, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestExecuteContextCancellationReturnsContextError verifies that executor step contexts respect caller cancellation during element lookup.
func TestExecuteContextCancellationReturnsContextError(t *testing.T) {
	client := &recordingAppiumClient{
		findElementFunc: func(ctx context.Context, strategy string, selector string) (string, error) {
			<-ctx.Done()         // Wait for the executor's step context to be cancelled so the mock simulates a blocked Appium lookup cleanly.
			return "", ctx.Err() // Return the step-context cancellation error so the executor exposes the caller cancellation directly.
		},
	}
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Second) // Construct one executor with bounded timings so the cancellation path remains deterministic.
	ctx, cancel := context.WithCancel(context.Background())                       // Create one caller context whose cancellation should terminate the in-flight executor step.
	cancel()                                                                      // Cancel immediately so the executor observes cancellation on its first lookup attempt.

	err := executor.Execute(ctx, []PlanStep{{Type: "click", Selector: "//button"}}) // Execute one click plan so the executor enters the lookup path and sees the cancelled context.
	if !stdErrors.Is(err, context.Canceled) {                                       // Assert that the executor returns the caller cancellation rather than masking it.
		t.Fatalf("expected context cancellation error, got %v", err) // Surface the actual error so cancellation regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Status != "failed" { // Assert that cancellation still emits a failed terminal event for downstream traces.
		t.Fatalf("expected failure events for cancellation, got %#v", *recordedEvents) // Surface the actual event sequence so trace regressions are easy to diagnose.
	}
}

// TestClickRetriesAfterElementNotFound verifies that element-not-found failures now participate in the executor's outer retry loop after auto-wait exhaustion.
func TestClickRetriesAfterElementNotFound(t *testing.T) {
	client := &recordingAppiumClient{} // Construct one recording Appium client so the test can count lookup retries and final click dispatch precisely.
	findAttempts := 0                  // Track how many outer lookup attempts the executor performed before succeeding.
	client.findElementFunc = func(ctx context.Context, strategy string, selector string) (string, error) {
		findAttempts++        // Count the lookup attempt so the test can prove element-not-found errors now trigger outer retries.
		if findAttempts < 3 { // Return element-not-found twice so the executor must retry beyond the first auto-wait exhaustion.
			return "", apperrors.New(apperrors.CodeAppElemNotFound, "element not ready") // Return the structured element-not-found error that should now be treated as transient.
		}
		return "element-123", nil // Return one stable element id on the third attempt so the executor can complete the click successfully.
	}
	executor, recordedEvents := newTestExecutor(client, time.Second, time.Nanosecond) // Construct one executor with near-zero auto-wait so each outer attempt fails fast when the element is still missing.
	executor.retryMinJitter = 0                                                       // Remove retry sleep so the unit test stays fast while still exercising the outer retry loop.
	executor.retryMaxJitter = 0                                                       // Remove retry sleep so the unit test stays fast while still exercising the outer retry loop.

	err := executor.Execute(context.Background(), []PlanStep{{Type: "click", Selector: "xpath=//button"}}) // Execute one click plan so the executor must retry the missing element until it becomes available.
	if err != nil {                                                                                        // Fail immediately when the executor still treats element-not-found as terminal after the retry-policy change.
		t.Fatalf("expected click step to succeed after element-not-found retries, got error: %v", err) // Surface the unexpected error so transient-classification regressions are easy to diagnose.
	}
	if findAttempts != 3 { // Assert that the outer retry loop performed exactly two retries before the third successful lookup.
		t.Fatalf("expected 3 find attempts, got %d", findAttempts) // Surface the actual retry count so transient-classification regressions are easy to diagnose.
	}
	if len(client.clickCalls) != 1 { // Assert that the click still executed once after the element finally became available.
		t.Fatalf("expected one click call after retries, got %d", len(client.clickCalls)) // Surface the actual click count so retry regressions are easy to diagnose.
	}
	if len(*recordedEvents) != 2 || (*recordedEvents)[1].Status != "passed" { // Assert that the eventual success still emits one terminal passed event.
		t.Fatalf("expected running/passed events after retries, got %#v", *recordedEvents) // Surface the actual event sequence so retry regressions are easy to diagnose.
	}
}

// TestPlanStepJSONRoundTrip verifies that PlanStep JSON serialization preserves type, selector, and params content.
func TestPlanStepJSONRoundTrip(t *testing.T) {
	step := PlanStep{Type: "click", Selector: "//button[@id='submit']", Params: json.RawMessage(`{"timeout":5000}`)} // Build one representative step that exercises every serialized field.
	data, err := json.Marshal(step)                                                                                  // Marshal the step so the round-trip test exercises the real JSON representation.
	if err != nil {                                                                                                  // Fail immediately when the step cannot be marshaled.
		t.Fatalf("failed to marshal plan step: %v", err) // Surface the marshal failure because no round-trip assertions can proceed without JSON output.
	}

	var decoded PlanStep                                   // Allocate the destination step used by the JSON round-trip assertion.
	if err := json.Unmarshal(data, &decoded); err != nil { // Unmarshal the JSON back into a PlanStep so the round-trip can be asserted precisely.
		t.Fatalf("failed to unmarshal plan step: %v", err) // Surface the unmarshal failure because the round-trip contract is the behavior under test.
	}
	if decoded.Type != step.Type || decoded.Selector != step.Selector { // Assert that the top-level scalar fields survive the JSON round-trip unchanged.
		t.Fatalf("expected round-trip type/selector %#v, got %#v", step, decoded) // Surface the actual decoded step so serialization regressions are easy to diagnose.
	}

	var expectedParams map[string]interface{}                            // Allocate the expected params map so the raw JSON can be compared structurally.
	if err := json.Unmarshal(step.Params, &expectedParams); err != nil { // Decode the original params into a generic map so whitespace and ordering do not affect comparisons.
		t.Fatalf("failed to decode expected params: %v", err) // Surface the decode failure because parameter equality is part of the round-trip contract.
	}
	var actualParams map[string]interface{}                               // Allocate the actual params map so the decoded JSON can be compared structurally.
	if err := json.Unmarshal(decoded.Params, &actualParams); err != nil { // Decode the round-tripped params into a generic map so whitespace and ordering do not affect comparisons.
		t.Fatalf("failed to decode actual params: %v", err) // Surface the decode failure because parameter equality is part of the round-trip contract.
	}
	if len(actualParams) != len(expectedParams) || actualParams["timeout"] != expectedParams["timeout"] { // Assert the logical params content rather than raw JSON byte equality.
		t.Fatalf("expected params %#v, got %#v", expectedParams, actualParams) // Surface the actual params so serialization regressions are easy to diagnose.
	}
}
