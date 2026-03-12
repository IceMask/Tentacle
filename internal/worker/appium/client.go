package appium

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"

	stdErrors "errors"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/telemetry"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	sessionID  string

	mu                  sync.Mutex
	consecutiveFailures int
	breakerOpenUntil    time.Time
	breakerThreshold    int
	breakerOpenFor      time.Duration
	maxRetries          int
	minRetryDelay       time.Duration
	maxRetryDelay       time.Duration
}

// NewClient executes this operation.
func NewClient(url string) *Client {
	return &Client{
		baseURL: url,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 200,
				IdleConnTimeout:     30 * time.Second,
				DisableCompression:  true,
			},
		},
		breakerThreshold: 3,
		breakerOpenFor:   10 * time.Second,
		maxRetries:       2,
		minRetryDelay:    200 * time.Millisecond,
		maxRetryDelay:    1200 * time.Millisecond,
	}
}

// StartSession executes this operation.
func (c *Client) StartSession(ctx context.Context, caps map[string]interface{}) (string, error) {
	payload := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"alwaysMatch": caps,
		},
	}

	var resp struct {
		SessionID string `json:"sessionId"`
		Value     struct {
			SessionID string `json:"sessionId"`
		} `json:"value"`
	}

	if err := c.do(ctx, "POST", "/session", payload, &resp); err != nil {
		return "", err
	}

	if resp.Value.SessionID != "" {
		c.sessionID = resp.Value.SessionID
	} else {
		c.sessionID = resp.SessionID
	}
	if c.sessionID == "" {
		return "", errors.New(errors.CodeInternal, "missing sessionId in Appium response")
	}
	return c.sessionID, nil
}

// DeleteSession executes this operation.
func (c *Client) DeleteSession(ctx context.Context) error {
	if c.sessionID == "" {
		return nil
	}
	if err := c.do(ctx, "DELETE", "/session/"+c.sessionID, nil, nil); err != nil {
		return err
	}
	c.sessionID = ""
	return nil
}

// AttachSession binds the client to an existing Appium session id.
// This is used by orchestrator recovery paths after process restart.
func (c *Client) AttachSession(sessionID string) {
	c.sessionID = sessionID
}

// FindElement executes this operation.
func (c *Client) FindElement(ctx context.Context, strategy, selector string) (string, error) {
	if err := c.ensureSession(); err != nil {
		return "", err
	}
	payload := map[string]string{
		"using": strategy,
		"value": selector,
	}

	var resp struct {
		Value map[string]string `json:"value"` // Element ID is in value
	}

	if err := c.do(ctx, "POST", "/session/"+c.sessionID+"/element", payload, &resp); err != nil {
		return "", err
	}

	// Extract element ID (key varies by protocol, usually element-6066-11e4-a52e-4f735466cecf)
	for _, v := range resp.Value {
		return v, nil
	}
	return "", errors.New(errors.CodeAppElemNotFound, "element id not found in response")
}

// Click executes this operation.
func (c *Client) Click(ctx context.Context, elementID string) error {
	if err := c.ensureSession(); err != nil {
		return err
	}
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/element/"+elementID+"/click", map[string]interface{}{}, nil)
}

// SendKeys executes this operation.
func (c *Client) SendKeys(ctx context.Context, elementID, text string) error {
	if err := c.ensureSession(); err != nil {
		return err
	}
	payload := map[string]interface{}{
		"text":  text,
		"value": []string{text},
	}
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/element/"+elementID+"/value", payload, nil)
}

// Screenshot executes this operation.
func (c *Client) Screenshot(ctx context.Context) ([]byte, error) {
	if err := c.ensureSession(); err != nil {
		return nil, err
	}
	var resp struct {
		Value string `json:"value"`
	}
	if err := c.do(ctx, "GET", "/session/"+c.sessionID+"/screenshot", nil, &resp); err != nil {
		return nil, err
	}
	data, err := base64.StdEncoding.DecodeString(resp.Value)
	if err != nil {
		return nil, errors.Wrap(errors.CodeInternal, "failed to decode screenshot", err)
	}
	return data, nil
}

// PageSource executes this operation.
func (c *Client) PageSource(ctx context.Context) (string, error) {
	if err := c.ensureSession(); err != nil {
		return "", err
	}
	var resp struct {
		Value string `json:"value"`
	}
	if err := c.do(ctx, "GET", "/session/"+c.sessionID+"/source", nil, &resp); err != nil {
		return "", err
	}
	return resp.Value, nil
}

// Clear clears the text content of an element
func (c *Client) Clear(ctx context.Context, elementID string) error {
	if err := c.ensureSession(); err != nil {
		return err
	}
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/element/"+elementID+"/clear", map[string]interface{}{}, nil)
}

// GetText retrieves the visible text of an element
func (c *Client) GetText(ctx context.Context, elementID string) (string, error) {
	if err := c.ensureSession(); err != nil {
		return "", err
	}
	var resp struct {
		Value string `json:"value"`
	}
	if err := c.do(ctx, "GET", "/session/"+c.sessionID+"/element/"+elementID+"/text", nil, &resp); err != nil {
		return "", err
	}
	return resp.Value, nil
}

// GetAttribute retrieves an attribute value of an element
func (c *Client) GetAttribute(ctx context.Context, elementID, attribute string) (string, error) {
	if err := c.ensureSession(); err != nil {
		return "", err
	}
	var resp struct {
		Value string `json:"value"`
	}
	if err := c.do(ctx, "GET", "/session/"+c.sessionID+"/element/"+elementID+"/attribute/"+attribute, nil, &resp); err != nil {
		return "", err
	}
	return resp.Value, nil
}

// IsDisplayed checks if an element is visible
func (c *Client) IsDisplayed(ctx context.Context, elementID string) (bool, error) {
	if err := c.ensureSession(); err != nil {
		return false, err
	}
	var resp struct {
		Value bool `json:"value"`
	}
	if err := c.do(ctx, "GET", "/session/"+c.sessionID+"/element/"+elementID+"/displayed", nil, &resp); err != nil {
		return false, err
	}
	return resp.Value, nil
}

// Tap performs a tap action at coordinates (for touch actions)
func (c *Client) Tap(ctx context.Context, x, y int) error {
	if err := c.ensureSession(); err != nil {
		return err
	}
	payload := map[string]interface{}{
		"actions": []map[string]interface{}{
			{
				"type": "pointer",
				"id":   "finger1",
				"parameters": map[string]string{
					"pointerType": "touch",
				},
				"actions": []map[string]interface{}{
					{
						"type":     "pointerMove",
						"duration": 0,
						"x":        x,
						"y":        y,
					},
					{
						"type":   "pointerDown",
						"button": 0,
					},
					{
						"type":   "pointerUp",
						"button": 0,
					},
				},
			},
		},
	}
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/actions", payload, nil)
}

// Swipe performs a swipe gesture from (x1, y1) to (x2, y2)
func (c *Client) Swipe(ctx context.Context, x1, y1, x2, y2, durationMs int) error {
	if err := c.ensureSession(); err != nil {
		return err
	}
	payload := map[string]interface{}{
		"actions": []map[string]interface{}{
			{
				"type": "pointer",
				"id":   "finger1",
				"parameters": map[string]string{
					"pointerType": "touch",
				},
				"actions": []map[string]interface{}{
					{
						"type":     "pointerMove",
						"duration": 0,
						"x":        x1,
						"y":        y1,
					},
					{
						"type":   "pointerDown",
						"button": 0,
					},
					{
						"type":     "pointerMove",
						"duration": durationMs,
						"x":        x2,
						"y":        y2,
					},
					{
						"type":   "pointerUp",
						"button": 0,
					},
				},
			},
		},
	}
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/actions", payload, nil)
}

// LongPress performs a long press on an element
func (c *Client) LongPress(ctx context.Context, elementID string, durationMs int) error {
	if err := c.ensureSession(); err != nil {
		return err
	}
	payload := map[string]interface{}{
		"actions": []map[string]interface{}{
			{
				"type": "pointer",
				"id":   "finger1",
				"parameters": map[string]string{
					"pointerType": "touch",
				},
				"actions": []map[string]interface{}{
					{
						"type":     "pointerMove",
						"duration": 0,
						"origin": map[string]string{
							"element-6066-11e4-a52e-4f735466cecf": elementID,
						},
					},
					{
						"type":   "pointerDown",
						"button": 0,
					},
					{
						"type":     "pause",
						"duration": durationMs,
					},
					{
						"type":   "pointerUp",
						"button": 0,
					},
				},
			},
		},
	}
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/actions", payload, nil)
}

// Back presses the device back button
func (c *Client) Back(ctx context.Context) error {
	if err := c.ensureSession(); err != nil {
		return err
	}
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/back", map[string]interface{}{}, nil)
}

// HideKeyboard hides the on-screen keyboard
func (c *Client) HideKeyboard(ctx context.Context) error {
	if err := c.ensureSession(); err != nil {
		return err
	}
	// Try both strategies (Android and iOS differ)
	payload := map[string]interface{}{
		"strategy": "tapOutside",
	}
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/appium/device/hide_keyboard", payload, nil)
}

// do executes this operation.
func (c *Client) do(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	if err := c.checkBreaker(); err != nil {
		return err
	}

	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return errors.Wrap(errors.CodeInternal, "failed to marshal body", err)
		}
		payload = b
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if err := c.checkBreaker(); err != nil {
			return err
		}

		var reqBody io.Reader
		if payload != nil {
			reqBody = bytes.NewReader(payload)
		}

		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
		if err != nil {
			return errors.Wrap(errors.CodeInternal, "failed to create request", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		start := time.Now()
		resp, err := c.httpClient.Do(req)
		telemetry.AppiumRTT.WithLabelValues("", c.baseURL).Observe(float64(time.Since(start).Milliseconds()))
		if err != nil {
			lastErr = c.mapTransportError(err)
			if c.shouldRetry(nil, err, attempt) {
				continue
			}
			c.noteFailure()
			return lastErr
		}

		b, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = errors.Wrap(errors.CodeInternal, "failed to read response body", readErr)
			if c.shouldRetry(resp, readErr, attempt) {
				continue
			}
			c.noteFailure()
			return lastErr
		}

		if resp.StatusCode >= 400 {
			lastErr = c.mapAppiumError(resp.StatusCode, b)
			if c.shouldRetry(resp, nil, attempt) {
				continue
			}
			c.noteFailure()
			return lastErr
		}

		if result != nil {
			if err := json.Unmarshal(b, result); err != nil {
				lastErr = errors.Wrap(errors.CodeInternal, "failed to decode response", err)
				if c.shouldRetry(resp, err, attempt) {
					continue
				}
				c.noteFailure()
				return lastErr
			}
		}

		c.noteSuccess()
		return nil
	}

	if lastErr == nil {
		lastErr = errors.New(errors.CodeInternal, "appium request failed without response")
	}
	c.noteFailure()
	return lastErr
}

// ensureSession executes this operation.
func (c *Client) ensureSession() error {
	if c.sessionID == "" {
		return errors.New(errors.CodeSessionNotFound, "appium session not started")
	}
	return nil
}

// mapTransportError executes this operation.
func (c *Client) mapTransportError(err error) error {
	if stdErrors.Is(err, context.DeadlineExceeded) {
		return errors.Wrap(errors.CodeAppTimeout, "appium request timeout", err)
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		return errors.Wrap(errors.CodeAppTimeout, "appium request timeout", err)
	}
	return errors.Wrap(errors.CodeStoreConn, "appium request failed", err)
}

// mapAppiumError executes this operation.
func (c *Client) mapAppiumError(status int, body []byte) error {
	msg := strings.TrimSpace(string(body))
	var appiumResp struct {
		Value struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		} `json:"value"`
	}
	if err := json.Unmarshal(body, &appiumResp); err == nil && appiumResp.Value.Error != "" {
		if appiumResp.Value.Message != "" {
			msg = appiumResp.Value.Message
		}
		switch strings.ToLower(appiumResp.Value.Error) {
		case "no such element":
			return errors.New(errors.CodeAppElemNotFound, msg)
		case "invalid session id":
			return errors.New(errors.CodeSessionDead, msg)
		case "timeout", "script timeout":
			return errors.New(errors.CodeAppTimeout, msg)
		}
	}

	switch status {
	case http.StatusNotFound:
		return errors.New(errors.CodeAppElemNotFound, msg)
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return errors.New(errors.CodeAppTimeout, msg)
	}

	return errors.New(errors.CodeInternal, fmt.Sprintf("appium error %d: %s", status, msg))
}

// shouldRetry executes this operation.
func (c *Client) shouldRetry(resp *http.Response, err error, attempt int) bool {
	if attempt >= c.maxRetries {
		return false
	}
	if err != nil {
		if stdErrors.Is(err, syscall.ECONNRESET) || stdErrors.Is(err, syscall.EPIPE) || stdErrors.Is(err, io.EOF) {
			c.backoff(attempt)
			return true
		}
		if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
			c.backoff(attempt)
			return true
		}
		return false
	}
	if resp == nil {
		return false
	}
	if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
		c.backoff(attempt)
		return true
	}
	return false
}

// backoff executes this operation.
func (c *Client) backoff(attempt int) {
	delay := c.minRetryDelay
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= c.maxRetryDelay {
			delay = c.maxRetryDelay
			break
		}
	}
	time.Sleep(delay)
}

// checkBreaker executes this operation.
func (c *Client) checkBreaker() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.breakerOpenUntil.IsZero() {
		return nil
	}
	if time.Now().Before(c.breakerOpenUntil) {
		return errors.New(errors.CodeHealthDown, "appium circuit breaker open")
	}

	c.consecutiveFailures = 0
	c.breakerOpenUntil = time.Time{}
	return nil
}

// noteFailure executes this operation.
func (c *Client) noteFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.consecutiveFailures++
	if c.consecutiveFailures >= c.breakerThreshold {
		c.breakerOpenUntil = time.Now().Add(c.breakerOpenFor)
	}
}

// noteSuccess executes this operation.
func (c *Client) noteSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.consecutiveFailures = 0
	c.breakerOpenUntil = time.Time{}
}
