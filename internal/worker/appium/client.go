package appium

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"mcp_for_appium/internal/errors"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	sessionID  string
}

func NewClient(url string) *Client {
	return &Client{
		baseURL: url,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:       10,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: true,
			},
		},
	}
}

func (c *Client) StartSession(ctx context.Context, caps map[string]interface{}) (string, error) {
	payload := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"alwaysMatch": caps,
		},
	}

	var resp struct {
		Value struct {
			SessionID string `json:"sessionId"`
		} `json:"value"`
	}

	if err := c.do(ctx, "POST", "/session", payload, &resp); err != nil {
		return "", err
	}

	c.sessionID = resp.Value.SessionID
	return c.sessionID, nil
}

func (c *Client) DeleteSession(ctx context.Context) error {
	if c.sessionID == "" {
		return nil
	}
	return c.do(ctx, "DELETE", "/session/"+c.sessionID, nil, nil)
}

func (c *Client) FindElement(ctx context.Context, strategy, selector string) (string, error) {
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

func (c *Client) Click(ctx context.Context, elementID string) error {
	return c.do(ctx, "POST", "/session/"+c.sessionID+"/element/"+elementID+"/click", map[string]interface{}{}, nil)
}

func (c *Client) do(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return errors.Wrap(errors.CodeInternal, "failed to marshal body", err)
		}
		reqBody = bytes.NewBuffer(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return errors.Wrap(errors.CodeInternal, "failed to create request", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Retry logic could be here
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(errors.CodeStoreConn, "appium request failed", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		// Read error body
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == 404 {
			return errors.New(errors.CodeAppElemNotFound, string(b))
		}
		return errors.New(errors.CodeInternal, fmt.Sprintf("appium error %d: %s", resp.StatusCode, string(b)))
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return errors.Wrap(errors.CodeInternal, "failed to decode response", err)
		}
	}
	return nil
}
