package appium

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client 是一个轻量级的 Appium HTTP 客户端
type Client struct {
	baseURL    string
	httpClient *http.Client
	sessionID  string
}

// NewClient 创建客户端
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// Request 通用请求方法
func (c *Client) Request(ctx context.Context, method, path string, body interface{}) (json.RawMessage, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("appium error %d: %s", resp.StatusCode, string(respBody))
	}

	// Appium 响应格式通常是 {"value": ...}
	var result struct {
		Value json.RawMessage `json:"value"`
	}

	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	return result.Value, nil
}

// SetSessionID 设置会话ID
func (c *Client) SetSessionID(sessionID string) {
	c.sessionID = sessionID
}

// GetSessionID 获取会话ID
func (c *Client) GetSessionID() string {
	return c.sessionID
}
