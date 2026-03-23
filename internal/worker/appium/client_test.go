package appium

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewClient tests client initialization
func TestNewClient(t *testing.T) {
	client := NewClient("http://localhost:4723")

	if client == nil {
		t.Fatal("Expected non-nil client")
	}

	if client.baseURL != "http://localhost:4723" {
		t.Errorf("Expected baseURL to be 'http://localhost:4723', got '%s'", client.baseURL)
	}

	if client.breakerThreshold != 3 {
		t.Errorf("Expected breakerThreshold to be 3, got %d", client.breakerThreshold)
	}

	if client.maxRetries != 2 {
		t.Errorf("Expected maxRetries to be 2, got %d", client.maxRetries)
	}
}

// TestStartSession_Success tests successful session creation
func TestStartSession_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/session" {
			t.Errorf("Expected POST /session, got %s %s", r.Method, r.URL.Path)
		}

		resp := map[string]interface{}{
			"value": map[string]interface{}{
				"sessionId": "test-session-123",
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	caps := map[string]interface{}{
		"platformName": "iOS",
		"deviceName":   "iPhone 13",
	}

	sessionID, err := client.StartSession(context.Background(), caps)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if sessionID != "test-session-123" {
		t.Errorf("Expected sessionID 'test-session-123', got '%s'", sessionID)
	}

	if client.sessionID != "test-session-123" {
		t.Errorf("Expected client.sessionID to be set to 'test-session-123', got '%s'", client.sessionID)
	}
}

// TestStartSession_LegacyFormat tests session creation with legacy response format
func TestStartSession_LegacyFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"sessionId": "legacy-session-456",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	sessionID, err := client.StartSession(context.Background(), map[string]interface{}{})

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if sessionID != "legacy-session-456" {
		t.Errorf("Expected sessionID 'legacy-session-456', got '%s'", sessionID)
	}
}

// TestStartSession_MissingSessionID tests error when sessionId is missing
func TestStartSession_MissingSessionID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"value": map[string]interface{}{},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	_, err := client.StartSession(context.Background(), map[string]interface{}{})

	if err == nil {
		t.Fatal("Expected error for missing sessionId")
	}

	if !strings.Contains(err.Error(), "missing sessionId") {
		t.Errorf("Expected error message to contain 'missing sessionId', got '%v'", err)
	}
}

// TestStartSession_DefaultTimeoutApplied tests that StartSession enforces an internal timeout when caller has no deadline.
func TestStartSession_DefaultTimeoutApplied(t *testing.T) {
	originalTimeout := defaultStartSessionTimeout                   // Save global timeout so the test can restore shared state.
	defaultStartSessionTimeout = 50 * time.Millisecond              // Shrink timeout to keep the test fast and deterministic.
	defer func() { defaultStartSessionTimeout = originalTimeout }() // Restore global timeout after test completion.

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Simulate a slow Appium /session endpoint.
		time.Sleep(200 * time.Millisecond) // Delay beyond test timeout to force deadline handling.
		w.WriteHeader(http.StatusOK)       // Return nominal success if not canceled, though client should timeout first.
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"value": map[string]interface{}{"sessionId": "late-session"},
		}) // Keep response shape valid so failure reason is timeout, not decoding.
	}))
	defer server.Close() // Close test server resources.

	client := NewClient(server.URL) // Build client against the slow test server.
	client.maxRetries = 0           // Disable retries to make timeout path deterministic.

	_, err := client.StartSession(context.Background(), map[string]interface{}{"platformName": "Android"}) // Call without deadline to trigger default timeout.
	if err == nil {                                                                                        // Enforce timeout failure expectation.
		t.Fatal("Expected startSession timeout error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "timeout") { // Assert returned error clearly communicates timeout semantics.
		t.Fatalf("Expected timeout in error message, got: %v", err)
	}
}

// TestStartSession_InvalidCapabilities_MappedToConfigInvalid tests that vendor-prefix capability errors map to config-invalid code.
func TestStartSession_InvalidCapabilities_MappedToConfigInvalid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // Simulate Appium capability validation failure.
		w.WriteHeader(http.StatusBadRequest) // Return HTTP 400 for invalid capability payload.
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"value": map[string]interface{}{
				"error":   "invalid argument",
				"message": "All non-standard capabilities should have a vendor prefix.",
			},
		}) // Match Appium's capability-prefix error shape.
	}))
	defer server.Close() // Close server after assertions.

	client := NewClient(server.URL) // Create test client pointing to mock Appium server.
	client.maxRetries = 0           // Disable retries to keep failure classification straightforward.

	_, err := client.StartSession(context.Background(), map[string]interface{}{"platformName": "Android"}) // Trigger mock validation error.
	if err == nil {                                                                                        // Ensure error path is exercised.
		t.Fatal("Expected capability validation error")
	}
	if !strings.Contains(err.Error(), "E.CONFIG.INVALID") { // Assert mapped internal code is configuration-invalid.
		t.Fatalf("Expected E.CONFIG.INVALID, got: %v", err)
	}
}

// TestDeleteSession tests session deletion
func TestDeleteSession(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" && strings.Contains(r.URL.Path, "/session/") {
			deleted = true
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"value": nil})
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session-123"

	err := client.DeleteSession(context.Background())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !deleted {
		t.Error("Expected DELETE request to be made")
	}

	if client.sessionID != "" {
		t.Errorf("Expected sessionID to be cleared, got '%s'", client.sessionID)
	}
}

// TestDeleteSession_NoSession tests deleting when no session exists
func TestDeleteSession_NoSession(t *testing.T) {
	client := NewClient("http://localhost:4723")

	err := client.DeleteSession(context.Background())
	if err != nil {
		t.Errorf("Expected no error when deleting non-existent session, got %v", err)
	}
}

// TestFindElement_Success tests successful element finding
func TestFindElement_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/element") {
			resp := map[string]interface{}{
				"value": map[string]string{
					"element-6066-11e4-a52e-4f735466cecf": "element-123",
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"

	elementID, err := client.FindElement(context.Background(), "id", "loginButton")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if elementID != "element-123" {
		t.Errorf("Expected elementID 'element-123', got '%s'", elementID)
	}
}

// TestFindElement_NoSession tests finding element without session
func TestFindElement_NoSession(t *testing.T) {
	client := NewClient("http://localhost:4723")

	_, err := client.FindElement(context.Background(), "id", "loginButton")
	if err == nil {
		t.Fatal("Expected error when finding element without session")
	}

	if !strings.Contains(err.Error(), "session not started") {
		t.Errorf("Expected error about session, got '%v'", err)
	}
}

// TestFindElement_NotFound tests element not found scenario
func TestFindElement_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		resp := map[string]interface{}{
			"value": map[string]interface{}{
				"error":   "no such element",
				"message": "Element not found",
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"

	_, err := client.FindElement(context.Background(), "id", "nonExistent")
	if err == nil {
		t.Fatal("Expected error when element not found")
	}
}

// TestClick_Success tests successful click
func TestClick_Success(t *testing.T) {
	clicked := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/click") {
			clicked = true
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"value": nil})
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"

	err := client.Click(context.Background(), "element-123")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !clicked {
		t.Error("Expected click request to be made")
	}
}

// TestSendKeys_Success tests successful text input
func TestSendKeys_Success(t *testing.T) {
	var receivedText string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/value") {
			var payload map[string]interface{}
			json.NewDecoder(r.Body).Decode(&payload)
			receivedText = payload["text"].(string)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"value": nil})
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"

	testText := "Hello, World!"
	err := client.SendKeys(context.Background(), "element-123", testText)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if receivedText != testText {
		t.Errorf("Expected text '%s', got '%s'", testText, receivedText)
	}
}

// TestScreenshot_Success tests successful screenshot
func TestScreenshot_Success(t *testing.T) {
	// Base64 encoded "test-image-data"
	testImageData := "dGVzdC1pbWFnZS1kYXRh"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/screenshot") {
			resp := map[string]interface{}{
				"value": testImageData,
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"

	data, err := client.Screenshot(context.Background())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expected := "test-image-data"
	if string(data) != expected {
		t.Errorf("Expected decoded data '%s', got '%s'", expected, string(data))
	}
}

// TestPageSource_Success tests successful page source retrieval
func TestPageSource_Success(t *testing.T) {
	expectedSource := "<xml><node id='test'/></xml>"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/source") {
			resp := map[string]interface{}{
				"value": expectedSource,
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"

	source, err := client.PageSource(context.Background())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if source != expectedSource {
		t.Errorf("Expected source '%s', got '%s'", expectedSource, source)
	}
}

// TestRetryOn5xx tests retry mechanism on 5xx errors
func TestRetryOn5xx(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"value": map[string]interface{}{
					"error":   "internal error",
					"message": "Server error",
				},
			})
		} else {
			// Succeed on 3rd attempt
			resp := map[string]interface{}{
				"value": map[string]string{
					"element-6066-11e4-a52e-4f735466cecf": "element-123",
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"
	client.minRetryDelay = 10 * time.Millisecond // Speed up test

	elementID, err := client.FindElement(context.Background(), "id", "test")
	if err != nil {
		t.Fatalf("Expected successful retry, got error: %v", err)
	}

	if elementID != "element-123" {
		t.Errorf("Expected elementID 'element-123', got '%s'", elementID)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

// TestCircuitBreaker tests circuit breaker functionality
func TestCircuitBreaker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"value": map[string]interface{}{
				"error":   "internal error",
				"message": "Server error",
			},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"
	client.breakerThreshold = 2
	client.breakerOpenFor = 1 * time.Second
	client.maxRetries = 0 // Disable retries for this test

	// First failure
	_, err1 := client.FindElement(context.Background(), "id", "test")
	if err1 == nil {
		t.Fatal("Expected error on first request")
	}

	// Second failure - should trip circuit breaker
	_, err2 := client.FindElement(context.Background(), "id", "test")
	if err2 == nil {
		t.Fatal("Expected error on second request")
	}

	// Third request should be blocked by circuit breaker
	_, err3 := client.FindElement(context.Background(), "id", "test")
	if err3 == nil {
		t.Fatal("Expected circuit breaker to block request")
	}
	if !strings.Contains(err3.Error(), "circuit breaker") {
		t.Errorf("Expected circuit breaker error, got: %v", err3)
	}

	// Wait for breaker to reset
	time.Sleep(1100 * time.Millisecond)

	// Should allow request again (even though it will fail)
	_, err4 := client.FindElement(context.Background(), "id", "test")
	if err4 == nil {
		t.Fatal("Expected error after breaker reset")
	}
	// Should not be a circuit breaker error this time
	if strings.Contains(err4.Error(), "circuit breaker") {
		t.Errorf("Expected non-breaker error after reset, got: %v", err4)
	}
}

// TestTimeout tests request timeout handling
func TestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate slow server
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"
	client.httpClient.Timeout = 100 * time.Millisecond
	client.maxRetries = 0 // Disable retries

	ctx := context.Background()
	_, err := client.FindElement(ctx, "id", "test")

	if err == nil {
		t.Fatal("Expected timeout error")
	}
}

// TestErrorMapping tests Appium error to internal error code mapping
func TestErrorMapping(t *testing.T) {
	tests := []struct {
		name          string
		statusCode    int
		errorType     string
		errorMessage  string
		expectedError string
	}{
		{
			name:          "Element not found",
			statusCode:    http.StatusNotFound,
			errorType:     "no such element",
			errorMessage:  "Element could not be found",
			expectedError: "ELEM_NOT_FOUND",
		},
		{
			name:          "Invalid session",
			statusCode:    http.StatusNotFound,
			errorType:     "invalid session id",
			errorMessage:  "Session not found",
			expectedError: "SESSION_DEAD",
		},
		{
			name:          "Timeout",
			statusCode:    http.StatusRequestTimeout,
			errorType:     "timeout",
			errorMessage:  "Operation timed out",
			expectedError: "TIMEOUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				resp := map[string]interface{}{
					"value": map[string]interface{}{
						"error":   tt.errorType,
						"message": tt.errorMessage,
					},
				}
				json.NewEncoder(w).Encode(resp)
			}))
			defer server.Close()

			client := NewClient(server.URL)
			client.sessionID = "test-session"
			client.maxRetries = 0

			_, err := client.FindElement(context.Background(), "id", "test")
			if err == nil {
				t.Fatal("Expected error")
			}

			errMsg := fmt.Sprintf("%v", err)
			if !strings.Contains(errMsg, tt.errorMessage) {
				t.Errorf("Expected error to contain '%s', got: %v", tt.errorMessage, err)
			}
		})
	}
}

// TestConcurrentRequests tests that client handles concurrent requests safely
func TestConcurrentRequests(t *testing.T) {
	var requestCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1) // Count handled requests atomically so the assertion remains stable under concurrent goroutines.
		resp := map[string]interface{}{
			"value": "<xml/>",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.sessionID = "test-session"

	// Run multiple concurrent requests
	concurrency := 10
	done := make(chan bool, concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			_, err := client.PageSource(context.Background())
			if err != nil {
				t.Errorf("Concurrent request failed: %v", err)
			}
			done <- true
		}()
	}

	// Wait for all requests to complete
	for i := 0; i < concurrency; i++ {
		<-done
	}

	if got := int(requestCount.Load()); got != concurrency {
		t.Errorf("Expected %d requests, got %d", concurrency, got)
	}
}
