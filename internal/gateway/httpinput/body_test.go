// body_test.go verifies gateway request-body size enforcement, strict JSON framing, and HMAC-safe body restoration.
package httpinput

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestLimitBodyRejectsDeclaredOversize verifies that known oversized requests stop before downstream authentication or dispatch.
func TestLimitBodyRejectsDeclaredOversize(t *testing.T) {
	called := false                                                                      // Track downstream execution so the test can prove early rejection rather than only checking the status.
	handler := LimitBody(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { // Wrap one sentinel handler with the production body limiter.
		called = true                       // Record any unsafe downstream invocation after the oversized body declaration.
		w.WriteHeader(http.StatusNoContent) // Return a distinct status if the limiter accidentally forwards the request.
	}), 4)
	request := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader("12345")) // Build a five-byte request against a four-byte ceiling.
	recorder := httptest.NewRecorder()                                                      // Capture the limiter response for status assertions.
	handler.ServeHTTP(recorder, request)                                                    // Execute the oversized request through the production limiter.
	if recorder.Code != http.StatusRequestEntityTooLarge {                                  // Require the standard HTTP 413 transport classification.
		t.Fatalf("expected status %d, got %d", http.StatusRequestEntityTooLarge, recorder.Code) // Surface the unexpected status for quick diagnosis.
	}
	if called { // Reject any downstream invocation because authentication must not read known oversized payloads.
		t.Fatal("expected oversized request to stop before downstream handler") // Surface an ordering regression in the gateway middleware stack.
	}
}

// TestDecodeSingleJSONRejectsTrailingDocument verifies that concatenated JSON values cannot smuggle a second request past transport validation.
func TestDecodeSingleJSONRejectsTrailingDocument(t *testing.T) {
	var destination map[string]interface{}                                                    // Allocate a generic destination matching the first valid JSON object.
	err := DecodeSingleJSON(strings.NewReader(`{"first":true}{"second":true}`), &destination) // Decode two concatenated documents through the strict production helper.
	if err == nil {                                                                           // Reject successful decoding because only one top-level document is allowed per request.
		t.Fatal("expected trailing JSON document to be rejected") // Surface framing regressions that could create parser differentials.
	}
}

// TestReadAndRestoreBodyEnforcesLimit verifies that direct HMAC body cloning cannot allocate beyond its configured ceiling.
func TestReadAndRestoreBodyEnforcesLimit(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("12345")) // Build a body one byte beyond the direct clone limit.
	_, err := ReadAndRestoreBody(request, 4)                                            // Exercise the production body clone without an outer MaxBytesReader.
	if !IsBodyTooLarge(err) {                                                           // Require a typed size error so auth can map it to HTTP 413.
		t.Fatalf("expected body-too-large error, got %v", err) // Surface accidental generic authentication classification.
	}
}
