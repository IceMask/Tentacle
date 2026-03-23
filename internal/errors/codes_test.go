// codes_test.go verifies the transport-mapping helpers for standardized repository errors.
package errors

import (
	"testing"
)

// TestMapToHTTPUsesExpectedStatusCodes verifies that representative internal error codes map to the intended HTTP statuses.
func TestMapToHTTPUsesExpectedStatusCodes(t *testing.T) {
	if got := MapToHTTP(New(CodePermissionDenied, "forbidden")); got != 403 { // Map one permission error through the HTTP helper so auth failures remain transport-stable.
		t.Fatalf("expected 403 for permission denied, got %d", got) // Surface the unexpected HTTP status so external API regressions are obvious.
	}
	if got := MapToHTTP(New(CodeTimeoutPlan, "timeout")); got != 504 { // Map one timeout error through the HTTP helper so plan-timeout transport behavior stays stable.
		t.Fatalf("expected 504 for plan timeout, got %d", got) // Surface the unexpected HTTP status so external API regressions are obvious.
	}
}

// TestMapToJSONRPCIncludesInternalCode verifies that JSON-RPC mapping preserves the standardized internal error code for machine handling.
func TestMapToJSONRPCIncludesInternalCode(t *testing.T) {
	jsonErr := MapToJSONRPC(New(CodePlanInvalid, "bad plan")) // Map one invalid-plan error through the JSON-RPC helper so structured error metadata can be asserted directly.
	if jsonErr.Code != -32602 {                               // Fail the test when invalid-plan errors do not map to the JSON-RPC invalid-params code.
		t.Fatalf("expected JSON-RPC code -32602, got %d", jsonErr.Code) // Surface the unexpected transport code so JSON-RPC regressions are obvious.
	}
	data, ok := jsonErr.Data.(map[string]interface{}) // Decode the structured data payload emitted by the helper so the internal code can be inspected directly.
	if !ok {                                          // Fail the test when the helper does not expose the expected structured data shape.
		t.Fatalf("expected structured JSON-RPC data payload, got %#v", jsonErr.Data) // Surface the unexpected payload so regression diagnosis is straightforward.
	}
	if data["internalCode"] != string(CodePlanInvalid) { // Fail the test when the internal code is lost during JSON-RPC mapping.
		t.Fatalf("expected internal code %q, got %#v", CodePlanInvalid, data["internalCode"]) // Surface the unexpected internal code so machine-handling regressions are obvious.
	}
}

// TestIsCodeMatchesWrappedErrors verifies that the convenience helper recognizes wrapped repository errors by their standardized code.
func TestIsCodeMatchesWrappedErrors(t *testing.T) {
	err := Wrap(CodeStoreRead, "read failed", New(CodeTraceNotFound, "missing")) // Build one wrapped repository error so the helper can be exercised against the outer standardized code.
	if !IsCode(err, CodeStoreRead) {                                             // Fail the test when the helper does not recognize the wrapped outer error code.
		t.Fatal("expected wrapped error to match E.STORE.READ") // Surface the missing code match so transport-mapping regressions are obvious.
	}
	if IsCode(err, CodeTraceNotFound) { // Fail the test when the helper incorrectly treats the wrapped inner error code as the direct outer code.
		t.Fatal("expected wrapped error not to match nested E.TRACE.NOT_FOUND directly") // Surface the overly-permissive code match so helper semantics remain explicit.
	}
}
