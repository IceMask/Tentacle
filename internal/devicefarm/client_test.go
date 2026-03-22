// client_test.go verifies the local validation behavior of Device Farm client methods before any AWS API call is attempted.
package devicefarm

import (
	"context"
	"testing"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
)

// TestScheduleRunValidatesRequiredFields verifies that ScheduleRun fails fast on missing local configuration before any SDK call is needed.
func TestScheduleRunValidatesRequiredFields(t *testing.T) {
	client := &Client{cfg: config.DeviceFarmConfig{Mode: "run_api"}}                          // Construct one client without an SDK because this test exercises only the local input-validation path.
	if _, err := client.ScheduleRun(context.Background(), ScheduleRunRequest{}); err == nil { // Attempt to schedule one run with no project, app, or test package so the local validation path fails before SDK usage.
		t.Fatal("expected missing project ARN to fail") // Surface the missing validation failure because callers depend on fast local feedback for malformed requests.
	} else if !errors.IsCode(err, errors.CodeConfigMissing) { // Fail the test when the returned error does not preserve the stable missing-config code.
		t.Fatalf("expected config missing error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}

// TestCreateUploadValidatesNameAndType verifies that CreateUpload rejects missing required local fields before any SDK call is attempted.
func TestCreateUploadValidatesNameAndType(t *testing.T) {
	client := &Client{cfg: config.DeviceFarmConfig{ProjectARN: "arn:aws:devicefarm:project"}}   // Construct one client with a fallback project ARN so the local upload field validation path can be exercised directly.
	if _, err := client.CreateUpload(context.Background(), CreateUploadRequest{}); err == nil { // Attempt to create one upload with no name or type so the local validation path fails before SDK usage.
		t.Fatal("expected missing upload name to fail") // Surface the missing validation failure because callers depend on fast local feedback for malformed requests.
	} else if !errors.IsCode(err, errors.CodePlanInvalid) { // Fail the test when the returned error does not preserve the stable invalid-plan code.
		t.Fatalf("expected plan invalid error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}

// TestGetRunValidatesARN verifies that GetRun rejects an empty ARN before any SDK call is attempted.
func TestGetRunValidatesARN(t *testing.T) {
	client := &Client{}                                                // Construct one client without an SDK because this test exercises only the local ARN-validation path.
	if _, err := client.GetRun(context.Background(), ""); err == nil { // Attempt to read one run with an empty ARN so the local validation path fails before SDK usage.
		t.Fatal("expected empty run ARN to fail") // Surface the missing validation failure because callers depend on fast local feedback for malformed requests.
	} else if !errors.IsCode(err, errors.CodePlanInvalid) { // Fail the test when the returned error does not preserve the stable invalid-plan code.
		t.Fatalf("expected plan invalid error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}
