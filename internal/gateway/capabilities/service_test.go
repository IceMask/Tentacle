// service_test.go verifies the externally advertised capability snapshot returned by the gateway discovery service.
package capabilities

import (
	"context"
	"testing"

	"mcp_for_appium/internal/config"
)

// TestListReturnsExpectedDiscoverySnapshot verifies that capability discovery exposes the expected high-level v1 release posture.
func TestListReturnsExpectedDiscoverySnapshot(t *testing.T) {
	service := NewService(config.GatewayConfig{}) // Construct one capability service with a default gateway config because the current discovery snapshot is static.
	payload := service.List(context.Background()) // Read the production discovery snapshot so the externally advertised feature set can be asserted directly.
	if payload["apiVersion"] != "4.3.0" {         // Fail the test when the service no longer advertises the expected compatibility API version.
		t.Fatalf("expected apiVersion 4.3.0, got %#v", payload["apiVersion"]) // Surface the unexpected version so discovery regressions are obvious.
	}
	capabilitiesPayload, ok := payload["capabilities"].(map[string]interface{}) // Decode the nested capability envelope so individual feature flags can be asserted directly.
	if !ok {                                                                    // Fail the test when the discovery payload does not expose the expected envelope shape.
		t.Fatalf("expected capabilities envelope, got %#v", payload["capabilities"]) // Surface the unexpected payload shape so discovery regressions are easy to diagnose.
	}
	if capabilitiesPayload["recommendedExecutionMode"] != "monolith" { // Fail the test when the gateway no longer advertises monolith as the recommended execution mode.
		t.Fatalf("expected recommendedExecutionMode monolith, got %#v", capabilitiesPayload["recommendedExecutionMode"]) // Surface the unexpected execution mode so release-posture regressions are obvious.
	}
	if capabilitiesPayload["toolCount"] != 26 { // Fail the test when the advertised tool count drifts away from the current registry contract.
		t.Fatalf("expected toolCount 26, got %#v", capabilitiesPayload["toolCount"]) // Surface the unexpected tool count so discovery regressions are obvious.
	}
}
