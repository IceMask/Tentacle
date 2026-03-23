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

// TestListRemovesADBShellWhenDisabled verifies that the operator-level adbShell kill switch removes the tool from discovery output entirely.
func TestListRemovesADBShellWhenDisabled(t *testing.T) {
	service := NewService(config.GatewayConfig{DisableADBShellTool: true})      // Construct one capability service with adbShell explicitly disabled so discovery shaping can be asserted directly.
	payload := service.List(context.Background())                               // Read the production discovery snapshot so the disabled-tool view can be asserted directly.
	capabilitiesPayload, ok := payload["capabilities"].(map[string]interface{}) // Decode the nested capability envelope so the filtered tool list can be inspected directly.
	if !ok {                                                                    // Fail the test when the discovery payload does not expose the expected envelope shape.
		t.Fatalf("expected capabilities envelope, got %#v", payload["capabilities"]) // Surface the unexpected payload shape so discovery regressions are easy to diagnose.
	}
	if capabilitiesPayload["toolCount"] != 25 { // Fail the test when disabling adbShell does not reduce the externally advertised tool count by one.
		t.Fatalf("expected toolCount 25 when adbShell is disabled, got %#v", capabilitiesPayload["toolCount"]) // Surface the unexpected tool count so disablement regressions are obvious.
	}
	toolNames, ok := capabilitiesPayload["toolNames"].([]string) // Decode the typed tool-name slice so the disabled tool can be checked directly.
	if !ok {                                                     // Fall back to generic decoding failures because interface{} payloads from map literals must stay predictable.
		t.Fatalf("expected []string toolNames, got %#v", capabilitiesPayload["toolNames"]) // Surface the unexpected toolNames payload so discovery regressions are easy to diagnose.
	}
	for _, toolName := range toolNames { // Walk the filtered discovery catalog once so the disabled tool can be rejected directly.
		if toolName == "adbShell" { // Fail when adbShell is still externally advertised even though the operator disabled it.
			t.Fatal("expected adbShell to be absent from discovery when disabled") // Surface the unexpected tool presence so the kill-switch regression is obvious.
		}
	}
	if service.ADBShellToolEnabled() { // Fail when the convenience helper does not reflect the operator-level disable flag.
		t.Fatal("expected adbShell helper to report disabled") // Surface the helper mismatch so handler wiring regressions are easy to diagnose.
	}
}
