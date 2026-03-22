// loader_test.go verifies config defaults, compatibility normalization, and environment overrides.
package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestNormalizeCompatibilityFieldsPromotesLegacyAuthFields verifies that legacy auth fields still populate the v4.4 config layout.
func TestNormalizeCompatibilityFieldsPromotesLegacyAuthFields(t *testing.T) {
	cfg := &Config{ // Build one config that uses only legacy auth fields so the compatibility normalizer can populate the new layout.
		Auth: AuthConfig{
			EnablePAT:       true,
			PATStaticTokens: map[string]string{"token-id": "hash|tenant-1|active"},
			IssuerURL:       "https://issuer.example",
			Audience:        "mobile-worker",
			JWKSURL:         "https://issuer.example/jwks",
		},
	}

	normalizeCompatibilityFields(cfg) // Apply the production compatibility normalizer so the new auth layout is populated from legacy fields.

	if cfg.Auth.PAT.Mode != "static" { // Fail the test when the legacy PAT toggle and static token map do not promote into static PAT mode.
		t.Fatalf("expected PAT mode static, got %q", cfg.Auth.PAT.Mode) // Surface the unexpected PAT mode so compatibility regressions are obvious.
	}
	if cfg.Auth.OIDC.IssuerURL != "https://issuer.example" || cfg.Auth.OIDC.Audience != "mobile-worker" { // Fail the test when legacy OIDC fields do not populate the new layout.
		t.Fatalf("expected OIDC issuer/audience to be normalized, got %#v", cfg.Auth.OIDC) // Surface the unexpected OIDC config so compatibility regressions are obvious.
	}
	if cfg.Auth.OIDC.JWKSURLOverride != "https://issuer.example/jwks" { // Fail the test when the legacy JWKS field does not populate the new override field.
		t.Fatalf("expected JWKS override to be normalized, got %q", cfg.Auth.OIDC.JWKSURLOverride) // Surface the unexpected JWKS override so compatibility regressions are obvious.
	}
}

// TestLoadAppliesDefaultsAndEnvOverrides verifies that the loader applies tagged defaults and environment overrides on top of file-backed config.
func TestLoadAppliesDefaultsAndEnvOverrides(t *testing.T) {
	tempDir := t.TempDir()                                                                                                                                   // Allocate one isolated temporary directory so the loader can read a synthetic YAML config safely.
	configPath := filepath.Join(tempDir, "config.yaml")                                                                                                      // Build one synthetic config path inside the isolated temporary directory.
	configYAML := "gateway:\n  port: 18080\nstorage:\n  postgres:\n    dsn: postgres://postgres:postgres@127.0.0.1:5432/mcp_mobile_worker?sslmode=disable\n" // Define the minimal valid YAML payload required for the loader to pass validation.
	if err := os.WriteFile(configPath, []byte(configYAML), 0o644); err != nil {                                                                              // Write the synthetic config file that the production loader will read.
		t.Fatalf("failed to write config file: %v", err) // Surface the setup failure because the loader cannot be exercised without a real config file.
	}
	t.Setenv("WORKER__HEARTBEAT_INTERVAL", "15s") // Override one duration field through the environment so the production env-override path is exercised directly.

	cfg, err := Load(configPath) // Load the synthetic config file through the production loader so defaults, normalization, and env overrides all run together.
	if err != nil {              // Fail the test when the production loader rejects the valid synthetic config unexpectedly.
		t.Fatalf("expected config load to succeed, got error: %v", err) // Surface the unexpected load failure so configuration-regression diagnosis is straightforward.
	}
	if cfg.Gateway.Port != 18080 { // Fail the test when the loader does not preserve explicit file-backed values.
		t.Fatalf("expected gateway port 18080, got %d", cfg.Gateway.Port) // Surface the unexpected port so file-loading regressions are obvious.
	}
	if cfg.Worker.HeartbeatInterval != 15*time.Second { // Fail the test when the environment override does not replace the default duration.
		t.Fatalf("expected heartbeat interval 15s, got %s", cfg.Worker.HeartbeatInterval) // Surface the unexpected duration so env-override regressions are obvious.
	}
	if cfg.WebSocket.SubscriptionTokenTTL != 60*time.Second { // Fail the test when the tagged default does not populate the websocket token TTL.
		t.Fatalf("expected websocket token TTL 60s, got %s", cfg.WebSocket.SubscriptionTokenTTL) // Surface the unexpected duration so defaulting regressions are obvious.
	}
}
