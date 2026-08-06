package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"mcp_for_appium/internal/errors"

	"gopkg.in/yaml.v3"
)

// Load reads configuration from file and environment variables.
func Load(path string) (*Config, error) {
	cfg := &Config{} // Allocate one zero-value config before tagged defaults and external sources are layered in precedence order.

	// 1. Apply tagged defaults before decoding so explicit false, zero, and empty values in YAML remain authoritative.
	if err := applyDefaults(cfg); err != nil { // Seed omitted fields without overwriting values supplied by later sources.
		return nil, err // Preserve the precise default parsing failure for startup diagnostics.
	}

	// 2. Load from file if exists
	if path != "" {
		f, err := os.Open(path)
		if err == nil {
			defer f.Close()
			decoder := yaml.NewDecoder(f)
			if err := decoder.Decode(cfg); err != nil {
				return nil, errors.Wrap(errors.CodeConfigInvalid, "failed to decode config file", err)
			}
		} else if !os.IsNotExist(err) {
			return nil, errors.Wrap(errors.CodeConfigInvalid, "failed to open config file", err)
		}
	}

	// 3. Apply Environment Overrides
	if err := applyEnvOverrides(cfg); err != nil {
		return nil, err
	}

	// 4. Validate
	normalizeCompatibilityFields(cfg) // Fold legacy auth fields into the v4.4 config shape before validation so existing configs still map onto the new runtime model.
	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// normalizeCompatibilityFields copies legacy config fields into the current v4.4 auth layout when the new fields are still unset.
func normalizeCompatibilityFields(cfg *Config) {
	if cfg == nil { // Skip normalization when the caller somehow passes a nil config because there is nothing to translate safely.
		return // Return immediately because the later validator will report the nil config explicitly.
	}
	if strings.TrimSpace(cfg.Auth.PAT.Mode) == "" { // Fill the PAT mode from legacy fields only when the new config did not already choose a mode explicitly.
		if len(cfg.Auth.PAT.StaticTokens) == 0 && len(cfg.Auth.PATStaticTokens) > 0 { // Reuse legacy static PAT records only when the new PAT static map is still empty.
			cfg.Auth.PAT.StaticTokens = cfg.Auth.PATStaticTokens // Preserve backward compatibility for existing static PAT setups while centralizing runtime reads on the new field.
		}
		if cfg.Auth.EnablePAT && len(cfg.Auth.PAT.StaticTokens) > 0 { // Promote legacy "enabled + static records" into the v4.4 static PAT mode.
			cfg.Auth.PAT.Mode = "static" // Activate the static PAT source so the new runtime wiring can authenticate legacy deployments deterministically.
		} else { // Default every other legacy PAT shape to disabled because old "enable_pat=true" without a source never formed a complete runtime contract.
			cfg.Auth.PAT.Mode = "disabled" // Keep PAT inactive until a real static or postgres source is configured through the new model.
		}
	}
	if strings.TrimSpace(cfg.Auth.OIDC.IssuerURL) == "" && strings.TrimSpace(cfg.Auth.IssuerURL) != "" { // Reuse the legacy issuer field only when the new OIDC issuer field is still empty.
		cfg.Auth.OIDC.IssuerURL = cfg.Auth.IssuerURL // Preserve backward compatibility for legacy issuer-based OIDC configuration while centralizing runtime reads on the new field.
	}
	if strings.TrimSpace(cfg.Auth.OIDC.Audience) == "" && strings.TrimSpace(cfg.Auth.Audience) != "" { // Reuse the legacy audience field only when the new OIDC audience field is still empty.
		cfg.Auth.OIDC.Audience = cfg.Auth.Audience // Preserve backward compatibility for legacy OIDC audience configuration while centralizing runtime reads on the new field.
	}
	if strings.TrimSpace(cfg.Auth.OIDC.JWKSURLOverride) == "" && strings.TrimSpace(cfg.Auth.JWKSURL) != "" { // Reuse the legacy jwks_url field as the override field only when the new override remains unset.
		cfg.Auth.OIDC.JWKSURLOverride = cfg.Auth.JWKSURL // Preserve the legacy raw-JWKS escape hatch while making issuer_url the new primary runtime field.
	}
	if strings.TrimSpace(cfg.Auth.HMAC.KeySource) == "" { // Infer the HMAC key source only when the new config does not set it explicitly.
		if strings.TrimSpace(cfg.Auth.HMAC.StaticSecret) != "" { // Treat an explicit static secret in the new layout as an intentional static key source.
			cfg.Auth.HMAC.KeySource = "static" // Activate the static HMAC source so startup and runtime follow the same source-selection rule.
		} else if strings.TrimSpace(os.Getenv("HMAC_SECRET")) != "" { // Preserve compatibility with the legacy environment-only static HMAC configuration.
			cfg.Auth.HMAC.KeySource = "static" // Promote the legacy process environment secret into the new static HMAC source mode.
		} else { // Fall back to disabled when neither the new config nor the legacy env wiring provides a usable HMAC key source.
			cfg.Auth.HMAC.KeySource = "disabled" // Keep HMAC inactive until a static or postgres source is configured explicitly.
		}
	}
}

// MustLoad loads config and panics on error.
func MustLoad(path string) *Config {
	cfg, err := Load(path)
	if err != nil {
		panic(fmt.Sprintf("failed to load config: %v", err))
	}
	return cfg
}

// applyDefaults executes this operation.
func applyDefaults(v interface{}) error {
	// A simple implementation to set defaults based on `default` tag
	// This is a recursive function using reflection
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	t := val.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := val.Field(i)

		// Recurse for structs
		if value.Kind() == reflect.Struct {
			if err := applyDefaults(value.Addr().Interface()); err != nil {
				return err
			}
			continue
		}

		defaultValue := field.Tag.Get("default")
		if defaultValue == "" {
			continue
		}

		// Only set if currently zero
		if !value.IsZero() {
			continue
		}

		if err := setField(value, defaultValue); err != nil {
			return errors.Wrap(errors.CodeConfigInvalid, fmt.Sprintf("failed to set default for %s", field.Name), err)
		}
	}
	return nil
}

// applyEnvOverrides executes this operation.
func applyEnvOverrides(v interface{}) error {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	t := val.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := val.Field(i)

		// Recurse for structs
		if value.Kind() == reflect.Struct {
			if err := applyEnvOverrides(value.Addr().Interface()); err != nil {
				return err
			}
			continue
		}

		envKey := field.Tag.Get("env")
		if envKey == "" {
			continue
		}

		envVal := os.Getenv(envKey)
		if envVal == "" {
			continue
		}

		if err := setField(value, envVal); err != nil {
			return errors.Wrap(errors.CodeConfigEnv, fmt.Sprintf("failed to set env %s for %s", envKey, field.Name), err)
		}
	}
	return nil
}

// setField executes this operation.
func setField(value reflect.Value, s string) error {
	switch value.Kind() {
	case reflect.String:
		value.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Handle Duration specifically if it's an int64 underlying type but we want to parse "10s"
		if value.Type() == reflect.TypeOf(time.Duration(0)) {
			d, err := time.ParseDuration(s)
			if err != nil {
				return err
			}
			value.SetInt(int64(d))
		} else {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			value.SetInt(i)
		}
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		value.SetBool(b)
	case reflect.Map:
		// Simple map[string]string support: "key=val,key2=val2"
		if value.Type().Key().Kind() == reflect.String && value.Type().Elem().Kind() == reflect.String {
			m := make(map[string]string)
			parts := strings.Split(s, ",")
			for _, part := range parts {
				kv := strings.SplitN(part, "=", 2)
				if len(kv) == 2 {
					m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
				}
			}
			value.Set(reflect.ValueOf(m))
		}
	}
	return nil
}

// validate executes this operation.
func validate(cfg *Config) error {
	mode := strings.ToLower(strings.TrimSpace(cfg.Auth.PAT.Mode)) // Normalize the PAT mode once so validation and startup wiring share the same canonical values.
	switch mode {                                                 // Validate the new PAT source selector before any runtime code attempts to build validators from it.
	case "disabled", "static", "postgres":
		cfg.Auth.PAT.Mode = mode // Persist the canonical lowercase PAT mode so later code does not repeat normalization logic.
	default:
		return errors.New(errors.CodeConfigInvalid, "auth.pat.mode must be one of: disabled, static, postgres") // Surface invalid PAT mode values before runtime authentication wiring begins.
	}
	if cfg.Auth.PAT.Mode == "static" && len(cfg.Auth.PAT.StaticTokens) == 0 { // Reject static PAT mode without any records because the runtime validator would never authenticate successfully.
		return errors.New(errors.CodeConfigMissing, "auth.pat.static_tokens is required when auth.pat.mode=static") // Surface the missing static PAT source before startup continues.
	}

	if strings.TrimSpace(cfg.Auth.OIDC.IssuerURL) == "" && strings.TrimSpace(cfg.Auth.OIDC.Audience) != "" { // Reject partial OIDC config that sets only the audience because issuer validation would be impossible.
		return errors.New(errors.CodeConfigInvalid, "auth.oidc.issuer_url is required when auth.oidc.audience is set") // Surface the missing issuer field before runtime OIDC validator construction.
	}
	if strings.TrimSpace(cfg.Auth.OIDC.IssuerURL) != "" && strings.TrimSpace(cfg.Auth.OIDC.Audience) == "" { // Reject partial OIDC config that sets only the issuer because aud validation would be impossible.
		return errors.New(errors.CodeConfigInvalid, "auth.oidc.audience is required when auth.oidc.issuer_url is set") // Surface the missing audience field before runtime OIDC validator construction.
	}

	hmacKeySource := strings.ToLower(strings.TrimSpace(cfg.Auth.HMAC.KeySource)) // Normalize the HMAC key source once so validation and runtime wiring use the same canonical selector.
	switch hmacKeySource {                                                       // Validate the HMAC source selector before startup wiring attempts to resolve keys from it.
	case "disabled", "static", "postgres":
		cfg.Auth.HMAC.KeySource = hmacKeySource // Persist the canonical lowercase HMAC key-source selector for later startup wiring.
	default:
		return errors.New(errors.CodeConfigInvalid, "auth.hmac.key_source must be one of: disabled, static, postgres") // Surface invalid HMAC source values before runtime authentication wiring begins.
	}
	if cfg.Auth.HMAC.KeySource == "static" && strings.TrimSpace(cfg.Auth.HMAC.StaticSecret) == "" && strings.TrimSpace(os.Getenv("HMAC_SECRET")) == "" { // Reject static HMAC mode without either the new config secret or the legacy env secret.
		return errors.New(errors.CodeConfigMissing, "auth.hmac.static_secret or HMAC_SECRET is required when auth.hmac.key_source=static") // Surface the missing static HMAC secret before startup continues.
	}
	if cfg.Auth.HMAC.KeySource != "disabled" && cfg.Auth.HMAC.Window <= 0 { // Reject non-positive HMAC signature windows because request-signature time validation must advance forward.
		return errors.New(errors.CodeConfigInvalid, "auth.hmac.window must be > 0 when auth.hmac.key_source is enabled") // Surface the invalid HMAC window before runtime request validation begins.
	}
	if cfg.Auth.HMAC.KeySource != "disabled" && cfg.Auth.HMAC.NonceTTL <= 0 { // Reject non-positive nonce TTLs because replay-protection markers must expire after a positive interval.
		return errors.New(errors.CodeConfigInvalid, "auth.hmac.nonce_ttl must be > 0 when auth.hmac.key_source is enabled") // Surface the invalid nonce TTL before runtime request validation begins.
	}

	// 2. Gateway
	if cfg.Gateway.EnableTLS && (cfg.Gateway.TLSCertFile == "" || cfg.Gateway.TLSKeyFile == "") {
		return errors.New(errors.CodeConfigInvalid, "TLS enabled but cert/key missing")
	}
	if cfg.WebSocket.SubscriptionTokenTTL <= 0 { // Reject non-positive WebSocket subscription-token TTL values because issued browser tokens must expire after a positive duration.
		return errors.New(errors.CodeConfigInvalid, "websocket.subscription_token_ttl must be > 0") // Surface the invalid token TTL before HTTP startup wiring begins.
	}
	rpcSecurityMode := strings.ToLower(strings.TrimSpace(cfg.RPC.Security.Mode)) // Normalize the internal RPC security mode once so validation and runtime wiring share the same canonical selector.
	switch rpcSecurityMode {                                                     // Validate the internal RPC security mode before any server or client wiring attempts to interpret it.
	case "", "insecure", "tls", "mtls":
		if rpcSecurityMode == "" { // Fill in the compatibility default when the config omits the internal RPC security mode entirely.
			rpcSecurityMode = "insecure" // Preserve current repository behavior for configs that predate the v4.4 RPC security model.
		}
		cfg.RPC.Security.Mode = rpcSecurityMode // Persist the canonical lowercase RPC security mode so later startup and runtime code do not repeat normalization logic.
	default:
		return errors.New(errors.CodeConfigInvalid, "rpc.security.mode must be one of: insecure, tls, mtls") // Surface unsupported internal RPC security modes before startup continues.
	}

	// 3. Storage
	if cfg.Storage.Postgres.DSN == "" {
		// Maybe optional for some modes? But generally required.
		// return errors.New(errors.CodeConfigMissing, "Postgres DSN missing")
	}

	// 4. Device Farm
	switch strings.ToLower(strings.TrimSpace(cfg.DeviceFarm.Mode)) {
	case "", "disabled", "test_grid", "remote_access", "run_api":
		// valid
	default:
		return errors.New(errors.CodeConfigInvalid, "devicefarm.mode must be one of: disabled, remote_access, test_grid, run_api")
	}

	return nil
}
