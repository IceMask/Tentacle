// preflight.go provides startup-time configuration validation and dependency checks for the gateway, orchestrator, and worker binaries.
package startup

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
	storages3 "mcp_for_appium/internal/storage/s3"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ValidateGatewayStartup checks the effective gateway configuration before any listeners or long-running goroutines are started.
func ValidateGatewayStartup(cfg *config.Config, stdioMode bool) error {
	if cfg == nil { // Reject a missing root config because none of the downstream startup checks can run safely without it.
		return errors.New(errors.CodeConfigInvalid, "gateway config is nil") // Return a stable config error instead of letting later code panic on a nil pointer.
	}
	if strings.ToLower(strings.TrimSpace(cfg.Orchestrator.ExecutionMode)) != "monolith" { // Gate the gateway binary to the only execution mode that is actually wired today.
		return errors.New(errors.CodeConfigConflict, "gateway startup requires orchestrator.execution_mode=monolith") // Surface the unsupported execution mode before any network listeners start.
	}
	if err := validateSharedStorageConfig(cfg); err != nil { // Validate the shared storage fields used by the embedded orchestrator before dependency clients are initialized.
		return err // Preserve the first precise validation failure so operators can fix the correct config field.
	}
	if err := validateWorkerAppiumConfig(cfg.Worker.AppiumURL); err != nil { // Validate the embedded Appium base URL because monolith gateway execution relies on it for session creation.
		return err // Stop immediately when the embedded Appium endpoint is malformed.
	}
	if err := validateGatewaySecurityConfig(cfg); err != nil { // Validate auth-related config and WebSocket origin policy before the HTTP server wiring starts.
		return err // Preserve the specific security configuration failure for the caller.
	}
	if err := validateDeviceFarmConfig(cfg); err != nil { // Validate optional Device Farm settings so later tool handlers do not fail on obvious missing fields.
		return err // Preserve the specific Device Farm validation error for the caller.
	}
	if stdioMode { // Skip HTTP-only checks when the process is running as a stdio MCP server.
		return nil // Return success because port, shutdown timeout, and TLS are irrelevant in stdio mode.
	}
	if err := validatePort(cfg.Gateway.Port, "gateway.port"); err != nil { // Validate the HTTP listener port before net.Listen is attempted.
		return err // Surface the invalid port with a config error instead of a later listen failure.
	}
	if err := validatePositiveDuration(cfg.Gateway.ShutdownTimeout, "gateway.shutdown_timeout"); err != nil { // Validate graceful-shutdown timing before the HTTP server is created.
		return err // Preserve the duration validation failure for the caller.
	}
	if err := validateTLSFiles(cfg.Gateway.EnableTLS, cfg.Gateway.TLSCertFile, cfg.Gateway.TLSKeyFile); err != nil { // Validate TLS files only when TLS is enabled for HTTP mode.
		return err // Stop immediately when the TLS configuration is incomplete or unreadable.
	}

	return nil // Return success once every gateway startup prerequisite has been validated.
}

// ValidateOrchestratorStartup checks the standalone orchestrator configuration before storage clients, registries, or gRPC listeners are started.
func ValidateOrchestratorStartup(cfg *config.Config) error {
	if cfg == nil { // Reject a missing root config because later startup logic dereferences multiple nested config sections.
		return errors.New(errors.CodeConfigInvalid, "orchestrator config is nil") // Return a stable config error instead of allowing a nil-pointer panic later.
	}
	if strings.ToLower(strings.TrimSpace(cfg.Orchestrator.ExecutionMode)) != "distributed" { // Enforce the only execution mode the standalone orchestrator binary is intended to run with.
		return errors.New(errors.CodeConfigConflict, "standalone orchestrator startup requires orchestrator.execution_mode=distributed") // Surface the incompatible execution mode before any background loops start.
	}
	if err := validatePort(cfg.Orchestrator.GRPCPort, "orchestrator.grpc_port"); err != nil { // Validate the orchestrator gRPC listener port before net.Listen is attempted.
		return err // Preserve the port validation failure for the caller.
	}
	if err := validatePositiveDuration(cfg.Orchestrator.PlanTimeout, "orchestrator.plan_timeout"); err != nil { // Validate the plan timeout so execution contexts are always bounded.
		return err // Preserve the duration validation failure for the caller.
	}
	if err := validatePositiveDuration(cfg.Orchestrator.StepTimeout, "orchestrator.step_timeout"); err != nil { // Validate the per-step timeout so worker execution contexts are bounded too.
		return err // Preserve the duration validation failure for the caller.
	}
	if err := validatePositiveDuration(cfg.Orchestrator.AutoWaitMax, "orchestrator.auto_wait_max"); err != nil { // Validate the auto-wait ceiling used by executor logic.
		return err // Preserve the duration validation failure for the caller.
	}
	if err := validatePositiveDuration(cfg.Orchestrator.SnapshotTTL, "orchestrator.snapshot_ttl"); err != nil { // Validate the snapshot cache TTL before cache state is used.
		return err // Preserve the duration validation failure for the caller.
	}
	if err := validateSharedStorageConfig(cfg); err != nil { // Validate the shared storage config used by the orchestrator binary before clients are initialized.
		return err // Preserve the first precise storage validation error.
	}
	if err := validateRPCSecurityConfig(cfg.RPC.Security); err != nil { // Validate internal RPC security before the orchestrator starts its worker-facing gRPC listener.
		return err // Preserve the RPC security validation failure for the caller.
	}
	if err := validateDeviceFarmConfig(cfg); err != nil { // Validate optional Device Farm configuration before the service decides whether to initialize that client.
		return err // Preserve the specific Device Farm validation error for the caller.
	}

	return nil // Return success once every standalone orchestrator startup prerequisite has been validated.
}

// ValidateWorkerStartup checks the worker configuration before the gRPC server starts or registration begins.
func ValidateWorkerStartup(cfg *config.Config) error {
	if cfg == nil { // Reject a missing root config because worker startup dereferences nested sections immediately.
		return errors.New(errors.CodeConfigInvalid, "worker config is nil") // Return a stable config error instead of allowing a nil-pointer panic later.
	}
	if err := validatePort(cfg.Worker.GRPCPort, "worker.grpc_port"); err != nil { // Validate the worker gRPC listener port before net.Listen is attempted.
		return err // Preserve the port validation failure for the caller.
	}
	if cfg.Worker.Concurrency < 0 { // Reject negative concurrency because only zero and positive values have meaning in the worker binary.
		return errors.New(errors.CodeConfigInvalid, "worker.concurrency must be >= 0") // Surface the unsupported negative concurrency value clearly.
	}
	if err := validatePositiveDuration(cfg.Worker.HeartbeatInterval, "worker.heartbeat_interval"); err != nil { // Validate the heartbeat cadence before the ticker is created.
		return err // Preserve the duration validation failure for the caller.
	}
	if err := validateWorkerAppiumConfig(cfg.Worker.AppiumURL); err != nil { // Validate the Appium base URL before the worker attempts any device automation.
		return err // Preserve the URL validation failure for the caller.
	}
	if err := validateHostPort(cfg.Worker.OrchestratorAddr, "worker.orchestrator_addr"); err != nil { // Validate the orchestrator target address before gRPC registration is attempted.
		return err // Preserve the host:port validation failure for the caller.
	}
	if strings.TrimSpace(cfg.Worker.AdvertiseAddr) != "" { // Validate the optional worker advertise address only when the operator overrides the default hostname-derived value.
		if err := validateHostPort(cfg.Worker.AdvertiseAddr, "worker.advertise_addr"); err != nil { // Reuse the shared host:port validator so distributed worker registration uses one routable address shape.
			return err // Preserve the advertise-address validation failure for the caller.
		}
	}
	if err := validateRPCSecurityConfig(cfg.RPC.Security); err != nil { // Validate internal RPC security before the worker starts its gRPC listener and dials the orchestrator.
		return err // Preserve the RPC security validation failure for the caller.
	}

	return nil // Return success once every worker startup prerequisite has been validated.
}

// CheckAppiumReachability verifies that the configured Appium base URL responds successfully on its status endpoint.
func CheckAppiumReachability(ctx context.Context, rawURL string) error {
	parsedURL, err := parseHTTPURL(rawURL, "worker.appium_url") // Reuse the same URL parser so the reachability check always starts from a valid HTTP base URL.
	if err != nil {                                             // Stop immediately when the Appium URL is malformed.
		return err // Stop immediately when the Appium URL is malformed.
	}
	statusURL := strings.TrimRight(parsedURL.String(), "/") + "/status"             // Build the canonical Appium status URL while preserving an optional base path such as /wd/hub.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil) // Bind the HTTP request to the caller's timeout so startup never hangs indefinitely on Appium.
	if err != nil {                                                                 // Stop when the status request cannot be constructed from the validated URL.
		return errors.Wrap(errors.CodeConfigInvalid, "failed to build appium status request", err) // Surface request-construction failures as config problems because the base URL is the root cause.
	}
	response, err := http.DefaultClient.Do(request) // Execute the lightweight status probe against Appium before any long-lived server starts.
	if err != nil {                                 // Stop when the Appium status endpoint cannot be reached or negotiated successfully.
		return errors.Wrap(errors.CodeHealthDown, "failed to reach appium status endpoint", err) // Surface dependency reachability issues before runtime traffic is accepted.
	}
	defer response.Body.Close()                                                              // Ensure the status response body is always closed before startup continues or aborts.
	_, _ = io.Copy(io.Discard, response.Body)                                                // Drain the body so the shared HTTP transport can reuse the underlying connection cleanly.
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusBadRequest { // Treat any non-2xx/3xx response as an unhealthy Appium dependency.
		return errors.New(errors.CodeHealthDown, fmt.Sprintf("appium status endpoint returned HTTP %d", response.StatusCode)) // Surface the exact failing status code for quicker diagnosis.
	}

	return nil // Return success once Appium has responded successfully to the startup probe.
}

// CheckS3BucketAccess verifies that the configured artifact bucket exists and is reachable before the process starts serving requests.
func CheckS3BucketAccess(ctx context.Context, client *storages3.Client) error {
	if client == nil { // Reject a nil client because no bucket probe can be performed without a constructed S3 wrapper.
		return errors.New(errors.CodeConfigInvalid, "s3 client is nil") // Surface the missing client as a config/startup wiring failure.
	}
	if err := client.ValidateBucket(ctx); err != nil { // Reuse the S3 wrapper's bucket validation so the startup check matches runtime storage behavior.
		return err // Preserve the underlying bucket validation failure for the caller.
	}

	return nil // Return success once the artifact bucket has been verified successfully.
}

// validateSharedStorageConfig checks the storage settings that are shared by gateway and standalone orchestrator startup.
func validateSharedStorageConfig(cfg *config.Config) error {
	if strings.TrimSpace(cfg.Storage.Postgres.DSN) == "" { // Reject an empty Postgres DSN because DAO construction depends on it immediately.
		return errors.New(errors.CodeConfigMissing, "storage.postgres.dsn is required") // Surface the missing DSN explicitly instead of failing later during DAO construction.
	}
	if _, err := pgxpool.ParseConfig(cfg.Storage.Postgres.DSN); err != nil { // Parse the DSN without connecting so malformed DSNs are caught before network setup.
		return errors.Wrap(errors.CodeConfigInvalid, "storage.postgres.dsn is invalid", err) // Surface DSN syntax problems as startup config errors.
	}
	if cfg.Storage.Postgres.MaxOpenConns <= 0 { // Reject non-positive pool sizes because pgx expects a usable upper bound.
		return errors.New(errors.CodeConfigInvalid, "storage.postgres.max_open_conns must be > 0") // Surface the invalid pool size before DAO construction.
	}
	if cfg.Storage.Postgres.MaxIdleConns <= 0 { // Reject non-positive idle pool sizes because the config should reserve at least one reusable connection.
		return errors.New(errors.CodeConfigInvalid, "storage.postgres.max_idle_conns must be > 0") // Surface the invalid idle-pool size before DAO construction.
	}
	if err := validatePositiveDuration(cfg.Storage.Postgres.ConnMaxLifetime, "storage.postgres.conn_max_lifetime"); err != nil { // Validate the connection lifetime before pgx pool configuration is created.
		return err // Preserve the duration validation failure for the caller.
	}
	if err := validateHostPort(cfg.Storage.Redis.Addr, "storage.redis.addr"); err != nil { // Validate the Redis address before the cache client attempts to dial it.
		return err // Preserve the host:port validation failure for the caller.
	}
	if strings.TrimSpace(cfg.Storage.S3.Region) == "" { // Reject an empty S3 region because the AWS SDK configuration requires a concrete region.
		return errors.New(errors.CodeConfigMissing, "storage.s3.region is required") // Surface the missing region explicitly before the AWS SDK is initialized.
	}
	if strings.TrimSpace(cfg.Storage.S3.Bucket) == "" { // Reject an empty artifact bucket because screenshot and artifact writes depend on it later.
		return errors.New(errors.CodeConfigMissing, "storage.s3.bucket is required") // Surface the missing bucket explicitly instead of letting runtime artifact writes fail.
	}
	if strings.TrimSpace(cfg.Storage.S3.Endpoint) != "" { // Validate the optional custom S3 endpoint only when the user configured one.
		if _, err := parseHTTPURL(cfg.Storage.S3.Endpoint, "storage.s3.endpoint"); err != nil { // Reuse the common HTTP URL validator for the custom endpoint.
			return err // Preserve the endpoint validation failure for the caller.
		}
	}

	return nil // Return success once every shared storage field has passed startup validation.
}

// validateWorkerAppiumConfig checks that the configured Appium URL is a valid HTTP or HTTPS base URL.
func validateWorkerAppiumConfig(rawURL string) error {
	if _, err := parseHTTPURL(rawURL, "worker.appium_url"); err != nil { // Reuse the common HTTP URL validator so Appium URL rules stay centralized.
		return err // Preserve the HTTP URL validation failure for the caller.
	}

	return nil // Return success once the Appium base URL has been validated.
}

// validateDeviceFarmConfig checks optional Device Farm settings for the currently selected mode.
func validateDeviceFarmConfig(cfg *config.Config) error {
	mode := strings.ToLower(strings.TrimSpace(cfg.DeviceFarm.Mode)) // Normalize the Device Farm mode once so all later comparisons are case-insensitive.
	switch mode {                                                   // Branch on the configured Device Farm mode so each mode can enforce only the fields it actually needs.
	case "", "disabled": // Treat empty and explicit disabled as equivalent no-op modes.
		return nil // Return success because disabled mode does not require extra startup fields.
	case "run_api": // Validate the fields needed by the currently implemented Device Farm run_api flow.
		if strings.TrimSpace(cfg.AWS.Region) == "" { // Reject an empty AWS region because the SDK client cannot target Device Farm without it.
			return errors.New(errors.CodeConfigMissing, "aws.region is required when devicefarm.mode=run_api") // Surface the missing region before the Device Farm client is initialized.
		}
		if strings.TrimSpace(cfg.DeviceFarm.ProjectARN) == "" { // Reject a missing default project ARN because run_api helper paths fall back to this config.
			return errors.New(errors.CodeConfigMissing, "devicefarm.project_arn is required when devicefarm.mode=run_api") // Surface the missing project ARN before the service accepts Device Farm tool calls.
		}
	case "test_grid": // Validate the fields expected for test_grid mode even though the runtime integration is still limited.
		if strings.TrimSpace(cfg.AWS.Region) == "" { // Reject an empty AWS region because the SDK client would still need it for test grid URL generation.
			return errors.New(errors.CodeConfigMissing, "aws.region is required when devicefarm.mode=test_grid") // Surface the missing region before startup continues.
		}
		if strings.TrimSpace(cfg.DeviceFarm.TestGridProjectARN) == "" && strings.TrimSpace(cfg.DeviceFarm.ProjectARN) == "" { // Accept either the dedicated test-grid ARN or the generic project ARN as the source project.
			return errors.New(errors.CodeConfigMissing, "devicefarm.test_grid_project_arn or devicefarm.project_arn is required when devicefarm.mode=test_grid") // Surface the missing project identifier before startup continues.
		}
		if cfg.DeviceFarm.URLExpiresSeconds <= 0 { // Reject non-positive URL expiry because generated test-grid URLs must have a usable lifetime.
			return errors.New(errors.CodeConfigInvalid, "devicefarm.url_expires_seconds must be > 0 when devicefarm.mode=test_grid") // Surface the invalid URL expiry before runtime URL generation is attempted.
		}
	default: // Defend against future callers that bypass config.Load validation and still reach startup preflight.
		return errors.New(errors.CodeConfigInvalid, "devicefarm.mode must be one of: disabled, test_grid, run_api") // Return a stable config error for unsupported Device Farm modes.
	}

	return nil // Return success once the selected Device Farm mode has all required startup fields.
}

// validateTLSFiles checks that the configured certificate and key paths exist and can be loaded as one key pair when TLS is enabled.
func validateTLSFiles(enabled bool, certFile string, keyFile string) error {
	if !enabled { // Skip TLS file validation completely when HTTP TLS is disabled.
		return nil // Return success because no cert or key files are needed in plaintext HTTP mode.
	}
	if strings.TrimSpace(certFile) == "" || strings.TrimSpace(keyFile) == "" { // Reject incomplete TLS config before any listener starts.
		return errors.New(errors.CodeConfigMissing, "gateway TLS requires both tls_cert_file and tls_key_file") // Surface the missing TLS path fields explicitly.
	}
	if _, err := os.Stat(certFile); err != nil { // Check that the certificate file exists and is readable before the server attempts to start.
		return errors.Wrap(errors.CodeConfigInvalid, "gateway tls_cert_file is not readable", err) // Surface missing or unreadable certificate files as config errors.
	}
	if _, err := os.Stat(keyFile); err != nil { // Check that the private-key file exists and is readable before the server attempts to start.
		return errors.Wrap(errors.CodeConfigInvalid, "gateway tls_key_file is not readable", err) // Surface missing or unreadable key files as config errors.
	}
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil { // Parse the key pair up front so invalid PEM contents fail before the server starts.
		return errors.Wrap(errors.CodeTLSInvalid, "failed to load gateway TLS key pair", err) // Surface invalid TLS material as a dedicated TLS startup error.
	}

	return nil // Return success once the configured TLS key pair has been verified successfully.
}

// validateRPCSecurityConfig checks the shared internal RPC security mode and any TLS or token material required by that mode.
func validateRPCSecurityConfig(cfg config.RPCSecurityConfig) error {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode)) // Normalize the internal RPC security mode once so validation behavior stays case-insensitive and deterministic.
	switch mode {                                        // Validate the shared internal RPC transport mode before any worker or orchestrator server starts.
	case "", "insecure": // Accept the empty compatibility default and the explicit insecure mode without requiring TLS material.
		return nil // Return success because insecure mode does not require TLS files or a shared RPC auth token.
	case "tls": // Validate the first-stage secure transport mode that requires server TLS plus a shared client token.
		if err := validateTLSFiles(true, cfg.ServerCertFile, cfg.ServerKeyFile); err != nil { // Reuse the existing TLS key-pair validation helper for the internal RPC server certificate.
			return errors.Wrap(errors.CodeConfigInvalid, "rpc.security server TLS config is invalid", err) // Surface internal RPC server TLS validation failures with RPC-specific context.
		}
		if strings.TrimSpace(cfg.AuthToken) == "" { // Reject missing shared RPC auth tokens because the first-stage secure transport model requires client-token authorization too.
			return errors.New(errors.CodeConfigMissing, "rpc.security.auth_token is required when rpc.security.mode=tls") // Surface the missing shared RPC auth token before startup continues.
		}
		if strings.TrimSpace(cfg.CAFile) != "" { // Validate the optional CA bundle only when the operator configures one explicitly.
			if _, err := os.Stat(cfg.CAFile); err != nil { // Check that the CA bundle exists and is readable before any TLS client or mTLS server tries to load it.
				return errors.Wrap(errors.CodeConfigInvalid, "rpc.security.ca_file is not readable", err) // Surface unreadable CA bundles as config errors before startup continues.
			}
		}
		return nil // Return success once the first-stage secure internal RPC configuration has passed validation.
	case "mtls": // Validate the stronger mutual-TLS transport mode that requires server certs, client certs, a CA bundle, and a shared RPC auth token.
		if err := validateTLSFiles(true, cfg.ServerCertFile, cfg.ServerKeyFile); err != nil { // Reuse the existing TLS key-pair validation helper for the internal RPC server certificate.
			return errors.Wrap(errors.CodeConfigInvalid, "rpc.security server TLS config is invalid", err) // Surface internal RPC server TLS validation failures with RPC-specific context.
		}
		if strings.TrimSpace(cfg.ClientCertFile) == "" || strings.TrimSpace(cfg.ClientKeyFile) == "" { // Reject missing client certificate material because mutual TLS requires client-side key pairs too.
			return errors.New(errors.CodeConfigMissing, "rpc.security.client_cert_file and rpc.security.client_key_file are required when rpc.security.mode=mtls") // Surface the missing client mTLS material before startup continues.
		}
		if _, err := os.Stat(cfg.ClientCertFile); err != nil { // Check that the client certificate file exists and is readable before any worker or orchestrator dials with mTLS.
			return errors.Wrap(errors.CodeConfigInvalid, "rpc.security.client_cert_file is not readable", err) // Surface unreadable client certificates as config errors before startup continues.
		}
		if _, err := os.Stat(cfg.ClientKeyFile); err != nil { // Check that the client private-key file exists and is readable before any worker or orchestrator dials with mTLS.
			return errors.Wrap(errors.CodeConfigInvalid, "rpc.security.client_key_file is not readable", err) // Surface unreadable client keys as config errors before startup continues.
		}
		if strings.TrimSpace(cfg.CAFile) == "" { // Reject missing CA bundles because mutual TLS needs one shared trust anchor to validate peers.
			return errors.New(errors.CodeConfigMissing, "rpc.security.ca_file is required when rpc.security.mode=mtls") // Surface the missing CA bundle before startup continues.
		}
		if _, err := os.Stat(cfg.CAFile); err != nil { // Check that the CA bundle exists and is readable before any peer-verification occurs.
			return errors.Wrap(errors.CodeConfigInvalid, "rpc.security.ca_file is not readable", err) // Surface unreadable CA bundles as config errors before startup continues.
		}
		if strings.TrimSpace(cfg.AuthToken) == "" { // Keep the shared RPC auth token requirement even in mTLS mode so first-stage authorization semantics remain intact during the migration path.
			return errors.New(errors.CodeConfigMissing, "rpc.security.auth_token is required when rpc.security.mode=mtls") // Surface the missing shared RPC auth token before startup continues.
		}
		return nil // Return success once the mutual-TLS internal RPC configuration has passed validation.
	default:
		return errors.New(errors.CodeConfigInvalid, fmt.Sprintf("rpc.security.mode %q is not supported", cfg.Mode)) // Surface unsupported modes defensively even though config.Load already normalizes the accepted set.
	}
}

// validatePort checks that a TCP or UDP port number is within the valid user-space range.
func validatePort(port int, fieldName string) error {
	if port <= 0 || port > 65535 { // Reject ports outside the valid TCP/UDP port range before any listener attempts to bind them.
		return errors.New(errors.CodeConfigInvalid, fmt.Sprintf("%s must be between 1 and 65535", fieldName)) // Surface the invalid port together with its config field name.
	}

	return nil // Return success once the port value is inside the valid range.
}

// validatePositiveDuration checks that a duration-based config field is strictly positive.
func validatePositiveDuration(value interface{}, fieldName string) error {
	duration, ok := value.(interface{ String() string }) // Use the duration stringer contract so callers can pass time.Duration values directly without another helper type.
	if !ok {                                             // Defend against accidental misuse of the helper with non-duration values.
		return errors.New(errors.CodeConfigInvalid, fmt.Sprintf("%s is not a duration value", fieldName)) // Surface helper misuse as a config error because the caller passed an unsupported field type.
	}
	if duration.String() == "0s" || strings.HasPrefix(duration.String(), "-") { // Reject zero or negative durations because startup timeouts and intervals must advance forward.
		return errors.New(errors.CodeConfigInvalid, fmt.Sprintf("%s must be > 0", fieldName)) // Surface the invalid duration together with its config field name.
	}

	return nil // Return success once the duration is positive.
}

// validateHostPort checks that a client-style address contains both a host and port in a form net.SplitHostPort accepts.
func validateHostPort(address string, fieldName string) error {
	trimmedAddress := strings.TrimSpace(address) // Normalize surrounding whitespace before parsing the client target address.
	if trimmedAddress == "" {                    // Reject empty addresses before host:port parsing is attempted.
		return errors.New(errors.CodeConfigMissing, fmt.Sprintf("%s is required", fieldName)) // Surface the missing address field explicitly.
	}
	host, port, err := net.SplitHostPort(trimmedAddress) // Parse the client address using the standard host:port rules.
	if err != nil {                                      // Reject malformed host:port addresses before any dial attempt occurs.
		return errors.Wrap(errors.CodeConfigInvalid, fmt.Sprintf("%s must be a valid host:port", fieldName), err) // Surface the parse failure together with the config field name.
	}
	if strings.TrimSpace(host) == "" { // Reject empty hosts because client targets must identify a concrete remote host or IP address.
		return errors.New(errors.CodeConfigInvalid, fmt.Sprintf("%s must include a host", fieldName)) // Surface the missing host component explicitly.
	}
	if strings.TrimSpace(port) == "" { // Reject empty ports because client targets must identify a concrete remote service port.
		return errors.New(errors.CodeConfigInvalid, fmt.Sprintf("%s must include a port", fieldName)) // Surface the missing port component explicitly.
	}

	return nil // Return success once the address has a valid host and port.
}

// parseHTTPURL validates that a string is a usable HTTP or HTTPS URL and returns the parsed form.
func parseHTTPURL(rawURL string, fieldName string) (*url.URL, error) {
	trimmedURL := strings.TrimSpace(rawURL) // Normalize surrounding whitespace before URL parsing.
	if trimmedURL == "" {                   // Reject empty URLs before parse and dependency checks are attempted.
		return nil, errors.New(errors.CodeConfigMissing, fmt.Sprintf("%s is required", fieldName)) // Surface the missing URL field explicitly.
	}
	parsedURL, err := url.Parse(trimmedURL) // Parse the URL using the standard library so invalid syntax is rejected early.
	if err != nil {                         // Stop immediately when the supplied URL is malformed.
		return nil, errors.Wrap(errors.CodeConfigInvalid, fmt.Sprintf("%s must be a valid URL", fieldName), err) // Surface the parse failure together with the config field name.
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" { // Reject non-HTTP schemes because all current dependency endpoints use HTTP(S).
		return nil, errors.New(errors.CodeConfigInvalid, fmt.Sprintf("%s must use http or https", fieldName)) // Surface the unsupported URL scheme explicitly.
	}
	if strings.TrimSpace(parsedURL.Host) == "" { // Reject URLs without a host because they cannot be used for dependency probes or clients.
		return nil, errors.New(errors.CodeConfigInvalid, fmt.Sprintf("%s must include a host", fieldName)) // Surface the missing host component explicitly.
	}

	return parsedURL, nil // Return the parsed URL so callers can build dependency-specific request paths safely.
}
