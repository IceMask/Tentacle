// security.go builds internal RPC transport credentials and shared-token interceptors for worker and orchestrator gRPC traffic.
package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"mcp_for_appium/internal/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const rpcAuthTokenHeader = "x-mcp-rpc-token" // rpcAuthTokenHeader names the shared metadata key used to authorize internal worker-orchestrator RPC calls.

// NewServerOptions builds the gRPC server options required by the configured internal RPC security mode.
func NewServerOptions(cfg config.RPCSecurityConfig) ([]grpc.ServerOption, error) {
	options := make([]grpc.ServerOption, 0, 2)                     // Allocate the server-option slice once because at most transport credentials and a unary interceptor are appended.
	normalizedMode := strings.ToLower(strings.TrimSpace(cfg.Mode)) // Normalize the internal RPC security mode once so transport wiring stays deterministic and case-insensitive.
	if normalizedMode == "" || normalizedMode == "insecure" {      // Treat empty mode as insecure for backward-compatible startup behavior.
		if interceptor := newServerTokenInterceptor(cfg.AuthToken); interceptor != nil { // Still allow shared-token authorization in insecure mode when the operator configures one explicitly.
			options = append(options, grpc.UnaryInterceptor(interceptor)) // Attach the shared-token unary interceptor so insecure mode can still enforce a shared client token when desired.
		}
		return options, nil // Return the insecure-mode server options because no TLS credentials need to be loaded.
	}

	tlsConfig, err := buildServerTLSConfig(cfg, normalizedMode) // Build the internal RPC server TLS configuration for tls or mtls mode before constructing server credentials.
	if err != nil {                                             // Stop immediately when the configured TLS material cannot be loaded or parsed.
		return nil, err // Preserve the TLS configuration failure for the caller so startup can fail with the precise root cause.
	}
	options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig)))             // Attach the transport credentials so the gRPC server enforces the configured internal TLS mode.
	if interceptor := newServerTokenInterceptor(cfg.AuthToken); interceptor != nil { // Attach the shared-token interceptor whenever an auth token is configured for the internal RPC path.
		options = append(options, grpc.UnaryInterceptor(interceptor)) // Enforce the shared internal RPC auth token after the transport handshake completes successfully.
	}

	return options, nil // Return the fully built gRPC server option slice for the configured transport mode.
}

// NewDialOptions builds the gRPC dial options required by the configured internal RPC security mode.
func NewDialOptions(cfg config.RPCSecurityConfig) ([]grpc.DialOption, error) {
	options := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json"))} // Seed the dial options with the repository's JSON codec selection used by every internal RPC call.
	normalizedMode := strings.ToLower(strings.TrimSpace(cfg.Mode))                             // Normalize the internal RPC security mode once so client transport wiring stays deterministic and case-insensitive.
	if normalizedMode == "" || normalizedMode == "insecure" {                                  // Treat empty mode as insecure for backward-compatible dialing behavior.
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials())) // Attach the insecure transport credentials used by the legacy repository behavior.
	} else {
		tlsConfig, err := buildClientTLSConfig(cfg, normalizedMode) // Build the internal RPC client TLS configuration for tls or mtls mode before constructing dial credentials.
		if err != nil {                                             // Stop immediately when the configured TLS material cannot be loaded or parsed.
			return nil, err // Preserve the TLS configuration failure for the caller so startup can fail with the precise root cause.
		}
		options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))) // Attach the TLS transport credentials so the client verifies the server according to the configured internal mode.
	}
	if interceptor := newClientTokenInterceptor(cfg.AuthToken); interceptor != nil { // Attach the shared-token interceptor whenever an auth token is configured for the internal RPC path.
		options = append(options, grpc.WithUnaryInterceptor(interceptor)) // Inject the shared internal RPC auth token on every unary request automatically.
	}

	return options, nil // Return the fully built gRPC dial option slice for the configured transport mode.
}

// newServerTokenInterceptor builds the unary server interceptor that rejects internal RPC calls missing the configured shared auth token.
func newServerTokenInterceptor(authToken string) grpc.UnaryServerInterceptor {
	normalizedToken := strings.TrimSpace(authToken) // Normalize the configured shared auth token once so empty and whitespace-only values behave consistently.
	if normalizedToken == "" {                      // Skip the interceptor entirely when no shared token has been configured for the internal RPC path.
		return nil // Return nil so callers can omit the unary interceptor option cleanly.
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) { // Build the unary interceptor that validates the shared token before the RPC handler runs.
		metadataValues, ok := metadata.FromIncomingContext(ctx) // Resolve the incoming metadata so the shared internal RPC token header can be validated.
		if !ok {                                                // Reject requests that carry no metadata because they cannot include the required shared token.
			return nil, status.Error(codes.Unauthenticated, "missing rpc auth metadata") // Surface missing metadata as an unauthenticated RPC call.
		}
		tokenValues := metadataValues.Get(rpcAuthTokenHeader)                              // Read the configured internal RPC token header from the incoming metadata.
		if len(tokenValues) == 0 || strings.TrimSpace(tokenValues[0]) != normalizedToken { // Reject missing or mismatched tokens before the RPC handler runs.
			return nil, status.Error(codes.Unauthenticated, "invalid rpc auth token") // Surface token mismatches as unauthenticated RPC calls.
		}

		return handler(ctx, req) // Allow the RPC to proceed once the shared internal auth token has been validated successfully.
	}
}

// newClientTokenInterceptor builds the unary client interceptor that injects the configured shared auth token into every internal RPC call.
func newClientTokenInterceptor(authToken string) grpc.UnaryClientInterceptor {
	normalizedToken := strings.TrimSpace(authToken) // Normalize the configured shared auth token once so empty and whitespace-only values behave consistently.
	if normalizedToken == "" {                      // Skip the interceptor entirely when no shared token has been configured for the internal RPC path.
		return nil // Return nil so callers can omit the unary interceptor option cleanly.
	}

	return func(ctx context.Context, method string, req interface{}, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error { // Build the unary interceptor that injects the shared token before each RPC invoke.
		outgoingContext := metadata.AppendToOutgoingContext(ctx, rpcAuthTokenHeader, normalizedToken) // Attach the shared internal RPC auth token to the outgoing metadata for the current unary call.
		return invoker(outgoingContext, method, req, reply, cc, opts...)                              // Invoke the downstream RPC with the injected shared-token metadata.
	}
}

// buildServerTLSConfig loads the TLS configuration used by the worker and orchestrator gRPC servers for tls or mtls mode.
func buildServerTLSConfig(cfg config.RPCSecurityConfig, mode string) (*tls.Config, error) {
	serverCertificate, err := tls.LoadX509KeyPair(cfg.ServerCertFile, cfg.ServerKeyFile) // Load the configured server certificate pair before constructing the transport credentials.
	if err != nil {                                                                      // Stop immediately when the internal RPC server certificate pair cannot be loaded.
		return nil, fmt.Errorf("failed to load rpc server certificate pair: %w", err) // Preserve the TLS load failure so startup can fail with the precise root cause.
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate}} // Build the shared server TLS config with the repository TLS floor and configured certificate pair.
	if mode == "mtls" {                                                                                        // Extend the transport config only when mutual TLS is enabled explicitly.
		clientCAs, err := loadCertPool(cfg.CAFile) // Load the configured CA bundle so the server can verify connecting client certificates.
		if err != nil {                            // Stop immediately when the CA bundle cannot be loaded or parsed.
			return nil, err // Preserve the CA load failure so startup can fail with the precise root cause.
		}
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert // Require and verify client certificates on every inbound RPC connection in mtls mode.
		tlsConfig.ClientCAs = clientCAs                       // Attach the configured CA bundle so client certificates can be verified against the expected trust roots.
	}

	return tlsConfig, nil // Return the fully built internal RPC server TLS config for credentials.NewTLS.
}

// buildClientTLSConfig loads the TLS configuration used by the worker and orchestrator gRPC clients for tls or mtls mode.
func buildClientTLSConfig(cfg config.RPCSecurityConfig, mode string) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12} // Build the shared client TLS config with the repository TLS floor before any optional CA or client-certificate material is attached.
	if strings.TrimSpace(cfg.CAFile) != "" {               // Load the configured CA bundle only when the operator explicitly supplies one for server verification.
		rootCAs, err := loadCertPool(cfg.CAFile) // Load the CA bundle so the client can verify the internal server certificate chain against the expected trust roots.
		if err != nil {                          // Stop immediately when the CA bundle cannot be loaded or parsed.
			return nil, err // Preserve the CA load failure so startup can fail with the precise root cause.
		}
		tlsConfig.RootCAs = rootCAs // Attach the configured CA bundle so the client verifies the internal server against the expected roots.
	}
	if strings.TrimSpace(cfg.ServerName) != "" { // Override the server name only when the operator configures one explicitly for hostname verification.
		tlsConfig.ServerName = strings.TrimSpace(cfg.ServerName) // Attach the configured server name so TLS verification can succeed against certificates issued for logical names such as localhost.
	}
	if mode == "mtls" { // Extend the client transport config only when mutual TLS is enabled explicitly.
		clientCertificate, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile) // Load the configured client certificate pair before building mTLS dial credentials.
		if err != nil {                                                                      // Stop immediately when the client certificate pair cannot be loaded.
			return nil, fmt.Errorf("failed to load rpc client certificate pair: %w", err) // Preserve the TLS load failure so startup can fail with the precise root cause.
		}
		tlsConfig.Certificates = []tls.Certificate{clientCertificate} // Attach the configured client certificate pair so the server can verify the caller in mtls mode.
	}

	return tlsConfig, nil // Return the fully built internal RPC client TLS config for credentials.NewTLS.
}

// loadCertPool reads one PEM bundle from disk and returns it as an x509 cert pool suitable for TLS verification.
func loadCertPool(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path) // Read the configured PEM bundle from disk before parsing it into a certificate pool.
	if err != nil {                    // Stop immediately when the PEM bundle cannot be read.
		return nil, fmt.Errorf("failed to read certificate authority file %q: %w", path, err) // Preserve the read failure together with the source path for easier diagnosis.
	}
	certPool := x509.NewCertPool()              // Allocate a new empty certificate pool before parsing the PEM bundle.
	if !certPool.AppendCertsFromPEM(pemBytes) { // Reject bundles that contain no parseable PEM certificates because verification would otherwise silently trust nothing.
		return nil, fmt.Errorf("failed to parse certificate authority file %q", path) // Surface the PEM parse failure together with the source path for easier diagnosis.
	}

	return certPool, nil // Return the parsed certificate pool so TLS verification can trust the configured authority bundle.
}
