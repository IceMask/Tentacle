// security_test.go verifies shared-token and TLS security behavior for internal worker-orchestrator RPC connections.
package rpc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mcp_for_appium/internal/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// successWorkerService returns successful responses immediately so transport and auth behavior can be asserted without execution noise.
type successWorkerService struct{}

// ExecutePlan returns an immediate successful response so the caller can focus on transport and auth verification.
func (successWorkerService) ExecutePlan(ctx context.Context, req *ExecutePlanRequest) (*ExecutePlanResponse, error) {
	return &ExecutePlanResponse{Status: "accepted"}, nil // Return one accepted response so the test can assert whether the request reached the server at all.
}

// CancelPlan returns an immediate successful response so the caller can focus on transport and auth verification.
func (successWorkerService) CancelPlan(ctx context.Context, req *CancelPlanRequest) (*CancelPlanResponse, error) {
	return &CancelPlanResponse{Status: "cancelled"}, nil // Return one cancelled response so the test can assert whether the request reached the server at all.
}

// TestWorkerClientRejectsMissingSharedRPCToken verifies that a server configured with a shared RPC token rejects callers that do not send that token.
func TestWorkerClientRejectsMissingSharedRPCToken(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind one IPv4 loopback listener explicitly so the test server works in restricted environments too.
	if err != nil {                                    // Skip the test when the environment cannot provide a loopback listener for the internal RPC server.
		t.Skipf("skipping rpc shared-token test because listen failed: %v", err) // Surface the environment limitation without flagging the production transport path as broken.
	}
	serverOptions, err := NewServerOptions(config.RPCSecurityConfig{Mode: "insecure", AuthToken: "rpc-shared-token"}) // Build the production server options for insecure transport plus shared-token authorization.
	if err != nil {                                                                                                   // Stop immediately when the server options cannot be constructed from the supplied test config.
		t.Fatalf("failed to build rpc server options: %v", err) // Surface the option-construction failure because no token behavior can be asserted without a server.
	}
	server := grpc.NewServer(serverOptions...)                  // Construct one production gRPC server over the shared-token interceptor returned by the helper under test.
	RegisterWorkerServiceServer(server, successWorkerService{}) // Register the simple success worker service so ExecutePlan reaches a concrete handler after auth passes.
	go func() {
		_ = server.Serve(listener) // Serve requests in the background so the client under test can connect to the loopback endpoint.
	}()
	defer func() {
		server.Stop()        // Stop the gRPC server immediately once the shared-token assertions have completed.
		_ = listener.Close() // Close the loopback listener explicitly so the TCP port is released right away.
	}()

	client, err := NewWorkerClient(listener.Addr().String()) // Build the production worker client without a shared token so the request should be rejected by the server interceptor.
	if err != nil {                                          // Stop immediately when the client wrapper cannot dial the loopback test server.
		t.Fatalf("failed to create worker client without token: %v", err) // Surface the client-construction failure because no auth behavior can be asserted without it.
	}
	defer client.Close() // Ensure the client connection is closed after the assertion completes.

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)                                                 // Bound the request so auth failures return promptly even if the transport misbehaves.
	defer cancel()                                                                                                        // Release the timeout resources after the RPC completes.
	_, err = client.ExecutePlan(ctx, &ExecutePlanRequest{TraceID: "trace-1", SessionID: "session-1", Plan: []byte(`[]`)}) // Invoke the production worker client without the required shared token so the server interceptor should reject the call.
	if status.Code(err) != codes.Unauthenticated {                                                                        // Fail the test when the server does not classify the missing shared token as an unauthenticated RPC.
		t.Fatalf("expected unauthenticated RPC for missing shared token, got %v", err) // Surface the unexpected status code together with the returned error.
	}
}

// TestWorkerClientTLSHandshakeAndToken verifies that TLS transport plus the shared RPC token work together on the internal worker RPC path.
func TestWorkerClientTLSHandshakeAndToken(t *testing.T) {
	certificateAuthorityPath, serverCertPath, serverKeyPath := writeTestRPCServerCertificateBundle(t) // Generate one localhost certificate bundle so the test can exercise real TLS transport.
	listener, err := net.Listen("tcp4", "127.0.0.1:0")                                                // Bind one IPv4 loopback listener explicitly so the test server works in restricted environments too.
	if err != nil {                                                                                   // Skip the test when the environment cannot provide a loopback listener for the internal RPC server.
		t.Skipf("skipping rpc tls test because listen failed: %v", err) // Surface the environment limitation without flagging the production TLS path as broken.
	}
	serverSecurity := config.RPCSecurityConfig{Mode: "tls", ServerCertFile: serverCertPath, ServerKeyFile: serverKeyPath, AuthToken: "rpc-shared-token"} // Build the production server security config for first-stage TLS plus shared-token authorization.
	serverOptions, err := NewServerOptions(serverSecurity)                                                                                               // Build the production server options for the TLS transport plus shared-token authorization path.
	if err != nil {                                                                                                                                      // Stop immediately when the server options cannot be constructed from the supplied TLS config.
		t.Fatalf("failed to build rpc tls server options: %v", err) // Surface the option-construction failure because no TLS behavior can be asserted without a server.
	}
	server := grpc.NewServer(serverOptions...)                  // Construct one production gRPC server over the TLS credentials and shared-token interceptor returned by the helper under test.
	RegisterWorkerServiceServer(server, successWorkerService{}) // Register the simple success worker service so ExecutePlan reaches a concrete handler after transport and auth pass.
	go func() {
		_ = server.Serve(listener) // Serve requests in the background so the TLS-enabled client under test can connect to the loopback endpoint.
	}()
	defer func() {
		server.Stop()        // Stop the gRPC server immediately once the TLS assertions have completed.
		_ = listener.Close() // Close the loopback listener explicitly so the TCP port is released right away.
	}()

	clientSecurity := config.RPCSecurityConfig{Mode: "tls", CAFile: certificateAuthorityPath, ServerName: "localhost", AuthToken: "rpc-shared-token"} // Build the production client security config for TLS verification plus shared-token injection.
	client, err := NewWorkerClient(listener.Addr().String(), clientSecurity)                                                                          // Build the production worker client with the TLS verification config and shared token required by the server.
	if err != nil {                                                                                                                                   // Stop immediately when the TLS-enabled client cannot be constructed.
		t.Fatalf("failed to create tls worker client: %v", err) // Surface the client-construction failure because no TLS behavior can be asserted without it.
	}
	defer client.Close() // Ensure the TLS client connection is closed after the assertion completes.

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)                                                         // Bound the request so transport or auth failures return promptly.
	defer cancel()                                                                                                                // Release the timeout resources after the RPC completes.
	response, err := client.ExecutePlan(ctx, &ExecutePlanRequest{TraceID: "trace-1", SessionID: "session-1", Plan: []byte(`[]`)}) // Invoke the production worker client over real TLS transport with the required shared token.
	if err != nil {                                                                                                               // Fail the test when TLS handshake or shared-token auth unexpectedly rejects the request.
		t.Fatalf("expected TLS worker RPC to succeed, got error: %v", err) // Surface the unexpected transport or auth failure together with the returned error.
	}
	if response == nil || response.Status != "accepted" { // Fail the test when the accepted worker response does not make it back across the TLS connection.
		t.Fatalf("expected successful TLS worker RPC response, got %#v", response) // Surface the unexpected RPC response so transport drift is easy to diagnose.
	}
}

// writeTestRPCServerCertificateBundle generates one self-signed localhost server certificate and returns the CA, cert, and key file paths.
func writeTestRPCServerCertificateBundle(t *testing.T) (string, string, string) {
	t.Helper()                                            // Mark this helper so any failure points at the calling test rather than the helper body.
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048) // Generate one RSA private key so the TLS test can sign a real certificate chain.
	if err != nil {                                       // Stop immediately when the private key cannot be generated.
		t.Fatalf("failed to generate rpc tls test private key: %v", err) // Surface the key-generation failure because no certificate bundle can be written without it.
	}
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)     // Define the serial-number upper bound used for the self-signed test certificate.
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit) // Generate one random serial number so the certificate resembles a real issuance.
	if err != nil {                                               // Stop immediately when the serial number cannot be generated.
		t.Fatalf("failed to generate rpc tls test serial number: %v", err) // Surface the serial-number failure because no certificate can be written without it.
	}
	certificateTemplate := &x509.Certificate{ // Build one self-signed certificate template that is valid for localhost and the IPv4 loopback address used by the test listener.
		SerialNumber:          serialNumber,                                                                         // Attach the generated serial number so the certificate has a valid unique identifier.
		Subject:               pkix.Name{CommonName: "localhost"},                                                   // Set the common name to localhost because the TLS client verifies this logical server name explicitly in the test.
		NotBefore:             time.Now().Add(-time.Hour),                                                           // Start the validity window slightly in the past so clock skew does not break the TLS test.
		NotAfter:              time.Now().Add(time.Hour),                                                            // End the validity window in the near future because the certificate is test-only and should remain short-lived.
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign, // Allow the certificate to sign TLS handshakes and act as its own trust anchor for the test.
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},                                       // Mark the certificate for server-auth usage because the client will verify it as a TLS server cert.
		DNSNames:              []string{"localhost"},                                                                // Add localhost so TLS hostname verification succeeds when the client sets ServerName to localhost.
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},                                                   // Add the IPv4 loopback address so the certificate remains valid even if the client omits ServerName in future tests.
		BasicConstraintsValid: true,                                                                                 // Mark the certificate constraints as valid so x509 parsing accepts the generated chain.
		IsCA:                  true,                                                                                 // Reuse the same self-signed certificate as the trust anchor for this isolated test.
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, certificateTemplate, certificateTemplate, &privateKey.PublicKey, privateKey) // Create one self-signed certificate DER blob from the template and private key.
	if err != nil {                                                                                                                         // Stop immediately when certificate creation fails.
		t.Fatalf("failed to create rpc tls test certificate: %v", err) // Surface the certificate-creation failure because no TLS bundle can be written without it.
	}

	temporaryDirectory := t.TempDir()                                                                                        // Allocate one temporary directory so the generated PEM files are isolated to the current test.
	caPath := filepath.Join(temporaryDirectory, "rpc-test-ca.pem")                                                           // Build the CA bundle file path returned to the TLS client config.
	serverCertPath := filepath.Join(temporaryDirectory, "rpc-test-server-cert.pem")                                          // Build the server certificate file path returned to the TLS server config.
	serverKeyPath := filepath.Join(temporaryDirectory, "rpc-test-server-key.pem")                                            // Build the server private-key file path returned to the TLS server config.
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})                             // Encode the certificate DER blob into PEM so Go TLS helpers can load it from disk.
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}) // Encode the RSA private key into PEM so Go TLS helpers can load it from disk.
	if err := os.WriteFile(caPath, certificatePEM, 0o600); err != nil {                                                      // Write the CA bundle file used by the TLS client to verify the server certificate.
		t.Fatalf("failed to write rpc tls test CA file: %v", err) // Surface the CA write failure because the client cannot verify the server without it.
	}
	if err := os.WriteFile(serverCertPath, certificatePEM, 0o600); err != nil { // Write the server certificate file used by the gRPC server credentials helper.
		t.Fatalf("failed to write rpc tls test server certificate: %v", err) // Surface the certificate write failure because the server cannot start without it.
	}
	if err := os.WriteFile(serverKeyPath, privateKeyPEM, 0o600); err != nil { // Write the server private-key file used by the gRPC server credentials helper.
		t.Fatalf("failed to write rpc tls test server key: %v", err) // Surface the key write failure because the server cannot start without it.
	}

	return caPath, serverCertPath, serverKeyPath // Return the generated CA, certificate, and key file paths for the TLS test's client and server configs.
}
