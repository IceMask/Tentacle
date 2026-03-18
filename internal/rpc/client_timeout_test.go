// client_timeout_test.go verifies that the gRPC client wrappers fail fast when the remote peer accepts connections but never responds to unary RPCs.
package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// hangingWorkerService blocks every worker RPC until the caller's context is canceled or times out.
type hangingWorkerService struct{}

// ExecutePlan blocks until the RPC context is done so client-side deadline handling can be tested deterministically.
func (hangingWorkerService) ExecutePlan(ctx context.Context, req *ExecutePlanRequest) (*ExecutePlanResponse, error) {
	<-ctx.Done()          // Hold the unary RPC open without sending a response so the client deadline must terminate the call.
	return nil, ctx.Err() // Return the canceled context error once the client deadline has elapsed and the server context is torn down.
}

// CancelPlan blocks until the RPC context is done so client-side deadline handling can be tested deterministically.
func (hangingWorkerService) CancelPlan(ctx context.Context, req *CancelPlanRequest) (*CancelPlanResponse, error) {
	<-ctx.Done()          // Hold the unary RPC open without sending a response so the client deadline must terminate the call.
	return nil, ctx.Err() // Return the canceled context error once the client deadline has elapsed and the server context is torn down.
}

// hangingOrchestratorService blocks every orchestrator RPC until the caller's context is canceled or times out.
type hangingOrchestratorService struct{}

// RegisterWorker blocks until the RPC context is done so client-side deadline handling can be tested deterministically.
func (hangingOrchestratorService) RegisterWorker(ctx context.Context, req *RegisterWorkerRequest) (*RegisterWorkerResponse, error) {
	<-ctx.Done()          // Hold the unary RPC open without sending a response so the client deadline must terminate the call.
	return nil, ctx.Err() // Return the canceled context error once the client deadline has elapsed and the server context is torn down.
}

// Heartbeat blocks until the RPC context is done so client-side deadline handling can be tested deterministically.
func (hangingOrchestratorService) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	<-ctx.Done()          // Hold the unary RPC open without sending a response so the client deadline must terminate the call.
	return nil, ctx.Err() // Return the canceled context error once the client deadline has elapsed and the server context is torn down.
}

// startLoopbackGRPCServer starts one IPv4 loopback gRPC server and returns its address plus a stop function for the current test.
func startLoopbackGRPCServer(t *testing.T, register func(*grpc.Server)) (string, func()) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0") // Bind an IPv4 loopback listener explicitly so the test server works in restricted sandbox environments too.
	if err != nil {                                    // Stop immediately when the test environment cannot provide a loopback TCP listener.
		t.Skipf("skipping grpc timeout test because listen failed: %v", err) // Surface the environment limitation without flagging the production timeout path as broken.
		return "", func() {}                                                 // Return a no-op server handle after the skip to satisfy the compiler even though execution will not continue.
	}

	server := grpc.NewServer() // Construct one fresh gRPC server instance so the current test owns its entire unary-handler lifecycle.
	register(server)           // Register the specific hanging test service implementation needed by the current timeout test.
	go func() {
		_ = server.Serve(listener) // Serve requests in the background so the client wrapper can connect and block on the hanging unary handler.
	}()

	return listener.Addr().String(), func() { // Return the loopback address together with a stop closure so tests can clean up deterministically.
		server.Stop()        // Stop the gRPC server immediately once the timeout assertions have completed.
		_ = listener.Close() // Close the loopback listener explicitly so the TCP port is released right away.
	}
}

// TestWorkerClientExecutePlanRespectsDeadline verifies that the worker gRPC client wrapper returns promptly when the remote worker never responds.
func TestWorkerClientExecutePlanRespectsDeadline(t *testing.T) {
	address, stopServer := startLoopbackGRPCServer(t, func(server *grpc.Server) { RegisterWorkerServiceServer(server, hangingWorkerService{}) }) // Start one hanging worker gRPC server that accepts ExecutePlan calls but never answers.
	defer stopServer()                                                                                                                           // Ensure the temporary gRPC server is always stopped after the test completes.

	client, err := NewWorkerClient(address) // Build the production worker client wrapper against the hanging loopback gRPC server.
	if err != nil {                         // Stop immediately when the client wrapper cannot dial the test gRPC server.
		t.Fatalf("failed to create worker client: %v", err) // Surface the client-construction failure because no deadline behavior can be asserted without it.
	}
	defer client.Close() // Ensure the client connection is closed after the timeout assertion completes.

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)                                                    // Bound the worker RPC with a short timeout so the hanging-server test completes quickly.
	defer cancel()                                                                                                                    // Release the timeout resources once the worker RPC returns.
	startedAt := time.Now()                                                                                                           // Capture the RPC start time so the test can verify the client fails promptly.
	_, err = client.ExecutePlan(ctx, &ExecutePlanRequest{TraceID: "trace-timeout", SessionID: "session-timeout", Plan: []byte(`[]`)}) // Invoke the production worker client against the hanging unary handler.
	elapsed := time.Since(startedAt)                                                                                                  // Measure the wall-clock duration so the test can verify fail-fast deadline handling.
	if err == nil {                                                                                                                   // Fail the test when the hanging server unexpectedly yields a successful response.
		t.Fatal("expected worker ExecutePlan timeout error") // Surface the missing expected timeout failure clearly.
	}
	if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Canceled { // Fail the test when the gRPC error code does not reflect a deadline-driven cancellation path.
		t.Fatalf("expected worker ExecutePlan deadline/cancelled code, got %s (%v)", code, err) // Surface the unexpected gRPC code because callers depend on it for retry and timeout handling.
	}
	if elapsed >= time.Second { // Fail the test when the worker client hangs far beyond its timeout budget instead of failing promptly.
		t.Fatalf("expected worker ExecutePlan to fail fast, took %v", elapsed) // Surface the slow failure because unresponsive workers must not stall dispatch indefinitely.
	}
}

// TestOrchestratorClientHeartbeatRespectsDeadline verifies that the orchestrator gRPC client wrapper returns promptly when the remote orchestrator never responds.
func TestOrchestratorClientHeartbeatRespectsDeadline(t *testing.T) {
	address, stopServer := startLoopbackGRPCServer(t, func(server *grpc.Server) { RegisterOrchestratorServiceServer(server, hangingOrchestratorService{}) }) // Start one hanging orchestrator gRPC server that accepts Heartbeat calls but never answers.
	defer stopServer()                                                                                                                                       // Ensure the temporary gRPC server is always stopped after the test completes.

	client, err := NewOrchestratorClient(address) // Build the production orchestrator client wrapper against the hanging loopback gRPC server.
	if err != nil {                               // Stop immediately when the client wrapper cannot dial the test gRPC server.
		t.Fatalf("failed to create orchestrator client: %v", err) // Surface the client-construction failure because no deadline behavior can be asserted without it.
	}
	defer client.Close() // Ensure the client connection is closed after the timeout assertion completes.

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)               // Bound the heartbeat RPC with a short timeout so the hanging-server test completes quickly.
	defer cancel()                                                                               // Release the timeout resources once the heartbeat RPC returns.
	startedAt := time.Now()                                                                      // Capture the RPC start time so the test can verify the client fails promptly.
	_, err = client.Heartbeat(ctx, &HeartbeatRequest{WorkerID: "worker-timeout", ActiveLoad: 1}) // Invoke the production orchestrator client against the hanging unary handler.
	elapsed := time.Since(startedAt)                                                             // Measure the wall-clock duration so the test can verify fail-fast deadline handling.
	if err == nil {                                                                              // Fail the test when the hanging server unexpectedly yields a successful response.
		t.Fatal("expected orchestrator Heartbeat timeout error") // Surface the missing expected timeout failure clearly.
	}
	if code := status.Code(err); code != codes.DeadlineExceeded && code != codes.Canceled { // Fail the test when the gRPC error code does not reflect a deadline-driven cancellation path.
		t.Fatalf("expected orchestrator Heartbeat deadline/cancelled code, got %s (%v)", code, err) // Surface the unexpected gRPC code because callers depend on it for worker liveness handling.
	}
	if elapsed >= time.Second { // Fail the test when the orchestrator client hangs far beyond its timeout budget instead of failing promptly.
		t.Fatalf("expected orchestrator Heartbeat to fail fast, took %v", elapsed) // Surface the slow failure because unresponsive orchestrators must not stall workers indefinitely.
	}
}
