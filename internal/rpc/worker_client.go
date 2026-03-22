package rpc

import (
	"context"

	"mcp_for_appium/internal/config"

	"google.golang.org/grpc"
)

const (
	workerExecutePlanMethod = "/mcp.WorkerService/ExecutePlan"
	workerCancelPlanMethod  = "/mcp.WorkerService/CancelPlan"
)

// WorkerClient is a gRPC client for a single worker node.
type WorkerClient struct {
	conn *grpc.ClientConn
}

// NewWorkerClient dials the worker at the given address using the optional internal RPC security configuration.
// The connection is lazy; it will not block until the first RPC.
func NewWorkerClient(address string, security ...config.RPCSecurityConfig) (*WorkerClient, error) {
	var securityConfig config.RPCSecurityConfig // Start from the zero-value compatibility config so callers can omit explicit security configuration.
	if len(security) > 0 {                      // Reuse the first supplied security config only when the caller opts into the v4.4 internal RPC security model.
		securityConfig = security[0] // Copy the caller-supplied security config so dial-option construction remains deterministic.
	}
	dialOptions, err := NewDialOptions(securityConfig) // Build the internal RPC dial options that match the configured transport mode and shared auth token.
	if err != nil {                                    // Stop immediately when the security config cannot be turned into valid gRPC dial options.
		return nil, err // Preserve the dial-option construction failure for the caller.
	}
	conn, err := grpc.NewClient(address, dialOptions...)
	if err != nil {
		return nil, err
	}
	return &WorkerClient{conn: conn}, nil
}

// Close releases the underlying connection.
func (c *WorkerClient) Close() error {
	return c.conn.Close()
}

// ExecutePlan asks the worker to run a plan.
func (c *WorkerClient) ExecutePlan(ctx context.Context, req *ExecutePlanRequest) (*ExecutePlanResponse, error) {
	resp := new(ExecutePlanResponse)
	if err := c.conn.Invoke(ctx, workerExecutePlanMethod, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// CancelPlan asks the worker to cancel an in-flight plan.
func (c *WorkerClient) CancelPlan(ctx context.Context, req *CancelPlanRequest) (*CancelPlanResponse, error) {
	resp := new(CancelPlanResponse)
	if err := c.conn.Invoke(ctx, workerCancelPlanMethod, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}
