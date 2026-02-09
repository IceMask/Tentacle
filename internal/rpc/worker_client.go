package rpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	workerExecutePlanMethod = "/mcp.WorkerService/ExecutePlan"
	workerCancelPlanMethod  = "/mcp.WorkerService/CancelPlan"
)

// WorkerClient is a gRPC client for a single worker node.
type WorkerClient struct {
	conn *grpc.ClientConn
}

// NewWorkerClient dials the worker at the given address.
// The connection is lazy; it will not block until the first RPC.
func NewWorkerClient(address string) (*WorkerClient, error) {
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
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
