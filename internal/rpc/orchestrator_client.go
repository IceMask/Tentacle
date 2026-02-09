package rpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	orchRegisterWorkerMethod = "/mcp.OrchestratorService/RegisterWorker"
	orchHeartbeatMethod      = "/mcp.OrchestratorService/Heartbeat"
)

// OrchestratorClient is used by workers to register and send heartbeats.
type OrchestratorClient struct {
	conn *grpc.ClientConn
}

// NewOrchestratorClient dials the orchestrator at the given address.
func NewOrchestratorClient(address string) (*OrchestratorClient, error) {
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
	)
	if err != nil {
		return nil, err
	}
	return &OrchestratorClient{conn: conn}, nil
}

// Close releases the underlying connection.
func (c *OrchestratorClient) Close() error {
	return c.conn.Close()
}

// RegisterWorker registers this worker with the orchestrator.
func (c *OrchestratorClient) RegisterWorker(ctx context.Context, req *RegisterWorkerRequest) (*RegisterWorkerResponse, error) {
	resp := new(RegisterWorkerResponse)
	if err := c.conn.Invoke(ctx, orchRegisterWorkerMethod, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// Heartbeat sends a periodic heartbeat.
func (c *OrchestratorClient) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	resp := new(HeartbeatResponse)
	if err := c.conn.Invoke(ctx, orchHeartbeatMethod, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}
