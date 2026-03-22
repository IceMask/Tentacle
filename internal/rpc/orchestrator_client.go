package rpc

import (
	"context"

	"mcp_for_appium/internal/config"

	"google.golang.org/grpc"
)

const (
	orchRegisterWorkerMethod = "/mcp.OrchestratorService/RegisterWorker"
	orchHeartbeatMethod      = "/mcp.OrchestratorService/Heartbeat"
)

// OrchestratorClient is used by workers to register and send heartbeats.
type OrchestratorClient struct {
	conn *grpc.ClientConn
}

// NewOrchestratorClient dials the orchestrator at the given address using the optional internal RPC security configuration.
func NewOrchestratorClient(address string, security ...config.RPCSecurityConfig) (*OrchestratorClient, error) {
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
