package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/telemetry"

	"google.golang.org/grpc"
)

// GRPCServer is the orchestrator's gRPC server that workers connect to
// for registration and heartbeat reporting.
type GRPCServer struct {
	server   *grpc.Server
	registry *WorkerRegistry
	logger   *slog.Logger
}

// NewGRPCServer creates and registers the orchestrator gRPC service.
func NewGRPCServer(registry *WorkerRegistry) *GRPCServer {
	s := grpc.NewServer()
	gs := &GRPCServer{
		server:   s,
		registry: registry,
		logger:   telemetry.Logger(),
	}
	rpc.RegisterOrchestratorServiceServer(s, gs)
	return gs
}

// Start listens on the given port and serves gRPC requests (blocks).
func (gs *GRPCServer) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	gs.logger.Info("orchestrator gRPC server listening", "port", port)
	return gs.server.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (gs *GRPCServer) Stop() {
	gs.server.GracefulStop()
	gs.logger.Info("orchestrator gRPC server stopped")
}

// RegisterWorker implements rpc.OrchestratorServiceServer.
func (gs *GRPCServer) RegisterWorker(ctx context.Context, req *rpc.RegisterWorkerRequest) (*rpc.RegisterWorkerResponse, error) {
	node := &WorkerNode{
		ID:       req.WorkerID,
		Address:  req.Address,
		Capacity: req.Capacity,
		Tags:     req.Tags,
	}
	if err := gs.registry.Register(ctx, node); err != nil {
		return &rpc.RegisterWorkerResponse{Success: false, Message: err.Error()}, nil
	}
	gs.logger.InfoContext(ctx, "worker registered via gRPC",
		"worker_id", req.WorkerID,
		"address", req.Address)
	return &rpc.RegisterWorkerResponse{Success: true}, nil
}

// Heartbeat implements rpc.OrchestratorServiceServer.
func (gs *GRPCServer) Heartbeat(ctx context.Context, req *rpc.HeartbeatRequest) (*rpc.HeartbeatResponse, error) {
	if err := gs.registry.Heartbeat(ctx, req.WorkerID, req.ActiveLoad); err != nil {
		return &rpc.HeartbeatResponse{Success: false, Message: err.Error()}, nil
	}
	return &rpc.HeartbeatResponse{Success: true}, nil
}
