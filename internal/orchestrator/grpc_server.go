package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/telemetry"

	"google.golang.org/grpc"
)

// GRPCServer is the orchestrator's gRPC server that workers connect to
// for registration and heartbeat reporting.
type GRPCServer struct {
	server   *grpc.Server
	service  *Service
	registry *WorkerRegistry
	logger   *slog.Logger
}

// NewGRPCServer creates and registers the orchestrator gRPC service with the configured internal RPC security mode.
func NewGRPCServer(service *Service, security config.RPCSecurityConfig) (*GRPCServer, error) {
	serverOptions, err := rpc.NewServerOptions(security) // Build the transport-credential and shared-token server options required by the configured internal RPC mode.
	if err != nil {                                      // Stop immediately when the security config cannot be turned into valid gRPC server options.
		return nil, err // Preserve the server-option construction failure for the caller.
	}
	s := grpc.NewServer(serverOptions...) // Construct the gRPC server with the configured security options so worker registration traffic follows the selected transport mode.
	registry := service.Registry()        // Reuse the service-owned worker registry so registration, heartbeat, and distributed callbacks share one runtime state source.
	gs := &GRPCServer{
		server:   s,
		service:  service,
		registry: registry,
		logger:   telemetry.Logger(),
	}
	rpc.RegisterOrchestratorServiceServer(s, gs)
	return gs, nil
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

// ReportPlanEvent implements rpc.OrchestratorServiceServer.
func (gs *GRPCServer) ReportPlanEvent(ctx context.Context, req *rpc.ReportPlanEventRequest) (*rpc.ReportPlanEventResponse, error) {
	accepted, err := gs.service.ReportDistributedPlanEvent(ctx, req) // Delegate the worker-emitted step event to the orchestrator service so ownership checks and DAO appends stay centralized.
	if err != nil {                                                  // Surface service-layer callback failures while still returning a structured callback response payload.
		return &rpc.ReportPlanEventResponse{Accepted: false, Message: err.Error()}, nil // Return the wrapped failure text because gRPC transport success keeps worker retry logic simpler here.
	}

	return &rpc.ReportPlanEventResponse{Accepted: accepted, Stale: !accepted}, nil // Report whether the worker-emitted step event was appended or rejected as stale.
}

// RenewLease implements rpc.OrchestratorServiceServer.
func (gs *GRPCServer) RenewLease(ctx context.Context, req *rpc.RenewLeaseRequest) (*rpc.RenewLeaseResponse, error) {
	accepted, err := gs.service.RenewDistributedExecutionLease(ctx, req.TraceID, req.WorkerID, req.Attempt) // Delegate the lease renewal to the orchestrator service so Redis-backed ownership checks stay centralized.
	if err != nil {                                                                                         // Surface service-layer lease-renewal failures while still returning a structured callback response payload.
		return &rpc.RenewLeaseResponse{Accepted: false, Message: err.Error()}, nil // Return the wrapped failure text because gRPC transport success keeps worker retry logic simpler here.
	}

	return &rpc.RenewLeaseResponse{Accepted: accepted, Stale: !accepted}, nil // Report whether the distributed lease was refreshed or rejected as stale.
}

// CompletePlan implements rpc.OrchestratorServiceServer.
func (gs *GRPCServer) CompletePlan(ctx context.Context, req *rpc.CompletePlanRequest) (*rpc.CompletePlanResponse, error) {
	accepted, err := gs.service.CompleteDistributedPlan(ctx, req) // Delegate the worker terminal callback to the orchestrator service so attempt guards and terminalization stay centralized.
	if err != nil {                                               // Surface service-layer terminalization failures while still returning a structured callback response payload.
		return &rpc.CompletePlanResponse{Accepted: false, Message: err.Error()}, nil // Return the wrapped failure text because gRPC transport success keeps worker retry logic simpler here.
	}

	return &rpc.CompletePlanResponse{Accepted: accepted, Stale: !accepted}, nil // Report whether the worker terminal callback finalized the trace or was rejected as stale.
}
