package rpc

import (
	"context"

	"google.golang.org/grpc"
)

// OrchestratorServiceServer is the interface the orchestrator must implement
// so that workers can register and send heartbeats.
type OrchestratorServiceServer interface {
	RegisterWorker(ctx context.Context, req *RegisterWorkerRequest) (*RegisterWorkerResponse, error)
	Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error)
	ReportPlanEvent(ctx context.Context, req *ReportPlanEventRequest) (*ReportPlanEventResponse, error)
	RenewLease(ctx context.Context, req *RenewLeaseRequest) (*RenewLeaseResponse, error)
	CompletePlan(ctx context.Context, req *CompletePlanRequest) (*CompletePlanResponse, error)
}

// RegisterOrchestratorServiceServer registers srv with the gRPC server.
func RegisterOrchestratorServiceServer(s *grpc.Server, srv OrchestratorServiceServer) {
	s.RegisterService(&orchestratorServiceDesc, srv)
}

var orchestratorServiceDesc = grpc.ServiceDesc{
	ServiceName: "mcp.OrchestratorService",
	HandlerType: (*OrchestratorServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "RegisterWorker", Handler: orchestratorRegisterWorkerHandler},
		{MethodName: "Heartbeat", Handler: orchestratorHeartbeatHandler},
		{MethodName: "ReportPlanEvent", Handler: orchestratorReportPlanEventHandler},
		{MethodName: "RenewLease", Handler: orchestratorRenewLeaseHandler},
		{MethodName: "CompletePlan", Handler: orchestratorCompletePlanHandler},
	},
	Streams: []grpc.StreamDesc{},
}

// orchestratorRegisterWorkerHandler executes this operation.
func orchestratorRegisterWorkerHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterWorkerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OrchestratorServiceServer).RegisterWorker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/mcp.OrchestratorService/RegisterWorker"}
	return interceptor(ctx, in, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OrchestratorServiceServer).RegisterWorker(ctx, req.(*RegisterWorkerRequest))
	})
}

// orchestratorHeartbeatHandler executes this operation.
func orchestratorHeartbeatHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OrchestratorServiceServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/mcp.OrchestratorService/Heartbeat"}
	return interceptor(ctx, in, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OrchestratorServiceServer).Heartbeat(ctx, req.(*HeartbeatRequest))
	})
}

// orchestratorReportPlanEventHandler executes this operation.
func orchestratorReportPlanEventHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReportPlanEventRequest) // Allocate the distributed step-event callback request before the gRPC decoder populates it.
	if err := dec(in); err != nil {   // Stop immediately when the inbound gRPC payload cannot be decoded into the expected callback request shape.
		return nil, err // Preserve the decode failure so gRPC returns the canonical transport error to the worker caller.
	}
	if interceptor == nil { // Call the orchestrator callback handler directly when no unary interceptor wraps the current RPC.
		return srv.(OrchestratorServiceServer).ReportPlanEvent(ctx, in) // Delegate the decoded distributed step-event callback to the orchestrator service implementation.
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/mcp.OrchestratorService/ReportPlanEvent"}  // Describe the current unary callback RPC for any installed interceptor.
	return interceptor(ctx, in, info, func(ctx context.Context, req interface{}) (interface{}, error) { // Delegate through the interceptor chain while preserving the decoded callback request.
		return srv.(OrchestratorServiceServer).ReportPlanEvent(ctx, req.(*ReportPlanEventRequest)) // Invoke the orchestrator callback handler with the typed distributed step-event request.
	})
}

// orchestratorRenewLeaseHandler executes this operation.
func orchestratorRenewLeaseHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RenewLeaseRequest)    // Allocate the distributed lease-renewal request before the gRPC decoder populates it.
	if err := dec(in); err != nil { // Stop immediately when the inbound gRPC payload cannot be decoded into the expected renew request shape.
		return nil, err // Preserve the decode failure so gRPC returns the canonical transport error to the worker caller.
	}
	if interceptor == nil { // Call the orchestrator renew handler directly when no unary interceptor wraps the current RPC.
		return srv.(OrchestratorServiceServer).RenewLease(ctx, in) // Delegate the decoded lease-renewal callback to the orchestrator service implementation.
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/mcp.OrchestratorService/RenewLease"}       // Describe the current unary renewal RPC for any installed interceptor.
	return interceptor(ctx, in, info, func(ctx context.Context, req interface{}) (interface{}, error) { // Delegate through the interceptor chain while preserving the decoded renew request.
		return srv.(OrchestratorServiceServer).RenewLease(ctx, req.(*RenewLeaseRequest)) // Invoke the orchestrator renew handler with the typed distributed lease-renewal request.
	})
}

// orchestratorCompletePlanHandler executes this operation.
func orchestratorCompletePlanHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CompletePlanRequest)  // Allocate the distributed terminal-result request before the gRPC decoder populates it.
	if err := dec(in); err != nil { // Stop immediately when the inbound gRPC payload cannot be decoded into the expected completion request shape.
		return nil, err // Preserve the decode failure so gRPC returns the canonical transport error to the worker caller.
	}
	if interceptor == nil { // Call the orchestrator completion handler directly when no unary interceptor wraps the current RPC.
		return srv.(OrchestratorServiceServer).CompletePlan(ctx, in) // Delegate the decoded distributed terminal callback to the orchestrator service implementation.
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/mcp.OrchestratorService/CompletePlan"}     // Describe the current unary terminal-result RPC for any installed interceptor.
	return interceptor(ctx, in, info, func(ctx context.Context, req interface{}) (interface{}, error) { // Delegate through the interceptor chain while preserving the decoded completion request.
		return srv.(OrchestratorServiceServer).CompletePlan(ctx, req.(*CompletePlanRequest)) // Invoke the orchestrator completion handler with the typed distributed terminal request.
	})
}
