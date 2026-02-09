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
	},
	Streams: []grpc.StreamDesc{},
}

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
