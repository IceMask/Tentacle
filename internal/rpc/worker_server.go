package rpc

import (
	"context"

	"google.golang.org/grpc"
)

// WorkerServiceServer is the interface that a worker must implement.
type WorkerServiceServer interface {
	ExecutePlan(ctx context.Context, req *ExecutePlanRequest) (*ExecutePlanResponse, error)
	CancelPlan(ctx context.Context, req *CancelPlanRequest) (*CancelPlanResponse, error)
}

// RegisterWorkerServiceServer registers srv with the gRPC server.
func RegisterWorkerServiceServer(s *grpc.Server, srv WorkerServiceServer) {
	s.RegisterService(&workerServiceDesc, srv)
}

var workerServiceDesc = grpc.ServiceDesc{
	ServiceName: "mcp.WorkerService",
	HandlerType: (*WorkerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "ExecutePlan", Handler: workerExecutePlanHandler},
		{MethodName: "CancelPlan", Handler: workerCancelPlanHandler},
	},
	Streams: []grpc.StreamDesc{},
}

// workerExecutePlanHandler executes this operation.
func workerExecutePlanHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecutePlanRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerServiceServer).ExecutePlan(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/mcp.WorkerService/ExecutePlan"}
	return interceptor(ctx, in, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerServiceServer).ExecutePlan(ctx, req.(*ExecutePlanRequest))
	})
}

// workerCancelPlanHandler executes this operation.
func workerCancelPlanHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CancelPlanRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerServiceServer).CancelPlan(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/mcp.WorkerService/CancelPlan"}
	return interceptor(ctx, in, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerServiceServer).CancelPlan(ctx, req.(*CancelPlanRequest))
	})
}
