package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"mcp_for_appium/internal/device"
	pb "mcp_for_appium/proto/v1"
)

// SystemService 系统服务实现
type SystemService struct {
	pb.UnimplementedSystemServiceServer

	deviceManager *device.Manager
}

// NewSystemService 创建系统服务
func NewSystemService(dm *device.Manager) *SystemService {
	return &SystemService{
		deviceManager: dm,
	}
}

// 暂时所有方法都返回未实现
func (s *SystemService) RotateDevice(ctx context.Context, req *pb.RotateDeviceRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *SystemService) SetLocation(ctx context.Context, req *pb.SetLocationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *SystemService) SetNetwork(ctx context.Context, req *pb.SetNetworkRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *SystemService) SimulateCall(ctx context.Context, req *pb.SimulateCallRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *SystemService) SimulateSMS(ctx context.Context, req *pb.SimulateSMSRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *SystemService) ExecuteShellCommand(ctx context.Context, req *pb.ExecuteShellCommandRequest) (*pb.ShellCommandResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *SystemService) GetSystemLogs(req *pb.GetSystemLogsRequest, stream pb.SystemService_GetSystemLogsServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *SystemService) GetPerformanceMetrics(ctx context.Context, req *pb.GetPerformanceMetricsRequest) (*pb.PerformanceMetrics, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}
