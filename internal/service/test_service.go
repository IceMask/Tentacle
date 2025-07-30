package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/device"
	pb "mcp_for_appium/proto/v1"
)

// TestService 测试服务实现
type TestService struct {
	pb.UnimplementedTestServiceServer

	deviceManager *device.Manager
	config        config.TestConfig
}

// NewTestService 创建测试服务
func NewTestService(dm *device.Manager, cfg config.TestConfig) *TestService {
	return &TestService{
		deviceManager: dm,
		config:        cfg,
	}
}

// 暂时所有方法都返回未实现
func (s *TestService) RunTest(ctx context.Context, req *pb.RunTestRequest) (*pb.TestResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *TestService) RunTestSuite(ctx context.Context, req *pb.RunTestSuiteRequest) (*pb.TestSuiteResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *TestService) StreamTestExecution(req *pb.StreamTestExecutionRequest, stream pb.TestService_StreamTestExecutionServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *TestService) CreateTestPlan(ctx context.Context, req *pb.CreateTestPlanRequest) (*pb.TestPlan, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *TestService) GetTestHistory(ctx context.Context, req *pb.GetTestHistoryRequest) (*pb.TestHistory, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *TestService) RunParallelTests(req *pb.RunParallelTestsRequest, stream pb.TestService_RunParallelTestsServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}
