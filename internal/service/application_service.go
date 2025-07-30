package service

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"mcp_for_appium/internal/device"
	pb "mcp_for_appium/proto/v1"
)

// ApplicationService 应用服务实现
type ApplicationService struct {
	pb.UnimplementedApplicationServiceServer

	deviceManager  *device.Manager
	sessionService *SessionService
}

// NewApplicationService 创建应用服务
func NewApplicationService(dm *device.Manager) *ApplicationService {
	return &ApplicationService{
		deviceManager: dm,
	}
}

// SetSessionService 设置会话服务
func (s *ApplicationService) SetSessionService(ss *SessionService) {
	s.sessionService = ss
}

// LaunchApp 启动应用
func (s *ApplicationService) LaunchApp(ctx context.Context, req *pb.LaunchAppRequest) (*pb.LaunchAppResponse, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	// Appium 启动应用
	body := map[string]interface{}{
		"bundleId":   req.AppId, // iOS
		"appPackage": req.AppId, // Android
	}

	path := fmt.Sprintf("/session/%s/appium/device/activate_app", sessionInfo.AppiumClient.GetSessionID())
	_, err = sessionInfo.AppiumClient.Request(ctx, "POST", path, body)

	if err != nil {
		return &pb.LaunchAppResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.LaunchAppResponse{
		Success: true,
	}, nil
}

// TerminateApp 终止应用
func (s *ApplicationService) TerminateApp(ctx context.Context, req *pb.TerminateAppRequest) (*emptypb.Empty, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	body := map[string]interface{}{
		"bundleId":   req.AppId, // iOS
		"appPackage": req.AppId, // Android
	}

	path := fmt.Sprintf("/session/%s/appium/device/terminate_app", sessionInfo.AppiumClient.GetSessionID())
	_, _ = sessionInfo.AppiumClient.Request(ctx, "POST", path, body)

	return &emptypb.Empty{}, nil
}

// GetAppState 获取应用状态
func (s *ApplicationService) GetAppState(ctx context.Context, req *pb.GetAppStateRequest) (*pb.AppState, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	body := map[string]interface{}{
		"bundleId":   req.AppId, // iOS
		"appPackage": req.AppId, // Android
	}

	path := fmt.Sprintf("/session/%s/appium/device/app_state", sessionInfo.AppiumClient.GetSessionID())
	respData, err := sessionInfo.AppiumClient.Request(ctx, "POST", path, body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get app state: %v", err)
	}

	var state int
	json.Unmarshal(respData, &state)

	// 转换 Appium 状态码到我们的枚举
	appStatus := pb.AppStatus_APP_STATUS_UNSPECIFIED
	switch state {
	case 0:
		appStatus = pb.AppStatus_APP_STATUS_NOT_INSTALLED
	case 1:
		appStatus = pb.AppStatus_APP_STATUS_NOT_RUNNING
	case 3:
		appStatus = pb.AppStatus_APP_STATUS_RUNNING_BACKGROUND
	case 4:
		appStatus = pb.AppStatus_APP_STATUS_RUNNING_FOREGROUND
	}

	return &pb.AppState{
		AppId:  req.AppId,
		Status: appStatus,
	}, nil
}

// 其他方法暂时返回未实现
func (s *ApplicationService) InstallApp(ctx context.Context, req *pb.InstallAppRequest) (*pb.InstallAppResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ApplicationService) UninstallApp(ctx context.Context, req *pb.UninstallAppRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ApplicationService) GetAppInfo(ctx context.Context, req *pb.GetAppInfoRequest) (*pb.AppInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ApplicationService) ResetApp(ctx context.Context, req *pb.ResetAppRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ApplicationService) BackupAppData(ctx context.Context, req *pb.BackupAppDataRequest) (*pb.BackupInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ApplicationService) RestoreAppData(ctx context.Context, req *pb.RestoreAppDataRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}
