package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"mcp_for_appium/internal/device"
	pb "mcp_for_appium/proto/v1"
)

// ScreenService 屏幕服务实现
type ScreenService struct {
	pb.UnimplementedScreenServiceServer

	deviceManager  *device.Manager
	sessionService *SessionService
}

// NewScreenService 创建屏幕服务
func NewScreenService(dm *device.Manager) *ScreenService {
	return &ScreenService{
		deviceManager: dm,
	}
}

// SetSessionService 设置会话服务
func (s *ScreenService) SetSessionService(ss *SessionService) {
	s.sessionService = ss
}

// TakeScreenshot 截图
func (s *ScreenService) TakeScreenshot(ctx context.Context, req *pb.TakeScreenshotRequest) (*pb.Screenshot, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	// 调用 Appium 截图
	path := fmt.Sprintf("/session/%s/screenshot", sessionInfo.AppiumClient.GetSessionID())
	respData, err := sessionInfo.AppiumClient.Request(ctx, "GET", path, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to take screenshot: %v", err)
	}

	// Appium 返回 base64 编码的图片
	var base64Data string
	if err := json.Unmarshal(respData, &base64Data); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse screenshot: %v", err)
	}

	// 解码 base64
	imageData, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to decode screenshot: %v", err)
	}

	return &pb.Screenshot{
		Data:       imageData,
		Format:     "png",
		CapturedAt: timestamppb.Now(),
		Metadata: map[string]string{
			"session_id": req.SessionId,
		},
	}, nil
}

// GetScreenLayout 获取屏幕布局
func (s *ScreenService) GetScreenLayout(ctx context.Context, req *pb.GetScreenLayoutRequest) (*pb.ScreenLayout, error) {
	sessionInfo, err := s.sessionService.GetSessionInfo(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %v", err)
	}

	// 获取页面源码
	path := fmt.Sprintf("/session/%s/source", sessionInfo.AppiumClient.GetSessionID())
	respData, err := sessionInfo.AppiumClient.Request(ctx, "GET", path, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get page source: %v", err)
	}

	var pageSource string
	if err := json.Unmarshal(respData, &pageSource); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse page source: %v", err)
	}

	// TODO: 解析 XML/JSON 生成元素树
	// 现在先返回原始数据
	return &pb.ScreenLayout{
		RootElement: &pb.Element{
			Id:         "root",
			Type:       "hierarchy",
			Attributes: map[string]string{"source": pageSource},
		},
		SourceFormat: "xml",
		CapturedAt:   timestamppb.Now(),
	}, nil
}

// 其他方法暂时返回未实现
func (s *ScreenService) StartRecording(ctx context.Context, req *pb.StartRecordingRequest) (*pb.RecordingSession, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ScreenService) StopRecording(ctx context.Context, req *pb.StopRecordingRequest) (*pb.Recording, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ScreenService) CompareScreens(ctx context.Context, req *pb.CompareScreensRequest) (*pb.ScreenComparison, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ScreenService) FindVisualElement(ctx context.Context, req *pb.FindVisualElementRequest) (*pb.FindVisualElementResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ScreenService) StreamScreen(req *pb.StreamScreenRequest, stream pb.ScreenService_StreamScreenServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *ScreenService) StreamLayoutChanges(req *pb.StreamLayoutChangesRequest, stream pb.ScreenService_StreamLayoutChangesServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}
