package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"mcp_for_appium/internal/appium"
	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/device"
	pb "mcp_for_appium/proto/v1"
)

// SessionService 会话服务实现
type SessionService struct {
	pb.UnimplementedSessionServiceServer

	deviceManager *device.Manager
	config        config.SessionConfig

	mu       sync.RWMutex
	sessions map[string]*SessionInfo
}

// SessionInfo 会话信息
type SessionInfo struct {
	Session      *pb.Session
	Device       device.Device
	AppiumClient *appium.Client
	LastActivity time.Time
}

// NewSessionService 创建会话服务
func NewSessionService(dm *device.Manager, cfg config.SessionConfig) *SessionService {
	s := &SessionService{
		deviceManager: dm,
		config:        cfg,
		sessions:      make(map[string]*SessionInfo),
	}

	// 启动会话清理器
	go s.sessionCleaner()

	return s
}

// CreateSession 创建会话
func (s *SessionService) CreateSession(ctx context.Context, req *pb.CreateSessionRequest) (*pb.CreateSessionResponse, error) {
	// 获取设备
	device, err := s.deviceManager.GetDevice(req.DeviceId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "device not found: %v", err)
	}

	// 检查设备是否可用
	if device.GetStatus() != pb.DeviceStatus_DEVICE_STATUS_BUSY {
		return nil, status.Errorf(codes.FailedPrecondition, "device is not allocated")
	}

	// 创建 Appium 客户端
	appiumClient := appium.NewClient(device.GetAppiumURL(), 30*time.Second)

	// 合并 desired capabilities
	caps := device.GetDesiredCapabilities()
	for k, v := range req.DesiredCapabilities {
		caps[k] = v
	}

	// 如果指定了 app，添加到 capabilities
	if req.AppId != "" {
		caps["app"] = req.AppId
	}

	// 创建 Appium 会话
	body := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"alwaysMatch": caps,
		},
	}

	respData, err := appiumClient.Request(ctx, "POST", "/session", body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create appium session: %v", err)
	}

	// 解析响应获取 session ID
	var sessionResp struct {
		SessionID    string                 `json:"sessionId"`
		Capabilities map[string]interface{} `json:"capabilities"`
	}

	if err := json.Unmarshal(respData, &sessionResp); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse session response: %v", err)
	}

	appiumClient.SetSessionID(sessionResp.SessionID)

	// 创建会话记录
	session := &pb.Session{
		Id:           generateSessionID(),
		DeviceId:     req.DeviceId,
		AppId:        req.AppId,
		Status:       pb.SessionStatus_SESSION_STATUS_ACTIVE,
		CreatedAt:    timestampNow(),
		UpdatedAt:    timestampNow(),
		Capabilities: req.DesiredCapabilities,
		Metadata:     make(map[string]string),
	}

	sessionInfo := &SessionInfo{
		Session:      session,
		Device:       device,
		AppiumClient: appiumClient,
		LastActivity: time.Now(),
	}

	s.mu.Lock()
	s.sessions[session.Id] = sessionInfo
	s.mu.Unlock()

	// 转换实际 capabilities 为 map[string]string
	actualCaps := make(map[string]string)
	for k, v := range sessionResp.Capabilities {
		actualCaps[k] = fmt.Sprintf("%v", v)
	}

	return &pb.CreateSessionResponse{
		Session:            session,
		ActualCapabilities: actualCaps,
	}, nil
}

// GetSession 获取会话信息
func (s *SessionService) GetSession(ctx context.Context, req *pb.GetSessionRequest) (*pb.Session, error) {
	s.mu.RLock()
	sessionInfo, ok := s.sessions[req.SessionId]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "session not found")
	}

	return sessionInfo.Session, nil
}

// EndSession 结束会话
func (s *SessionService) EndSession(ctx context.Context, req *pb.EndSessionRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	sessionInfo, ok := s.sessions[req.SessionId]
	if ok {
		delete(s.sessions, req.SessionId)
	}
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "session not found")
	}

	// 删除 Appium 会话
	if sessionInfo.AppiumClient != nil {
		_, _ = sessionInfo.AppiumClient.Request(ctx, "DELETE",
			fmt.Sprintf("/session/%s", sessionInfo.AppiumClient.GetSessionID()), nil)
	}

	sessionInfo.Session.Status = pb.SessionStatus_SESSION_STATUS_TERMINATED

	return &emptypb.Empty{}, nil
}

// GetSessionInfo 获取会话信息（内部使用）
func (s *SessionService) GetSessionInfo(sessionID string) (*SessionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sessionInfo, ok := s.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session not found")
	}

	// 更新最后活动时间
	sessionInfo.LastActivity = time.Now()

	return sessionInfo, nil
}

// sessionCleaner 定期清理过期会话
func (s *SessionService) sessionCleaner() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.cleanupExpiredSessions()
	}
}

// cleanupExpiredSessions 清理过期会话
func (s *SessionService) cleanupExpiredSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, sessionInfo := range s.sessions {
		// 检查空闲超时
		if now.Sub(sessionInfo.LastActivity) > s.config.IdleTimeout {
			// 删除 Appium 会话
			if sessionInfo.AppiumClient != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, _ = sessionInfo.AppiumClient.Request(ctx, "DELETE",
					fmt.Sprintf("/session/%s", sessionInfo.AppiumClient.GetSessionID()), nil)
				cancel()
			}

			delete(s.sessions, id)
		}
	}
}

// 辅助函数
func generateSessionID() string {
	return fmt.Sprintf("session_%d", time.Now().UnixNano())
}

func timestampNow() *timestamppb.Timestamp {
	return timestamppb.Now()
}
