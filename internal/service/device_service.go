package service

import (
	"context"
	"fmt"

	"mcp_for_appium/internal/device"
	pb "mcp_for_appium/proto/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// DeviceService 设备服务实现
type DeviceService struct {
	pb.UnimplementedDeviceServiceServer
	deviceManager *device.Manager
}

// NewDeviceService 创建设备服务
func NewDeviceService(dm *device.Manager) *DeviceService {
	return &DeviceService{
		deviceManager: dm,
	}
}

// ListDevices 列出可用设备
func (s *DeviceService) ListDevices(ctx context.Context, req *pb.ListDevicesRequest) (*pb.ListDevicesResponse, error) {
	devices, err := s.deviceManager.ListDevices(ctx, req.Filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list devices: %v", err)
	}

	// 处理分页
	totalCount := len(devices)

	// 简单的分页实现
	pageSize := int(req.PageSize)
	if pageSize <= 0 {
		pageSize = 20
	}

	start := 0
	if req.PageToken != "" {
		// 简单实现：page token 就是起始索引
		fmt.Sscanf(req.PageToken, "%d", &start)
	}

	end := start + pageSize
	if end > totalCount {
		end = totalCount
	}

	var nextPageToken string
	if end < totalCount {
		nextPageToken = fmt.Sprintf("%d", end)
	}

	return &pb.ListDevicesResponse{
		Devices:       devices[start:end],
		NextPageToken: nextPageToken,
		TotalCount:    int32(totalCount),
	}, nil
}

// GetDevice 获取设备信息
func (s *DeviceService) GetDevice(ctx context.Context, req *pb.GetDeviceRequest) (*pb.Device, error) {
	device, err := s.deviceManager.GetDevice(req.DeviceId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "device not found: %v", err)
	}

	return &pb.Device{
		Id:            device.GetID(),
		Type:          device.GetType(),
		Status:        device.GetStatus(),
		Capabilities:  device.GetCapabilities(),
		Properties:    device.GetProperties(),
		ResourceUsage: device.GetResourceUsage(),
	}, nil
}

// AllocateDevice 分配设备
func (s *DeviceService) AllocateDevice(ctx context.Context, req *pb.AllocateDeviceRequest) (*pb.AllocateDeviceResponse, error) {
	resp, err := s.deviceManager.AllocateDevice(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "failed to allocate device: %v", err)
	}

	return resp, nil
}

// ReleaseDevice 释放设备
func (s *DeviceService) ReleaseDevice(ctx context.Context, req *pb.ReleaseDeviceRequest) (*emptypb.Empty, error) {
	err := s.deviceManager.ReleaseDevice(ctx, req.AllocationId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to release device: %v", err)
	}

	return &emptypb.Empty{}, nil
}

// StreamDeviceStatus 流式传输设备状态
func (s *DeviceService) StreamDeviceStatus(req *pb.StreamDeviceStatusRequest, stream pb.DeviceService_StreamDeviceStatusServer) error {
	// TODO: 实现设备状态流
	// 这里可以定期检查设备状态并推送更新
	return status.Errorf(codes.Unimplemented, "not implemented")
}

// StreamDeviceMetrics 流式传输设备指标
func (s *DeviceService) StreamDeviceMetrics(req *pb.StreamDeviceMetricsRequest, stream pb.DeviceService_StreamDeviceMetricsServer) error {
	// TODO: 实现设备指标流
	return status.Errorf(codes.Unimplemented, "not implemented")
}
