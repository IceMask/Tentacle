package device

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"mcp_for_appium/internal/config"
	pb "mcp_for_appium/proto/v1"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrDeviceNotFound     = errors.New("device not found")
	ErrDeviceNotAvailable = errors.New("device not available")
	ErrNoDeviceAvailable  = errors.New("no device available")
	ErrInvalidDeviceType  = errors.New("invalid device type")
)

// Device 接口定义了所有设备类型的通用操作
type Device interface {
	GetID() string
	GetType() pb.DeviceType
	GetStatus() pb.DeviceStatus
	GetCapabilities() *pb.DeviceCapabilities
	GetProperties() map[string]string

	// 连接管理
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
	IsConnected() bool

	// 健康检查
	HealthCheck(ctx context.Context) error
	GetResourceUsage() *pb.ResourceUsage

	// Appium 相关
	GetAppiumURL() string
	GetDesiredCapabilities() map[string]interface{}
}

// Manager 设备管理器
type Manager struct {
	mu sync.RWMutex

	// 设备池
	devices map[string]Device

	// 设备分配记录
	allocations map[string]*Allocation

	// 配置
	config *config.DeviceConfig

	// 设备提供者
	providers []DeviceProvider

	// 监控
	monitor *DeviceMonitor

	// 关闭信号
	done chan struct{}
}

// Allocation 设备分配记录
type Allocation struct {
	ID          string
	DeviceID    string
	RequesterID string
	AllocatedAt time.Time
	ExpiresAt   time.Time
	SessionID   string
}

// DeviceProvider 设备提供者接口
type DeviceProvider interface {
	Initialize(ctx context.Context) error
	ListDevices(ctx context.Context) ([]Device, error)
	GetDevice(ctx context.Context, deviceID string) (Device, error)
	Cleanup(ctx context.Context) error
}

// NewManager 创建设备管理器
func NewManager(cfg config.DeviceConfig) (*Manager, error) {
	m := &Manager{
		devices:     make(map[string]Device),
		allocations: make(map[string]*Allocation),
		config:      &cfg,
		done:        make(chan struct{}),
	}

	// 初始化设备提供者
	if err := m.initializeProviders(); err != nil {
		return nil, fmt.Errorf("failed to initialize providers: %w", err)
	}

	// 启动设备发现
	if err := m.discoverDevices(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to discover devices: %w", err)
	}

	// 启动监控
	m.monitor = NewDeviceMonitor(m)
	go m.monitor.Start()

	return m, nil
}

// initializeProviders 初始化设备提供者
func (m *Manager) initializeProviders() error {
	ctx := context.Background()

	// 物理设备提供者
	if m.config.Physical.Enabled {
		provider := NewPhysicalDeviceProvider(m.config.Physical, m.config.Appium)
		if err := provider.Initialize(ctx); err != nil {
			return fmt.Errorf("physical device provider: %w", err)
		}
		m.providers = append(m.providers, provider)
	}

	// 模拟器提供者
	if m.config.Emulator.Enabled {
		provider := NewEmulatorProvider(m.config.Emulator, m.config.Appium)
		if err := provider.Initialize(ctx); err != nil {
			return fmt.Errorf("emulator provider: %w", err)
		}
		m.providers = append(m.providers, provider)
	}

	// AWS 设备提供者
	if m.config.AWS.Enabled {
		provider := NewAWSDeviceProvider(m.config.AWS)
		if err := provider.Initialize(ctx); err != nil {
			return fmt.Errorf("AWS device provider: %w", err)
		}
		m.providers = append(m.providers, provider)
	}

	return nil
}

// discoverDevices 发现所有设备
func (m *Manager) discoverDevices(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 清空现有设备
	m.devices = make(map[string]Device)

	// 从每个提供者获取设备
	for _, provider := range m.providers {
		devices, err := provider.ListDevices(ctx)
		if err != nil {
			// 记录错误但继续
			continue
		}

		for _, device := range devices {
			m.devices[device.GetID()] = device
		}
	}

	if len(m.devices) == 0 {
		return errors.New("no devices found")
	}

	return nil
}

// ListDevices 列出设备
func (m *Manager) ListDevices(ctx context.Context, filter *pb.DeviceFilter) ([]*pb.Device, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*pb.Device

	for _, device := range m.devices {
		// 应用过滤器
		if !m.matchFilter(device, filter) {
			continue
		}

		pbDevice := &pb.Device{
			Id:            device.GetID(),
			Type:          device.GetType(),
			Status:        device.GetStatus(),
			Capabilities:  device.GetCapabilities(),
			Properties:    device.GetProperties(),
			ResourceUsage: device.GetResourceUsage(),
		}

		result = append(result, pbDevice)
	}

	return result, nil
}

// matchFilter 检查设备是否匹配过滤器
func (m *Manager) matchFilter(device Device, filter *pb.DeviceFilter) bool {
	if filter == nil {
		return true
	}

	// 设备类型过滤
	if len(filter.Types) > 0 {
		typeMatch := false
		for _, t := range filter.Types {
			if device.GetType() == t {
				typeMatch = true
				break
			}
		}
		if !typeMatch {
			return false
		}
	}

	// 状态过滤
	if len(filter.Statuses) > 0 {
		statusMatch := false
		for _, s := range filter.Statuses {
			if device.GetStatus() == s {
				statusMatch = true
				break
			}
		}
		if !statusMatch {
			return false
		}
	}

	// 平台过滤
	if filter.Platform != "" {
		caps := device.GetCapabilities()
		if caps == nil || caps.Platform != filter.Platform {
			return false
		}
	}

	// 版本过滤
	if filter.MinVersion != "" || filter.MaxVersion != "" {
		caps := device.GetCapabilities()
		if caps == nil {
			return false
		}

		if filter.MinVersion != "" && caps.PlatformVersion < filter.MinVersion {
			return false
		}
		if filter.MaxVersion != "" && caps.PlatformVersion > filter.MaxVersion {
			return false
		}
	}

	return true
}

// AllocateDevice 分配设备
func (m *Manager) AllocateDevice(ctx context.Context, req *pb.AllocateDeviceRequest) (*pb.AllocateDeviceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 查找合适的设备
	var selectedDevice Device
	var maxScore float64

	for _, device := range m.devices {
		// 检查设备是否可用
		if device.GetStatus() != pb.DeviceStatus_DEVICE_STATUS_AVAILABLE {
			continue
		}

		// 检查是否已分配
		if m.isDeviceAllocated(device.GetID()) {
			continue
		}

		// 计算匹配分数
		score := m.calculateMatchScore(device, req.Requirements)
		if score > maxScore {
			maxScore = score
			selectedDevice = device
		}
	}

	if selectedDevice == nil {
		return nil, ErrNoDeviceAvailable
	}

	// 创建分配记录
	allocation := &Allocation{
		ID:          generateID(),
		DeviceID:    selectedDevice.GetID(),
		RequesterID: req.RequesterId,
		AllocatedAt: time.Now(),
		ExpiresAt:   time.Now().Add(req.LeaseDuration.AsDuration()),
	}

	m.allocations[allocation.ID] = allocation

	// 连接设备
	if err := selectedDevice.Connect(ctx); err != nil {
		delete(m.allocations, allocation.ID)
		return nil, fmt.Errorf("failed to connect device: %w", err)
	}

	return &pb.AllocateDeviceResponse{
		AllocationId: allocation.ID,
		Device: &pb.Device{
			Id:           selectedDevice.GetID(),
			Type:         selectedDevice.GetType(),
			Status:       pb.DeviceStatus_DEVICE_STATUS_BUSY,
			Capabilities: selectedDevice.GetCapabilities(),
			Properties:   selectedDevice.GetProperties(),
		},
		ExpiresAt: timestampProto(allocation.ExpiresAt),
		ConnectionInfo: map[string]string{
			"appium_url": selectedDevice.GetAppiumURL(),
		},
	}, nil
}

// calculateMatchScore 计算设备匹配分数
func (m *Manager) calculateMatchScore(device Device, requirements *pb.DeviceRequirements) float64 {
	if requirements == nil {
		return 1.0
	}

	score := 0.0
	factors := 0.0

	// 设备类型匹配
	if requirements.PreferredType != pb.DeviceType_DEVICE_TYPE_UNSPECIFIED {
		factors += 1.0
		if device.GetType() == requirements.PreferredType {
			score += 1.0
		}
	}

	// 平台匹配
	if requirements.Platform != "" {
		factors += 2.0 // 平台更重要
		caps := device.GetCapabilities()
		if caps != nil && caps.Platform == requirements.Platform {
			score += 2.0
		}
	}

	// 版本匹配
	if requirements.MinVersion != "" {
		factors += 1.0
		caps := device.GetCapabilities()
		if caps != nil && caps.PlatformVersion >= requirements.MinVersion {
			score += 1.0
		}
	}

	// 功能匹配
	if len(requirements.RequiredFeatures) > 0 {
		factors += 1.0
		caps := device.GetCapabilities()
		if caps != nil {
			matchedFeatures := 0
			for _, feature := range requirements.RequiredFeatures {
				if caps.Features[feature] {
					matchedFeatures++
				}
			}
			score += float64(matchedFeatures) / float64(len(requirements.RequiredFeatures))
		}
	}

	if factors == 0 {
		return 1.0
	}

	return score / factors
}

// ReleaseDevice 释放设备
func (m *Manager) ReleaseDevice(ctx context.Context, allocationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	allocation, ok := m.allocations[allocationID]
	if !ok {
		return errors.New("allocation not found")
	}

	device, ok := m.devices[allocation.DeviceID]
	if ok {
		// 断开设备连接
		_ = device.Disconnect(ctx)
	}

	delete(m.allocations, allocationID)

	return nil
}

// GetDevice 获取设备
func (m *Manager) GetDevice(deviceID string) (Device, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	device, ok := m.devices[deviceID]
	if !ok {
		return nil, ErrDeviceNotFound
	}

	return device, nil
}

// isDeviceAllocated 检查设备是否已分配
func (m *Manager) isDeviceAllocated(deviceID string) bool {
	for _, allocation := range m.allocations {
		if allocation.DeviceID == deviceID && time.Now().Before(allocation.ExpiresAt) {
			return true
		}
	}
	return false
}

// Cleanup 清理资源
func (m *Manager) Cleanup() {
	close(m.done)

	ctx := context.Background()

	// 断开所有设备
	for _, device := range m.devices {
		_ = device.Disconnect(ctx)
	}

	// 清理提供者
	for _, provider := range m.providers {
		_ = provider.Cleanup(ctx)
	}
}

// Helper functions
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func timestampProto(t time.Time) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}
