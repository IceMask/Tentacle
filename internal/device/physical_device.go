package device

import (
	"context"

	"mcp_for_appium/internal/config"
	pb "mcp_for_appium/proto/v1"
)

// PhysicalDevice 物理设备实现
type PhysicalDevice struct {
	id           string
	config       config.PhysicalDevice
	appiumConfig config.AppiumConfig
	capabilities *pb.DeviceCapabilities
	status       pb.DeviceStatus
}

// NewPhysicalDevice 创建物理设备
func NewPhysicalDevice(cfg config.PhysicalDevice, appiumCfg config.AppiumConfig) *PhysicalDevice {
	// 解析设备能力
	capabilities := &pb.DeviceCapabilities{
		Platform:        cfg.Platform,
		PlatformVersion: cfg.Version,
		Model:           cfg.Name,
		Manufacturer:    "Unknown", // 可以从配置中获取
		Features:        make(map[string]bool),
	}

	// 设置功能特性
	if cfg.Platform == "android" {
		capabilities.Features["touch"] = true
		capabilities.Features["multitouch"] = true
		capabilities.Features["accelerometer"] = true
	}

	return &PhysicalDevice{
		id:           cfg.ID,
		config:       cfg,
		appiumConfig: appiumCfg,
		status:       pb.DeviceStatus_DEVICE_STATUS_AVAILABLE,
		capabilities: capabilities,
	}
}

// GetID 获取设备ID
func (d *PhysicalDevice) GetID() string {
	return d.id
}

// GetType 获取设备类型
func (d *PhysicalDevice) GetType() pb.DeviceType {
	return pb.DeviceType_DEVICE_TYPE_PHYSICAL
}

// GetStatus 获取设备状态
func (d *PhysicalDevice) GetStatus() pb.DeviceStatus {
	return d.status
}

// GetCapabilities 获取设备能力
func (d *PhysicalDevice) GetCapabilities() *pb.DeviceCapabilities {
	return d.capabilities
}

// GetProperties 获取设备属性
func (d *PhysicalDevice) GetProperties() map[string]string {
	props := map[string]string{
		"udid":       d.config.UDID,
		"platform":   d.config.Platform,
		"version":    d.config.Version,
		"name":       d.config.Name,
		"appium_url": d.GetAppiumURL(),
	}

	// 添加配置中的其他属性
	for k, v := range d.config.Capabilities {
		props["cap_"+k] = v
	}

	return props
}

// Connect 连接设备 - 现在只是标记状态，实际连接由 SessionService 处理
func (d *PhysicalDevice) Connect(ctx context.Context) error {
	// 这里可以做一些预检查，比如检查 Appium 服务是否可用
	// 但实际的 Appium 会话创建由 SessionService 负责

	d.status = pb.DeviceStatus_DEVICE_STATUS_BUSY
	return nil
}

// Disconnect 断开设备连接
func (d *PhysicalDevice) Disconnect(ctx context.Context) error {
	d.status = pb.DeviceStatus_DEVICE_STATUS_AVAILABLE
	return nil
}

// IsConnected 检查是否已连接
func (d *PhysicalDevice) IsConnected() bool {
	return d.status == pb.DeviceStatus_DEVICE_STATUS_BUSY
}

// HealthCheck 健康检查
func (d *PhysicalDevice) HealthCheck(ctx context.Context) error {
	// 可以检查设备是否在线
	// 例如：通过 adb devices 检查 Android 设备
	// 或者简单地 ping Appium 服务器

	// TODO: 实现实际的健康检查
	return nil
}

// GetResourceUsage 获取资源使用情况
func (d *PhysicalDevice) GetResourceUsage() *pb.ResourceUsage {
	// TODO: 可以通过 adb 或其他方式获取实际资源使用情况
	return &pb.ResourceUsage{
		CpuPercent:         10.0,
		MemoryUsedBytes:    1024 * 1024 * 1024,     // 1GB
		MemoryTotalBytes:   4 * 1024 * 1024 * 1024, // 4GB
		BatteryPercent:     85.0,
		TemperatureCelsius: 25.0,
	}
}

// GetAppiumURL 获取 Appium URL
func (d *PhysicalDevice) GetAppiumURL() string {
	if d.config.AppiumURL != "" {
		return d.config.AppiumURL
	}
	return d.appiumConfig.DefaultURL
}

// GetDesiredCapabilities 获取 Appium desired capabilities
func (d *PhysicalDevice) GetDesiredCapabilities() map[string]interface{} {
	caps := make(map[string]interface{})

	// 基本能力
	caps["platformName"] = d.config.Platform
	caps["platformVersion"] = d.config.Version
	caps["deviceName"] = d.config.Name

	// Android 特定
	if d.config.Platform == "android" {
		caps["automationName"] = "UiAutomator2"
		caps["udid"] = d.config.UDID
	}

	// iOS 特定
	if d.config.Platform == "ios" {
		caps["automationName"] = "XCUITest"
		caps["udid"] = d.config.UDID
	}

	// 合并配置的能力
	for k, v := range d.config.Capabilities {
		caps[k] = v
	}

	return caps
}
