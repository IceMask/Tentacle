package device

import (
	"context"
	"log"
	"time"
)

// DeviceMonitor 设备监控器
type DeviceMonitor struct {
	manager *Manager
	ticker  *time.Ticker
	done    chan struct{}
}

// NewDeviceMonitor 创建设备监控器
func NewDeviceMonitor(manager *Manager) *DeviceMonitor {
	return &DeviceMonitor{
		manager: manager,
		done:    make(chan struct{}),
	}
}

// Start 启动监控
func (m *DeviceMonitor) Start() {
	m.ticker = time.NewTicker(30 * time.Second)
	defer m.ticker.Stop()

	for {
		select {
		case <-m.ticker.C:
			m.checkDevices()
			m.cleanupExpiredAllocations()
		case <-m.done:
			return
		}
	}
}

// checkDevices 检查设备状态
func (m *DeviceMonitor) checkDevices() {
	ctx := context.Background()

	m.manager.mu.RLock()
	devices := make([]Device, 0, len(m.manager.devices))
	for _, device := range m.manager.devices {
		devices = append(devices, device)
	}
	m.manager.mu.RUnlock()

	for _, device := range devices {
		if err := device.HealthCheck(ctx); err != nil {
			log.Printf("Device %s health check failed: %v", device.GetID(), err)
			// TODO: 更新设备状态
		}
	}
}

// cleanupExpiredAllocations 清理过期的分配
func (m *DeviceMonitor) cleanupExpiredAllocations() {
	m.manager.mu.Lock()
	defer m.manager.mu.Unlock()

	now := time.Now()
	for id, allocation := range m.manager.allocations {
		if now.After(allocation.ExpiresAt) {
			log.Printf("Cleaning up expired allocation: %s", id)
			delete(m.manager.allocations, id)

			// 断开设备连接
			if device, ok := m.manager.devices[allocation.DeviceID]; ok {
				_ = device.Disconnect(context.Background())
			}
		}
	}
}

// Stop 停止监控
func (m *DeviceMonitor) Stop() {
	close(m.done)
}
