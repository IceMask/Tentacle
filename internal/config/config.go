package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 主配置结构
type Config struct {
	Server       ServerConfig  `yaml:"server"`
	DeviceConfig DeviceConfig  `yaml:"devices"`
	Session      SessionConfig `yaml:"session"`
	Test         TestConfig    `yaml:"test"`
	TLS          TLSConfig     `yaml:"tls"`
	Logging      LoggingConfig `yaml:"logging"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port            int           `yaml:"port"`
	MaxConnections  int           `yaml:"max_connections"`
	RequestTimeout  time.Duration `yaml:"request_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// DeviceConfig 设备配置
type DeviceConfig struct {
	Physical PhysicalDeviceConfig `yaml:"physical"`
	Emulator EmulatorConfig       `yaml:"emulator"`
	AWS      AWSConfig            `yaml:"aws"`
	Appium   AppiumConfig         `yaml:"appium"`
}

// PhysicalDeviceConfig 物理设备配置
type PhysicalDeviceConfig struct {
	Enabled bool             `yaml:"enabled"`
	Devices []PhysicalDevice `yaml:"devices"`
}

type PhysicalDevice struct {
	ID           string            `yaml:"id"`
	Name         string            `yaml:"name"`
	UDID         string            `yaml:"udid"`
	Platform     string            `yaml:"platform"`
	Version      string            `yaml:"version"`
	AppiumURL    string            `yaml:"appium_url"`
	Capabilities map[string]string `yaml:"capabilities"`
}

// EmulatorConfig 模拟器配置
type EmulatorConfig struct {
	Enabled      bool               `yaml:"enabled"`
	DockerHost   string             `yaml:"docker_host"`
	MaxInstances int                `yaml:"max_instances"`
	Templates    []EmulatorTemplate `yaml:"templates"`
}

type EmulatorTemplate struct {
	Name        string         `yaml:"name"`
	Image       string         `yaml:"image"`
	Platform    string         `yaml:"platform"`
	Version     string         `yaml:"version"`
	DeviceModel string         `yaml:"device_model"`
	Resources   ResourceLimits `yaml:"resources"`
}

type ResourceLimits struct {
	CPUCores  float64 `yaml:"cpu_cores"`
	MemoryMB  int     `yaml:"memory_mb"`
	StorageGB int     `yaml:"storage_gb"`
}

// AWSConfig AWS Device Farm 配置
type AWSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Region     string `yaml:"region"`
	ProjectARN string `yaml:"project_arn"`
	AccessKey  string `yaml:"access_key"`
	SecretKey  string `yaml:"secret_key"`
}

// AppiumConfig Appium 配置
type AppiumConfig struct {
	DefaultURL          string        `yaml:"default_url"`
	CommandTimeout      time.Duration `yaml:"command_timeout"`
	ImplicitWaitTimeout time.Duration `yaml:"implicit_wait_timeout"`
	RetryAttempts       int           `yaml:"retry_attempts"`
}

// SessionConfig 会话配置
type SessionConfig struct {
	MaxDuration     time.Duration `yaml:"max_duration"`
	IdleTimeout     time.Duration `yaml:"idle_timeout"`
	MaxConcurrent   int           `yaml:"max_concurrent"`
	EnableRecording bool          `yaml:"enable_recording"`
}

// TestConfig 测试配置
type TestConfig struct {
	DefaultTimeout   time.Duration `yaml:"default_timeout"`
	ScreenshotOnFail bool          `yaml:"screenshot_on_fail"`
	VideoRecording   bool          `yaml:"video_recording"`
	MaxRetries       int           `yaml:"max_retries"`
}

// TLSConfig TLS 配置
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// LoggingConfig 日志配置
type LoggingConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	OutputPath string `yaml:"output_path"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAgeDays int    `yaml:"max_age_days"`
}

// Load 加载配置文件
func Load(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// 设置默认值
	cfg.setDefaults()

	return &cfg, nil
}

// setDefaults 设置默认配置值
func (c *Config) setDefaults() {
	if c.Server.Port == 0 {
		c.Server.Port = 50051
	}
	if c.Server.RequestTimeout == 0 {
		c.Server.RequestTimeout = 30 * time.Second
	}
	if c.Server.ShutdownTimeout == 0 {
		c.Server.ShutdownTimeout = 30 * time.Second
	}
	if c.Session.MaxDuration == 0 {
		c.Session.MaxDuration = 2 * time.Hour
	}
	if c.Session.IdleTimeout == 0 {
		c.Session.IdleTimeout = 30 * time.Minute
	}
	if c.Test.DefaultTimeout == 0 {
		c.Test.DefaultTimeout = 60 * time.Second
	}
	if c.DeviceConfig.Appium.CommandTimeout == 0 {
		c.DeviceConfig.Appium.CommandTimeout = 30 * time.Second
	}
	if c.DeviceConfig.Appium.RetryAttempts == 0 {
		c.DeviceConfig.Appium.RetryAttempts = 3
	}
}
