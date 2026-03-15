package config

import (
	"time"
)

type Config struct {
	Gateway      GatewayConfig      `yaml:"gateway"`
	Orchestrator OrchestratorConfig `yaml:"orchestrator"`
	Worker       WorkerConfig       `yaml:"worker"`
	Storage      StorageConfig      `yaml:"storage"`
	AWS          AWSConfig          `yaml:"aws"`
	DeviceFarm   DeviceFarmConfig   `yaml:"devicefarm"`
	Auth         AuthConfig         `yaml:"auth"`
	Telemetry    TelemetryConfig    `yaml:"telemetry"`
}

type AWSConfig struct {
	Region  string `yaml:"region" env:"AWS__REGION" default:"us-west-2"`
	Profile string `yaml:"profile" env:"AWS__PROFILE"`
}

type DeviceFarmConfig struct {
	Mode               string `yaml:"mode" env:"DEVICEFARM__MODE" default:"disabled"` // disabled|test_grid|run_api
	ProjectARN         string `yaml:"project_arn" env:"DEVICEFARM__PROJECT_ARN"`
	TestGridProjectARN string `yaml:"test_grid_project_arn" env:"DEVICEFARM__TEST_GRID_PROJECT_ARN"`
	URLExpiresSeconds  int    `yaml:"url_expires_seconds" env:"DEVICEFARM__URL_EXPIRES_SECONDS" default:"600"`
}

type GatewayConfig struct {
	Port            int           `yaml:"port" env:"GATEWAY__PORT" default:"8080"`
	EnableTLS       bool          `yaml:"enable_tls" env:"GATEWAY__ENABLE_TLS" default:"false"`
	TLSCertFile     string        `yaml:"tls_cert_file" env:"GATEWAY__TLS_CERT_FILE"`
	TLSKeyFile      string        `yaml:"tls_key_file" env:"GATEWAY__TLS_KEY_FILE"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" env:"GATEWAY__SHUTDOWN_TIMEOUT" default:"30s"`
}

type OrchestratorConfig struct {
	GRPCPort      int           `yaml:"grpc_port" env:"ORCHESTRATOR__GRPC_PORT" default:"9090"`
	PlanTimeout   time.Duration `yaml:"plan_timeout" env:"ORCHESTRATOR__PLAN_TIMEOUT" default:"10m"`
	StepTimeout   time.Duration `yaml:"step_timeout" env:"ORCHESTRATOR__STEP_TIMEOUT" default:"30s"`
	AutoWaitMax   time.Duration `yaml:"auto_wait_max" env:"ORCHESTRATOR__AUTO_WAIT_MAX" default:"5s"`
	SnapshotTTL   time.Duration `yaml:"snapshot_ttl" env:"ORCHESTRATOR__SNAPSHOT_TTL" default:"1.5s"`
	ExecutionMode string        `yaml:"execution_mode" env:"ORCHESTRATOR__EXECUTION_MODE" default:"monolith"`
}

type WorkerConfig struct {
	AppiumURL         string            `yaml:"appium_url" env:"WORKER__APPIUM_URL" default:"http://localhost:4723"`
	Concurrency       int               `yaml:"concurrency" env:"WORKER__CONCURRENCY" default:"0"` // 0 means auto-detect
	HeartbeatInterval time.Duration     `yaml:"heartbeat_interval" env:"WORKER__HEARTBEAT_INTERVAL" default:"10s"`
	Tags              map[string]string `yaml:"tags"`
	OrchestratorAddr  string            `yaml:"orchestrator_addr" env:"WORKER__ORCHESTRATOR_ADDR" default:"localhost:9090"`
	GRPCPort          int               `yaml:"grpc_port" env:"WORKER__GRPC_PORT" default:"9092"`
}

type StorageConfig struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Redis    RedisConfig    `yaml:"redis"`
	S3       S3Config       `yaml:"s3"`
}

type PostgresConfig struct {
	DSN             string        `yaml:"dsn" env:"STORAGE__POSTGRES__DSN"`
	MaxOpenConns    int           `yaml:"max_open_conns" env:"STORAGE__POSTGRES__MAX_OPEN_CONNS" default:"25"`
	MaxIdleConns    int           `yaml:"max_idle_conns" env:"STORAGE__POSTGRES__MAX_IDLE_CONNS" default:"5"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime" env:"STORAGE__POSTGRES__CONN_MAX_LIFETIME" default:"5m"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr" env:"STORAGE__REDIS__ADDR" default:"localhost:6379"`
	Password string `yaml:"password" env:"STORAGE__REDIS__PASSWORD"`
	DB       int    `yaml:"db" env:"STORAGE__REDIS__DB" default:"0"`
}

type S3Config struct {
	Region          string `yaml:"region" env:"STORAGE__S3__REGION" default:"us-east-1"`
	Bucket          string `yaml:"bucket" env:"STORAGE__S3__BUCKET"`
	AccessKeyID     string `yaml:"access_key_id" env:"STORAGE__S3__ACCESS_KEY_ID"`
	SecretAccessKey string `yaml:"secret_access_key" env:"STORAGE__S3__SECRET_ACCESS_KEY"`
	Endpoint        string `yaml:"endpoint" env:"STORAGE__S3__ENDPOINT"` // For MinIO etc.
	ForcePathStyle  bool   `yaml:"force_path_style" env:"STORAGE__S3__FORCE_PATH_STYLE" default:"false"`
}

type AuthConfig struct {
	EnablePAT  bool   `yaml:"enable_pat" env:"AUTH__ENABLE_PAT" default:"true"`
	EnableOIDC bool   `yaml:"enable_oidc" env:"AUTH__ENABLE_OIDC" default:"false"`
	JWKSURL    string `yaml:"jwks_url" env:"AUTH__JWKS_URL"`
	Audience   string `yaml:"audience" env:"AUTH__AUDIENCE"`
}

type TelemetryConfig struct {
	LogLevel     string `yaml:"log_level" env:"TELEMETRY__LOG_LEVEL" default:"info"` // Control minimum log verbosity emitted by the process.
	LogFile      string `yaml:"log_file" env:"TELEMETRY__LOG_FILE"`                   // Optional absolute or relative file path for persistent JSON logs.
	OTLPEndpoint string `yaml:"otlp_endpoint" env:"TELEMETRY__OTLP_ENDPOINT"`         // Optional OTLP collector endpoint for distributed tracing export.
	MetricsPort  int    `yaml:"metrics_port" env:"TELEMETRY__METRICS_PORT" default:"9091"` // Prometheus metrics HTTP port.
}
