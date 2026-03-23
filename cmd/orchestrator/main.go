package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/startup"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/storage/s3"
	"mcp_for_appium/internal/telemetry"
)

// main is the entry point for this binary.
func main() {
	// 加载配置
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	effectiveMode, forced := normalizeStandaloneExecutionMode(cfg.Orchestrator.ExecutionMode) // Normalize the standalone orchestrator execution mode so startup always runs with the supported distributed setting.
	if forced {                                                                               // Emit the compatibility log only when startup overrides a non-distributed configured mode.
		log.Printf("forcing execution mode to %q for standalone orchestrator", orchestrator.ExecutionModeDistributed) // Surface the effective runtime mode so operators can see the normalization.
	}
	cfg.Orchestrator.ExecutionMode = effectiveMode                   // Apply the effective mode before startup preflight checks inspect the config.
	if err := startup.ValidateOrchestratorStartup(cfg); err != nil { // Reject malformed startup config before telemetry or dependency initialization begins.
		log.Fatalf("orchestrator startup preflight failed: %v", err) // Stop immediately so no background loops or listeners start with invalid config.
	}

	// 2. Init Telemetry
	// 初始化日志（Tracing 待接入）
	telemetry.InitLogger(cfg.Telemetry.LogLevel, cfg.Telemetry.LogFile) // Initialize structured logging with optional file sink.
	log.SetOutput(telemetry.StdLogWriter())                             // Route standard log package output to configured sinks.
	shutdownTracer := telemetry.InitTracer("orchestrator", cfg.Telemetry.OTLPEndpoint)
	defer func() {
		_ = shutdownTracer(context.Background())
	}()

	// 3. Init Storage
	// 初始化 Postgres/Redis/S3 依赖
	pgDAO, err := postgres.NewDAO(context.Background(), cfg.Storage.Postgres)
	if err != nil {
		log.Fatalf("failed to init postgres: %v", err)
	}
	defer pgDAO.Close()

	redisCache, err := redis.NewCache(cfg.Storage.Redis)
	if err != nil {
		log.Fatalf("failed to init redis: %v", err)
	}
	defer redisCache.Close()

	s3Client, err := s3.NewClient(context.Background(), cfg.Storage.S3)
	if err != nil {
		log.Fatalf("failed to init s3: %v", err)
	}

	// 4. Init Service
	// 构造 orchestrator 服务，内部包含 dispatcher/registry
	svc := orchestrator.NewService(cfg.Orchestrator, cfg.RPC.Security, cfg.Worker, cfg.AWS, cfg.DeviceFarm, pgDAO, redisCache, s3Client)
	preflightCtx, preflightCancel := context.WithTimeout(context.Background(), 5*time.Second) // Bound dependency probes so standalone startup fails fast when a required dependency is unavailable.
	defer preflightCancel()                                                                   // Release the dependency-check timeout resources after startup validation finishes.
	if err := startup.CheckS3BucketAccess(preflightCtx, s3Client); err != nil {               // Verify the artifact bucket exists and is reachable before the orchestrator starts background loops.
		log.Fatalf("orchestrator startup preflight failed for s3: %v", err) // Stop immediately so later artifact operations do not fail only after runtime traffic begins.
	}

	// 5. Start Service (dispatcher and registry monitor)
	// 启动后台循环
	if err := svc.Start(context.Background()); err != nil {
		log.Fatalf("failed to start service: %v", err)
	}

	// 6. Start gRPC Server for worker registration / heartbeat
	grpcSrv, err := orchestrator.NewGRPCServer(svc, cfg.RPC.Security) // Build the worker-facing gRPC server with the configured internal RPC transport mode and shared-token enforcement.
	if err != nil {                                                   // Stop immediately when the configured internal RPC security settings cannot be turned into a gRPC server safely.
		log.Fatalf("failed to initialize orchestrator gRPC security: %v", err) // Surface the gRPC security wiring failure before the listener starts.
	}
	go func() {
		if err := grpcSrv.Start(cfg.Orchestrator.GRPCPort); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()
	log.Printf("Orchestrator started on port %d", cfg.Orchestrator.GRPCPort)

	// 7. Graceful Shutdown
	// 监听退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down orchestrator...")

	// Cleanup logic
	// 停止后台循环并清理资源
	grpcSrv.Stop()
	svc.Stop()
	defer pgDAO.Close()
	defer redisCache.Close()
}

// normalizeStandaloneExecutionMode returns the effective standalone orchestrator execution mode together with whether startup had to override the configured value.
func normalizeStandaloneExecutionMode(configuredMode string) (string, bool) {
	if configuredMode == orchestrator.ExecutionModeDistributed { // Preserve the configured value when it already matches the only supported standalone orchestrator mode.
		return configuredMode, false // Report that no override was needed because startup can proceed with the caller-supplied distributed mode.
	}

	return orchestrator.ExecutionModeDistributed, true // Force the standalone binary into distributed mode because that is the only supported runtime posture for this process today.
}
