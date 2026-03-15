package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/orchestrator"
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

	// 2. Init Telemetry
	// 初始化日志（Tracing 待接入）
	telemetry.InitLogger(cfg.Telemetry.LogLevel, cfg.Telemetry.LogFile) // Initialize structured logging with optional file sink.
	log.SetOutput(telemetry.StdLogWriter())                              // Route standard log package output to configured sinks.
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
	if cfg.Orchestrator.ExecutionMode != orchestrator.ExecutionModeDistributed {
		log.Printf("forcing execution mode to %q for standalone orchestrator", orchestrator.ExecutionModeDistributed)
		cfg.Orchestrator.ExecutionMode = orchestrator.ExecutionModeDistributed
	}
	svc := orchestrator.NewService(cfg.Orchestrator, cfg.Worker, cfg.AWS, cfg.DeviceFarm, pgDAO, redisCache, s3Client)

	// 5. Start Service (dispatcher and registry monitor)
	// 启动后台循环
	if err := svc.Start(context.Background()); err != nil {
		log.Fatalf("failed to start service: %v", err)
	}

	// 6. Start gRPC Server for worker registration / heartbeat
	grpcSrv := orchestrator.NewGRPCServer(svc.Registry())
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
