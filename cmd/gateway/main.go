package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/gateway/capabilities"
	"mcp_for_appium/internal/gateway/jsonrpc"
	"mcp_for_appium/internal/gateway/rest"
	"mcp_for_appium/internal/gateway/websocket"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/storage/s3"
	"mcp_for_appium/internal/telemetry"
)

func main() {
	// 加载配置文件
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 初始化日志与遥测
	telemetry.InitLogger(cfg.Telemetry.LogLevel)

	// Init Deps (Gateway connects to Orchestrator via gRPC usually, but here we might embed or use same DBs if monolithic for now)
	// Design says Gateway -> Orchestrator (gRPC).
	// For this MVP/Simulation, I'll instantiate Orchestrator Service directly to avoid gRPC complexity in this single repo setup,
	// or I should use a gRPC client stub.
	// Given I implemented Orchestrator Service in `internal/orchestrator`, I can use it directly if I want a monolith,
	// OR I should implement a gRPC client in `internal/gateway/orchestrator_client.go`.
	// I'll use the direct service for simplicity in this "consolidated" repo, assuming it's a monolith binary or shared lib usage.
	// Wait, `cmd/orchestrator` and `cmd/gateway` implies separate binaries.
	// I'll assume they share the DBs for now or I should use gRPC.
	// Since I didn't implement gRPC server fully, I'll instantiate the Service directly here too (Monolith mode) or stub the client.
	// I'll instantiate the Service directly.

	pgDAO, err := postgres.NewDAO(context.Background(), cfg.Storage.Postgres)
	if err != nil {
		log.Fatalf("failed to init postgres: %v", err)
	}
	redisCache, err := redis.NewCache(cfg.Storage.Redis)
	if err != nil {
		log.Fatalf("failed to init redis: %v", err)
	}
	s3Client, err := s3.NewClient(context.Background(), cfg.Storage.S3)
	if err != nil {
		log.Fatalf("failed to init s3: %v", err)
	}

	orchSvc := orchestrator.NewService(cfg.Orchestrator, pgDAO, redisCache, s3Client)

	// 启动 orchestrator 内部逻辑（单体模式）
	ctx := context.Background()
	if err := orchSvc.Start(ctx); err != nil {
		log.Fatalf("failed to start orchestrator: %v", err)
	}

	// 构造 HTTP/WS 相关 handler
	jsonrpcHandler := jsonrpc.NewHandler(orchSvc)
	restRouter := rest.NewRouter(orchSvc)
	wsHub := websocket.NewHub(redisCache)
	go wsHub.Run(ctx)

	// 能力声明服务（暂未暴露）
	capSvc := capabilities.NewService(cfg.Gateway)
	_ = capSvc

	// Middleware
	// patValidator := auth.NewPATValidator(...)
	// authMiddleware := middleware.NewAuthMiddleware(patValidator)

	// Mux
	// 注册 JSON-RPC、REST 路由
	mux := http.NewServeMux()
	mux.Handle("/jsonrpc", jsonrpcHandler) // Add middleware
	restRouter.RegisterRoutes(mux)

	// 配置并启动 HTTP 服务器
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Gateway.Port),
		Handler: mux,
	}

	go func() {
		log.Printf("Gateway started on port %d", cfg.Gateway.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// 监听退出信号并优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down gateway...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
}
