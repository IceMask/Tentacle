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

func main() {
	// 1. Load Config
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 2. Init Telemetry
	telemetry.InitLogger(cfg.Telemetry.LogLevel)
	// telemetry.InitTracer(...)

	// 3. Init Storage
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
	svc := orchestrator.NewService(cfg.Orchestrator, pgDAO, redisCache, s3Client)

	// 5. Start Service (dispatcher and registry monitor)
	if err := svc.Start(context.Background()); err != nil {
		log.Fatalf("failed to start service: %v", err)
	}

	// 6. Start Server (gRPC)
	// TODO: Implement gRPC server in internal/orchestrator/grpc_server.go and start it here.
	// For now, just block.
	log.Printf("Orchestrator started on port %d", cfg.Orchestrator.GRPCPort)

	// 7. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down orchestrator...")

	// Cleanup logic
	svc.Stop()
	defer pgDAO.Close()
	defer redisCache.Close()
}
