package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mcp_for_appium/internal/auth"
	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/gateway/capabilities"
	"mcp_for_appium/internal/gateway/jsonrpc"
	"mcp_for_appium/internal/gateway/rest"
	"mcp_for_appium/internal/gateway/stdio"
	"mcp_for_appium/internal/gateway/websocket"
	"mcp_for_appium/internal/orchestrator"
	"mcp_for_appium/internal/startup"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/storage/s3"
	"mcp_for_appium/internal/telemetry"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// main is the entry point for this binary.
func main() {
	// Parse command line flags
	stdioMode := flag.Bool("stdio", false, "Run in stdio mode for MCP protocol")
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	// 加载配置文件
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	if err := startup.ValidateGatewayStartup(cfg, *stdioMode); err != nil { // Reject malformed startup config before telemetry or dependency initialization begins.
		log.Fatalf("gateway startup preflight failed: %v", err) // Stop immediately so no listener starts with an invalid runtime configuration.
	}

	// 初始化日志与遥测
	telemetry.InitLogger(cfg.Telemetry.LogLevel, cfg.Telemetry.LogFile) // Initialize structured logging with optional file sink.
	log.SetOutput(telemetry.StdLogWriter())                             // Mirror standard-library logs into the configured log destinations.
	shutdownTracer := telemetry.InitTracer("gateway", cfg.Telemetry.OTLPEndpoint)
	defer func() {
		_ = shutdownTracer(context.Background())
	}()

	// If stdio mode, run stdio transport and exit
	if *stdioMode {
		runStdioMode(cfg)
		return
	}

	// Otherwise, run HTTP server mode
	runHTTPMode(cfg)
}

// runStdioMode runs the gateway in stdio mode for MCP protocol
func runStdioMode(cfg *config.Config) {
	// Set log output to stderr (stdout is reserved for JSON-RPC)
	log.SetOutput(telemetry.StdLogWriter()) // Keep stdio transport diagnostics in the shared configured log writer.
	log.Println("Starting MCP gateway in stdio mode...")

	// Initialize dependencies (minimal setup for stdio mode)
	ctx := context.Background()
	if cfg.Orchestrator.ExecutionMode == orchestrator.ExecutionModeDistributed {
		log.Fatalf("stdio mode currently supports only %q execution mode", orchestrator.ExecutionModeMonolith)
	}

	pgDAO, err := postgres.NewDAO(ctx, cfg.Storage.Postgres)
	if err != nil {
		log.Fatalf("failed to init postgres: %v", err)
	}
	redisCache, err := redis.NewCache(cfg.Storage.Redis)
	if err != nil {
		log.Fatalf("failed to init redis: %v", err)
	}
	s3Client, err := s3.NewClient(ctx, cfg.Storage.S3)
	if err != nil {
		log.Fatalf("failed to init s3: %v", err)
	}
	preflightCtx, preflightCancel := context.WithTimeout(ctx, 5*time.Second)    // Bound dependency probes so startup fails fast instead of hanging indefinitely on unavailable services.
	defer preflightCancel()                                                     // Release the dependency-check timeout resources after startup validation finishes.
	if err := startup.CheckS3BucketAccess(preflightCtx, s3Client); err != nil { // Verify the artifact bucket exists and is reachable before accepting stdio traffic.
		log.Fatalf("stdio startup preflight failed for s3: %v", err) // Stop immediately so later artifact writes do not fail on the first real request.
	}
	if err := startup.CheckAppiumReachability(preflightCtx, cfg.Worker.AppiumURL); err != nil { // Verify the embedded Appium dependency is reachable before the monolith orchestrator starts.
		log.Fatalf("stdio startup preflight failed for appium: %v", err) // Stop immediately so session creation does not fail only after the first user request.
	}

	orchSvc := orchestrator.NewService(cfg.Orchestrator, cfg.Worker, cfg.AWS, cfg.DeviceFarm, pgDAO, redisCache, s3Client)
	if err := orchSvc.Start(ctx); err != nil {
		log.Fatalf("failed to start orchestrator: %v", err)
	}

	// Create and run stdio transport
	capSvc := capabilities.NewService(cfg.Gateway)
	transport := stdio.NewTransport(orchSvc, capSvc)

	// Handle shutdown signals
	sigCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Run the transport (blocks until shutdown)
	if err := transport.Run(sigCtx); err != nil && err != context.Canceled {
		log.Fatalf("stdio transport error: %v", err)
	}

	log.Println("MCP gateway stdio mode shut down gracefully")
}

// runHTTPMode runs the gateway in HTTP server mode
func runHTTPMode(cfg *config.Config) {

	// Monolith mode: gateway embeds the orchestrator Service directly.
	// Distributed mode (gateway -> external orchestrator) is not wired yet.
	if cfg.Orchestrator.ExecutionMode == orchestrator.ExecutionModeDistributed {
		log.Fatalf("gateway HTTP mode currently supports only %q execution mode", orchestrator.ExecutionModeMonolith)
	}

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
	preflightCtx, preflightCancel := context.WithTimeout(context.Background(), 5*time.Second) // Bound dependency probes so HTTP startup fails fast when a required dependency is unavailable.
	defer preflightCancel()                                                                   // Release the dependency-check timeout resources after startup validation finishes.
	if err := startup.CheckS3BucketAccess(preflightCtx, s3Client); err != nil {               // Verify the artifact bucket exists and is reachable before the HTTP server starts.
		log.Fatalf("http startup preflight failed for s3: %v", err) // Stop immediately so artifact operations do not fail only after requests arrive.
	}
	if err := startup.CheckAppiumReachability(preflightCtx, cfg.Worker.AppiumURL); err != nil { // Verify the embedded Appium dependency is reachable before the monolith orchestrator starts.
		log.Fatalf("http startup preflight failed for appium: %v", err) // Stop immediately so session creation does not fail only after requests arrive.
	}

	orchSvc := orchestrator.NewService(cfg.Orchestrator, cfg.Worker, cfg.AWS, cfg.DeviceFarm, pgDAO, redisCache, s3Client)

	// 启动 orchestrator 内部逻辑（单体模式）
	ctx := context.Background()
	if err := orchSvc.Start(ctx); err != nil {
		log.Fatalf("failed to start orchestrator: %v", err)
	}

	// 能力声明服务
	capSvc := capabilities.NewService(cfg.Gateway)

	// 构造 HTTP/WS 相关 handler
	jsonrpcHandler := jsonrpc.NewHandler(orchSvc, capSvc)
	restRouter := rest.NewRouter(orchSvc, redisCache, capSvc)
	if secret := os.Getenv("HMAC_SECRET"); secret != "" {
		keyID := os.Getenv("HMAC_KEY_ID")
		validator := auth.NewHMACValidator(func(id string) (string, error) {
			if keyID == "" || id == keyID {
				return secret, nil
			}
			return "", errors.New("unknown hmac key id")
		}, redisCache)
		restRouter.SetHMACValidator(validator)
	}
	wsHub := websocket.NewHub(redisCache)
	go wsHub.Run(ctx)

	// Middleware
	// patValidator := auth.NewPATValidator(...)
	// authMiddleware := middleware.NewAuthMiddleware(patValidator)

	// Mux
	// 注册 JSON-RPC、REST 路由
	mux := http.NewServeMux()
	mux.Handle("/jsonrpc", jsonrpcHandler) // Add middleware
	restRouter.RegisterRoutes(mux)
	mux.Handle("/ws/plan-events", websocket.NewHandler(wsHub))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, orchSvc.HealthCheck(r.Context()))
	})
	mux.Handle("/metrics", promhttp.Handler())

	// 配置并启动 HTTP 服务器
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Gateway.Port),
		Handler: telemetry.TraceMiddleware("gateway", mux),
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

// writeJSON executes this operation.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
