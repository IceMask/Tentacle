package main

import (
	"context"
	"encoding/json"
	"flag"
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

	orchSvc := orchestrator.NewService(cfg.Orchestrator, cfg.RPC.Security, cfg.Worker, cfg.AWS, cfg.DeviceFarm, pgDAO, redisCache, s3Client)
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

	orchSvc := orchestrator.NewService(cfg.Orchestrator, cfg.RPC.Security, cfg.Worker, cfg.AWS, cfg.DeviceFarm, pgDAO, redisCache, s3Client)

	// 启动 orchestrator 内部逻辑（单体模式）
	ctx := context.Background()
	if err := orchSvc.Start(ctx); err != nil {
		log.Fatalf("failed to start orchestrator: %v", err)
	}

	// 能力声明服务
	capSvc := capabilities.NewService(cfg.Gateway)

	// 构造 HTTP/WS 相关 handler
	jsonrpcHandler := jsonrpc.NewHandler(orchSvc, capSvc)
	restRouter := rest.NewRouter(orchSvc)
	subscriptionTokenStore := websocket.NewSubscriptionTokenStore(redisCache, cfg.WebSocket.SubscriptionTokenTTL) // Construct the shared Redis-backed subscription-token store so the browser helper endpoint and WebSocket handshake validation use the same authority.
	restRouter.SetSubscriptionTokenStore(subscriptionTokenStore)                                                  // Inject the shared token store into the trimmed HTTP helper router so browser clients can mint short-lived subscription tokens.
	authMiddleware, err := buildGatewayAuthMiddleware(ctx, cfg, pgDAO, redisCache)                                // Construct the optional gateway auth middleware once so JSON-RPC and REST can share the same auth pipeline.
	if err != nil {                                                                                               // Stop startup when a configured auth source cannot be initialized safely.
		log.Fatalf("failed to init gateway auth: %v", err) // Surface the auth construction failure before the HTTP listener starts.
	}
	wsHub := websocket.NewHub(redisCache)
	go wsHub.Run(ctx)

	protectedMux := http.NewServeMux()              // Isolate auth-protected routes so health, metrics, and WebSocket can keep their dedicated exposure rules.
	protectedMux.Handle("/jsonrpc", jsonrpcHandler) // Register the JSON-RPC endpoint inside the protected route subtree.
	restRouter.RegisterRoutes(protectedMux)         // Register the remaining browser helper endpoint that issues short-lived WebSocket subscription tokens.
	protectedHandler := http.Handler(protectedMux)  // Seed the protected subtree handler with the raw mux before optional auth wrapping.
	if authMiddleware != nil {                      // Wrap the protected subtree only when at least one auth validator is active.
		protectedHandler = authMiddleware.Handle(protectedHandler) // Enforce HMAC/OIDC/PAT auth for JSON-RPC and REST requests.
	}

	mux := http.NewServeMux()                                                                                                                     // Build the public root mux that combines protected APIs with public operational endpoints.
	mux.Handle("/", protectedHandler)                                                                                                             // Mount the protected subtree under "/" so JSON-RPC and REST remain reachable through their existing paths.
	mux.Handle("/ws/plan-events", websocket.NewHandler(wsHub, subscriptionTokenStore, websocket.ParseAllowedOrigins(cfg.Gateway.AllowedOrigins))) // Enforce subscription-token and origin checks during the WebSocket handshake instead of accepting traceId-only requests.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		payload := orchSvc.HealthCheck(r.Context())      // Evaluate the live orchestrator health snapshot before deciding the HTTP status code returned by /healthz.
		writeJSON(w, healthHTTPStatus(payload), payload) // Return HTTP 503 only when the orchestrator reports a down state so callers can distinguish degraded from unavailable service.
	})
	mux.Handle("/metrics", promhttp.Handler())

	srv := newGatewayHTTPServer(cfg, telemetry.TraceMiddleware("gateway", mux)) // Construct the HTTP server once so TLS policy and port binding follow the shared helper logic.

	go func() {
		log.Printf("Gateway started on port %d", cfg.Gateway.Port)                        // Emit the listener port so operators can confirm the active HTTP endpoint quickly from logs.
		if err := serveGatewayHTTP(srv, cfg); err != nil && err != http.ErrServerClosed { // Start HTTP or HTTPS according to the gateway TLS configuration.
			log.Fatalf("listen: %s\n", err) // Stop the process when the listener fails outside the normal graceful-shutdown path.
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

// healthHTTPStatus maps the structured orchestrator health payload to the HTTP status returned by /healthz.
func healthHTTPStatus(payload map[string]interface{}) int {
	if payload["status"] == "down" { // Return 503 only when the orchestrator explicitly reports a down state that should fail upstream readiness checks.
		return http.StatusServiceUnavailable // Surface unavailable health directly through the HTTP status code so load balancers and probes react correctly.
	}

	return http.StatusOK // Return 200 for healthy and degraded states so non-fatal capability issues remain inspectable without ejecting the instance immediately.
}
