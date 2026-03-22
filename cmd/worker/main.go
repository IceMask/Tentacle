package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/startup"
	"mcp_for_appium/internal/telemetry"
	"mcp_for_appium/internal/worker"
)

// main is the entry point for this binary.
func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	if err := startup.ValidateWorkerStartup(cfg); err != nil { // Reject malformed startup config before any listener or registration logic begins.
		log.Fatalf("worker startup preflight failed: %v", err) // Stop immediately so the worker never advertises itself with invalid runtime settings.
	}

	telemetry.InitLogger(cfg.Telemetry.LogLevel, cfg.Telemetry.LogFile) // Initialize structured logging with optional file sink.
	log.SetOutput(telemetry.StdLogWriter())                             // Route standard log messages to configured sinks too.
	logger := telemetry.Logger()
	preflightCtx, preflightCancel := context.WithTimeout(context.Background(), 5*time.Second)   // Bound the Appium reachability probe so worker startup fails fast on unavailable devices.
	defer preflightCancel()                                                                     // Release the dependency-check timeout resources after startup validation finishes.
	if err := startup.CheckAppiumReachability(preflightCtx, cfg.Worker.AppiumURL); err != nil { // Verify the configured Appium endpoint is reachable before the worker starts serving plans.
		log.Fatalf("worker startup preflight failed for appium: %v", err) // Stop immediately so the worker does not register as healthy while its primary dependency is down.
	}

	workerID := uuid.New().String()

	// --- gRPC server (receives ExecutePlan / CancelPlan from orchestrator) ---
	workerSvc := worker.NewGRPCServer(cfg.Worker.AppiumURL, cfg.Orchestrator.StepTimeout, cfg.Orchestrator.AutoWaitMax)
	grpcServerOptions, err := rpc.NewServerOptions(cfg.RPC.Security) // Build the worker gRPC server options that match the configured internal RPC transport mode and shared-token enforcement.
	if err != nil {                                                  // Stop immediately when the configured internal RPC security settings cannot be turned into a gRPC server safely.
		log.Fatalf("failed to initialize worker gRPC security: %v", err) // Surface the gRPC security wiring failure before the listener starts.
	}
	grpcSrv := grpc.NewServer(grpcServerOptions...) // Construct the worker gRPC server with the configured transport credentials and shared-token interceptor.
	rpc.RegisterWorkerServiceServer(grpcSrv, workerSvc)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Worker.GRPCPort))
	if err != nil {
		log.Fatalf("failed to listen on port %d: %v", cfg.Worker.GRPCPort, err)
	}
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()
	logger.Info("worker gRPC server started", "port", cfg.Worker.GRPCPort)

	// --- connect to orchestrator, register, and start heartbeat loop ---
	orchClient, err := rpc.NewOrchestratorClient(cfg.Worker.OrchestratorAddr, cfg.RPC.Security) // Dial the orchestrator using the configured internal RPC transport mode and shared-token auth.
	if err != nil {
		log.Fatalf("failed to dial orchestrator at %s: %v", cfg.Worker.OrchestratorAddr, err)
	}
	defer orchClient.Close()

	concurrency := cfg.Worker.Concurrency
	if concurrency == 0 {
		concurrency = 4
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()

	resp, err := orchClient.RegisterWorker(regCtx, &rpc.RegisterWorkerRequest{
		WorkerID: workerID,
		Address:  fmt.Sprintf("localhost:%d", cfg.Worker.GRPCPort),
		Capacity: concurrency,
		Tags:     cfg.Worker.Tags,
	})
	if err != nil {
		log.Fatalf("failed to register with orchestrator: %v", err)
	}
	if !resp.Success {
		log.Fatalf("orchestrator rejected registration: %s", resp.Message)
	}
	logger.Info("worker registered", "worker_id", workerID, "orchestrator", cfg.Worker.OrchestratorAddr)

	// Heartbeat loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go heartbeatLoop(ctx, orchClient, workerID, cfg.Worker.HeartbeatInterval, workerSvc)

	// --- graceful shutdown on signal ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down worker...")
	cancel()
	grpcSrv.GracefulStop()
}

// heartbeatLoop executes this operation.
func heartbeatLoop(ctx context.Context, client *rpc.OrchestratorClient, workerID string, interval time.Duration, svc *worker.GRPCServer) {
	logger := telemetry.Logger()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := client.Heartbeat(ctx, &rpc.HeartbeatRequest{
				WorkerID:   workerID,
				ActiveLoad: svc.ActiveLoad(),
			}); err != nil {
				logger.Error("heartbeat failed", "worker_id", workerID, "error", err)
			}
		}
	}
}
