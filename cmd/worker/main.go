package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/startup"
	"mcp_for_appium/internal/telemetry"
	"mcp_for_appium/internal/worker"
)

// main is the entry point for this binary.
func main() {
	configPath := strings.TrimSpace(os.Getenv("CONFIG_PATH")) // Honor the shared deployment-level configuration override.
	if configPath == "" {                                     // Preserve local startup behavior when no environment override is supplied.
		configPath = "config.yaml" // Use the repository-default configuration file.
	}
	cfg, err := config.Load(configPath) // Load the resolved configuration path before worker startup validation.
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	if err := startup.ValidateWorkerStartup(cfg); err != nil { // Reject malformed startup config before any listener or registration logic begins.
		log.Fatalf("worker startup preflight failed: %v", err) // Stop immediately so the worker never advertises itself with invalid runtime settings.
	}

	telemetry.InitLogger(cfg.Telemetry.LogLevel, cfg.Telemetry.LogFile) // Initialize structured logging with optional file sink.
	log.SetOutput(telemetry.StdLogWriter())                             // Route standard log messages to configured sinks too.
	logger := telemetry.Logger()
	appiumSupervisor := startup.NewLocalAppiumSupervisor(cfg.Worker.AppiumURL) // Construct one worker-owned Appium supervisor so loopback Appium can be auto-started before registration and before later plan execution.
	defer func() {                                                             // Stop any worker-owned local Appium child process during process shutdown so the worker does not leave an orphaned automation server behind.
		if err := appiumSupervisor.Stop(); err != nil { // Surface supervisor shutdown failures without masking the rest of the worker shutdown flow.
			logger.Warn("failed to stop managed local appium service", "error", err) // Log the managed Appium shutdown failure for operator diagnosis.
		}
	}() // Close the deferred worker-owned Appium supervisor shutdown closure.
	preflightCtx, preflightCancel := context.WithTimeout(context.Background(), 5*time.Second) // Bound the Appium readiness probe so worker startup still fails fast when local auto-start or remote reachability cannot succeed.
	defer preflightCancel()                                                                   // Release the dependency-check timeout resources after startup validation finishes.
	if err := appiumSupervisor.EnsureReady(preflightCtx); err != nil {                        // Ensure the configured Appium endpoint is reachable and auto-start one local loopback Appium service before the worker registers as healthy.
		log.Fatalf("worker startup preflight failed for appium: %v", err) // Stop immediately so the worker does not register as healthy while its primary automation dependency is unavailable.
	}

	advertiseAddress := buildWorkerAdvertiseAddress(cfg.Worker.AdvertiseAddr, cfg.Worker.GRPCPort) // Resolve one routable worker address so the orchestrator can call back into this worker without relying on localhost.
	workerID := advertiseAddress                                                                   // Reuse the stable advertise address as the worker identifier so restart and heartbeat recovery keep the same identity.

	// --- connect to orchestrator before constructing the worker service so distributed callbacks can reuse the live client ---
	orchClient, err := rpc.NewOrchestratorClient(cfg.Worker.OrchestratorAddr, cfg.RPC.Security) // Dial the orchestrator using the configured internal RPC transport mode and shared-token auth.
	if err != nil {
		log.Fatalf("failed to dial orchestrator at %s: %v", cfg.Worker.OrchestratorAddr, err)
	}
	defer orchClient.Close()

	concurrency := cfg.Worker.Concurrency // Resolve worker admission capacity before constructing or advertising the gRPC service.
	if concurrency <= 0 {                 // Preserve the documented default when configuration omits or supplies an invalid capacity.
		concurrency = 4 // Use four authoritative worker execution slots by default.
	}

	// --- gRPC server (receives ExecutePlan / CancelPlan from orchestrator) ---
	workerSvc := worker.NewGRPCServer(cfg.Worker.AppiumURL, cfg.Orchestrator.StepTimeout, cfg.Orchestrator.AutoWaitMax, workerID, orchClient, leaseRenewEvery(cfg.Worker.HeartbeatInterval), concurrency) // Construct the distributed worker with the same authoritative capacity advertised during registration.
	workerSvc.SetAppiumReadyFunc(appiumSupervisor.EnsureReady)                                                                                                                                            // Reuse the same worker-owned Appium supervisor before every plan so local Appium is auto-started again if it goes away after startup.
	grpcServerOptions, err := rpc.NewServerOptions(cfg.RPC.Security)                                                                                                                                      // Build the worker gRPC server options that match the configured internal RPC transport mode and shared-token enforcement.
	if err != nil {                                                                                                                                                                                       // Stop immediately when the configured internal RPC security settings cannot be turned into a gRPC server safely.
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

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()

	resp, err := orchClient.RegisterWorker(regCtx, &rpc.RegisterWorkerRequest{
		WorkerID: workerID,
		Address:  advertiseAddress,
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
			heartbeatCtx, heartbeatCancel := context.WithTimeout(ctx, 5*time.Second) // Bound each heartbeat independently so one stalled RPC cannot halt future liveness reports forever.
			_, err := client.Heartbeat(heartbeatCtx, &rpc.HeartbeatRequest{          // Report authoritative admitted-run load through the bounded RPC context.
				WorkerID:   workerID,
				ActiveLoad: svc.ActiveLoad(),
			})
			heartbeatCancel() // Release the per-heartbeat timer immediately after the RPC returns.
			if err != nil {   // Log this heartbeat failure and allow the next ticker iteration to retry.
				logger.Error("heartbeat failed", "worker_id", workerID, "error", err)
			}
		}
	}
}

// buildWorkerAdvertiseAddress resolves the routable worker address registered with the orchestrator for later distributed ExecutePlan and CancelPlan RPCs.
func buildWorkerAdvertiseAddress(configuredAddress string, port int) string {
	if configuredAddress != "" { // Prefer the explicit configured advertise address whenever the operator provides one.
		return configuredAddress // Return the operator-supplied address verbatim so distributed routing follows explicit deployment intent.
	}

	hostname, err := os.Hostname()    // Resolve the current host name so the worker can register one stable non-localhost callback address by default.
	if err != nil || hostname == "" { // Fall back to loopback only when the host name cannot be resolved locally.
		hostname = "127.0.0.1" // Keep local development working even when host-name resolution is unavailable in the current environment.
	}

	return fmt.Sprintf("%s:%d", hostname, port) // Build the default worker advertise address from the resolved host name and worker gRPC port.
}

// leaseRenewEvery derives the worker-side distributed lease-renewal cadence from the configured heartbeat interval.
func leaseRenewEvery(heartbeatInterval time.Duration) time.Duration {
	if heartbeatInterval <= 0 { // Fall back to a conservative default renewal cadence when the worker heartbeat interval is missing or invalid.
		return 5 * time.Second // Keep lease renewals frequent enough that the orchestrator can detect lost ownership promptly.
	}
	if heartbeatInterval/2 < time.Second { // Clamp extremely small heartbeat intervals so the worker does not spin on sub-second lease renewal RPCs.
		return time.Second // Renew once per second in very small-interval test setups to avoid excessive gRPC chatter.
	}

	return heartbeatInterval / 2 // Renew several times inside each ownership window so transient RPC failures still leave time for later successful renewals.
}
