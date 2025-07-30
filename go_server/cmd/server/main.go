package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/device"
	"mcp_for_appium/internal/service"
	pb "mcp_for_appium/proto/v1"
)

var (
	configFile = flag.String("config", "config.yaml", "Path to configuration file")
	port       = flag.Int("port", 50051, "The server port")
	debug      = flag.Bool("debug", false, "Enable debug mode")
)

func main() {
	flag.Parse()

	// 初始化日志
	if *debug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	} else {
		log.SetFlags(log.LstdFlags)
	}

	// 加载配置
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 初始化设备管理器
	deviceManager, err := device.NewManager(cfg.DeviceConfig)
	if err != nil {
		log.Fatalf("Failed to initialize device manager: %v", err)
	}
	defer deviceManager.Cleanup()

	// 创建 gRPC 服务器
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Second,
			MaxConnectionAge:      30 * time.Second,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  5 * time.Second,
			Timeout:               1 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	if cfg.TLS.Enabled {
		// TODO: 添加 TLS 配置
	}

	grpcServer := grpc.NewServer(opts...)

	// 注册服务
	registerServices(grpcServer, deviceManager, cfg)

	// 注册健康检查
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	// 注册反射服务（用于调试）
	if *debug {
		reflection.Register(grpcServer)
	}

	// 启动服务器
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// 优雅关闭
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutting down server...")

		// 给正在进行的请求一些时间完成
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
			log.Println("Server stopped gracefully")
		case <-ctx.Done():
			log.Println("Forcing server stop")
			grpcServer.Stop()
		}

		deviceManager.Cleanup()
		os.Exit(0)
	}()

	log.Printf("Starting gRPC server on port %d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func registerServices(s *grpc.Server, dm *device.Manager, cfg *config.Config) {
	// 创建服务实例
	deviceService := service.NewDeviceService(dm)
	sessionService := service.NewSessionService(dm, cfg.Session)
	interactionService := service.NewInteractionService(dm)
	screenService := service.NewScreenService(dm)
	applicationService := service.NewApplicationService(dm)
	testService := service.NewTestService(dm, cfg.Test)
	systemService := service.NewSystemService(dm)

	// 注册到 gRPC 服务器
	pb.RegisterDeviceServiceServer(s, deviceService)
	pb.RegisterSessionServiceServer(s, sessionService)
	pb.RegisterInteractionServiceServer(s, interactionService)
	pb.RegisterScreenServiceServer(s, screenService)
	pb.RegisterApplicationServiceServer(s, applicationService)
	pb.RegisterTestServiceServer(s, testService)
	pb.RegisterSystemServiceServer(s, systemService)

	log.Println("All services registered successfully")
}
