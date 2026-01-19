package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/worker"
	"mcp_for_appium/internal/worker/appium"
)

func main() {
	// 加载配置
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// 构造 Appium 客户端
	appiumClient := appium.NewClient(cfg.Worker.AppiumURL)

	// 创建执行器（后续接入 orchestrator 任务）
	exec := worker.NewExecutor(appiumClient)

	// Connect to Orchestrator (gRPC) - TODO
	// For now, just log
	log.Printf("Worker started, connected to Appium at %s", cfg.Worker.AppiumURL)

	// 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down worker...")
	_ = exec
}
