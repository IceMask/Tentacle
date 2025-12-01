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
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Init Appium Client
	appiumClient := appium.NewClient(cfg.Worker.AppiumURL)

	// Init Executor
	exec := worker.NewExecutor(appiumClient)

	// Connect to Orchestrator (gRPC) - TODO
	// For now, just log
	log.Printf("Worker started, connected to Appium at %s", cfg.Worker.AppiumURL)

	// Wait for shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down worker...")
	_ = exec
}
