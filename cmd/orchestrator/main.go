package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/mcp/mobile-worker/internal/orchestrator"
	pb "github.com/mcp/mobile-worker/pkg/proto/orchestrator"
	"github.com/mcp/mobile-worker/internal/storage/redis"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisClient, err := redis.NewClient(redisAddr, "", 0)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()

	orchService := orchestrator.NewService(redisClient)
	pb.RegisterOrchestratorServer(s, orchService)

	log.Printf("Orchestrator server listening on %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
