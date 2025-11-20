package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/mcp/mobile-worker/internal/worker"
	pb "github.com/mcp/mobile-worker/pkg/proto/worker"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "50052"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()

	workerService := worker.NewService()
	pb.RegisterWorkerServer(s, workerService)

	log.Printf("Worker server listening on %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
