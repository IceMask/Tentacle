package orchestrator

import (
	"context"
	"log"

	pb "github.com/mcp/mobile-worker/pkg/proto/orchestrator"
	"github.com/mcp/mobile-worker/internal/storage/redis"
)

type Service struct {
	pb.UnimplementedOrchestratorServer
	redisClient *redis.Client
}

func NewService(redisClient *redis.Client) *Service {
	return &Service{
		redisClient: redisClient,
	}
}

func (s *Service) StartSession(ctx context.Context, req *pb.StartSessionRequest) (*pb.StartSessionResponse, error) {
	log.Printf("StartSession: project=%s, user=%s", req.ProjectId, req.UserId)
	// Logic to assign session, check quotas, etc.
	return &pb.StartSessionResponse{
		SessionId:    "generated-session-id",
		Capabilities: req.Capabilities,
	}, nil
}

func (s *Service) ExecutePlan(ctx context.Context, req *pb.ExecutePlanRequest) (*pb.ExecutePlanResponse, error) {
	log.Printf("ExecutePlan: session=%s, trace=%s", req.SessionId, req.TraceId)
	// Logic to dispatch plan to worker
	return &pb.ExecutePlanResponse{
		TraceId: req.TraceId,
		Status:  "queued",
	}, nil
}

func (s *Service) CancelPlan(ctx context.Context, req *pb.CancelPlanRequest) (*pb.CancelPlanResponse, error) {
	log.Printf("CancelPlan: trace=%s", req.TraceId)
	// Logic to cancel plan
	return &pb.CancelPlanResponse{Success: true}, nil
}

func (s *Service) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{Status: "healthy"}, nil
}
