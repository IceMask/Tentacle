package worker

import (
	"context"
	"log"
	"time"

	pb "github.com/mcp/mobile-worker/pkg/proto/worker"
)

type Service struct {
	pb.UnimplementedWorkerServer
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) StartSession(ctx context.Context, req *pb.StartSessionRequest) (*pb.StartSessionResponse, error) {
	log.Printf("Worker StartSession: session=%s", req.SessionId)
	return &pb.StartSessionResponse{Success: true}, nil
}

func (s *Service) ExecutePlan(req *pb.ExecutePlanRequest, stream pb.Worker_ExecutePlanServer) error {
	log.Printf("Worker ExecutePlan: session=%s, trace=%s", req.SessionId, req.TraceId)

	// Mock execution
	steps := 5
	for i := 0; i < steps; i++ {
		if err := stream.Send(&pb.PlanEvent{
			Seq:       int32(i),
			StepIndex: int32(i),
			Status:    "running",
			Message:   "Executing step",
		}); err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)

		if err := stream.Send(&pb.PlanEvent{
			Seq:       int32(i),
			StepIndex: int32(i),
			Status:    "passed",
			Message:   "Step passed",
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) CancelPlan(ctx context.Context, req *pb.CancelPlanRequest) (*pb.CancelPlanResponse, error) {
	log.Printf("Worker CancelPlan: trace=%s", req.TraceId)
	return &pb.CancelPlanResponse{Success: true}, nil
}

func (s *Service) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{Status: "healthy"}, nil
}
