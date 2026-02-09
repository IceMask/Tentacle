package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"mcp_for_appium/internal/rpc"
	"mcp_for_appium/internal/telemetry"
	"mcp_for_appium/internal/worker/appium"
)

// GRPCServer implements rpc.WorkerServiceServer.
// Each incoming ExecutePlan call spawns a goroutine that creates its own
// Appium client and Executor so that multiple plans can run concurrently.
type GRPCServer struct {
	appiumURL   string
	stepTimeout time.Duration
	autoWaitMax time.Duration

	planMu     sync.Mutex
	planCancel map[string]context.CancelFunc

	logger *slog.Logger
}

// NewGRPCServer creates a new worker gRPC service handler.
func NewGRPCServer(appiumURL string, stepTimeout, autoWaitMax time.Duration) *GRPCServer {
	return &GRPCServer{
		appiumURL:   appiumURL,
		stepTimeout: stepTimeout,
		autoWaitMax: autoWaitMax,
		planCancel:  make(map[string]context.CancelFunc),
		logger:      telemetry.Logger(),
	}
}

// ActiveLoad returns the number of plans currently executing on this worker.
func (s *GRPCServer) ActiveLoad() int {
	s.planMu.Lock()
	defer s.planMu.Unlock()
	return len(s.planCancel)
}

// ExecutePlan implements rpc.WorkerServiceServer.
// The plan is executed asynchronously; this method returns "accepted" immediately.
func (s *GRPCServer) ExecutePlan(_ context.Context, req *rpc.ExecutePlanRequest) (*rpc.ExecutePlanResponse, error) {
	s.logger.Info("received ExecutePlan", "trace_id", req.TraceID, "session_id", req.SessionID)
	go s.runPlan(req.TraceID, req.SessionID, req.Plan)
	return &rpc.ExecutePlanResponse{Status: "accepted"}, nil
}

// CancelPlan implements rpc.WorkerServiceServer.
func (s *GRPCServer) CancelPlan(_ context.Context, req *rpc.CancelPlanRequest) (*rpc.CancelPlanResponse, error) {
	s.planMu.Lock()
	cancel, ok := s.planCancel[req.TraceID]
	s.planMu.Unlock()

	if !ok {
		return &rpc.CancelPlanResponse{Status: "not_found"}, nil
	}
	cancel()
	s.logger.Info("plan cancelled", "trace_id", req.TraceID)
	return &rpc.CancelPlanResponse{Status: "cancelled"}, nil
}

func (s *GRPCServer) runPlan(traceID string, _ string, planRaw json.RawMessage) {
	ctx, cancel := context.WithCancel(context.Background())
	s.planMu.Lock()
	s.planCancel[traceID] = cancel
	s.planMu.Unlock()
	defer func() {
		cancel()
		s.planMu.Lock()
		delete(s.planCancel, traceID)
		s.planMu.Unlock()
	}()

	steps, err := ParsePlan(planRaw)
	if err != nil {
		s.logger.Error("failed to parse plan", "trace_id", traceID, "error", err)
		return
	}

	client := appium.NewClient(s.appiumURL)
	exec := NewExecutor(client, s.stepTimeout, s.autoWaitMax, func(ev StepEvent) {
		s.logger.Info("step event",
			"trace_id", traceID,
			"step_index", ev.StepIndex,
			"status", ev.Status,
			"message", ev.Message)
	})

	if err := exec.Execute(ctx, steps); err != nil {
		s.logger.Error("plan execution failed", "trace_id", traceID, "error", err)
		return
	}
	s.logger.Info("plan execution completed", "trace_id", traceID)
}
