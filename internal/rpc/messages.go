package rpc

import "encoding/json"

// --- Worker service messages ---

// ExecutePlanRequest is sent by the orchestrator dispatcher to a worker.
type ExecutePlanRequest struct {
	TraceID   string          `json:"trace_id"`
	SessionID string          `json:"session_id"`
	Attempt   int64           `json:"attempt"`
	Plan      json.RawMessage `json:"plan"`
}

// ExecutePlanResponse is returned by the worker after accepting or rejecting the plan.
type ExecutePlanResponse struct {
	Status  string `json:"status"` // "accepted" | "error"
	Message string `json:"message,omitempty"`
}

// CancelPlanRequest is sent to a worker to cancel an in-flight plan.
type CancelPlanRequest struct {
	TraceID string `json:"trace_id"`
}

// CancelPlanResponse is returned by the worker after processing the cancellation.
type CancelPlanResponse struct {
	Status  string `json:"status"` // "cancelled" | "not_found"
	Message string `json:"message,omitempty"`
}

// --- Orchestrator service messages ---

// RegisterWorkerRequest is sent by a worker on startup.
type RegisterWorkerRequest struct {
	WorkerID string            `json:"worker_id"`
	Address  string            `json:"address"`
	Capacity int               `json:"capacity"`
	Tags     map[string]string `json:"tags"`
}

// RegisterWorkerResponse is returned by the orchestrator after registration.
type RegisterWorkerResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// HeartbeatRequest is sent periodically by a worker.
type HeartbeatRequest struct {
	WorkerID   string `json:"worker_id"`
	ActiveLoad int    `json:"active_load"`
}

// HeartbeatResponse is returned by the orchestrator.
type HeartbeatResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// PlanEventMetrics mirrors the worker executor metrics embedded in distributed step-event callbacks.
type PlanEventMetrics struct {
	Attempt   int   `json:"attempt"`
	WDCalls   int   `json:"wd_calls"`
	ElapsedMs int64 `json:"elapsed_ms"`
}

// ReportPlanEventRequest is sent by a worker whenever one distributed step event should be persisted by the orchestrator.
type ReportPlanEventRequest struct {
	TraceID      string           `json:"trace_id"`
	WorkerID     string           `json:"worker_id"`
	Attempt      int64            `json:"attempt"`
	StepIndex    int              `json:"step_index"`
	Status       string           `json:"status"`
	Message      string           `json:"message,omitempty"`
	Phase        string           `json:"phase,omitempty"`
	ArtifactRefs []string         `json:"artifact_refs,omitempty"`
	Metrics      PlanEventMetrics `json:"metrics"`
}

// ReportPlanEventResponse is returned by the orchestrator after validating and optionally appending one distributed step event.
type ReportPlanEventResponse struct {
	Accepted bool   `json:"accepted"`
	Stale    bool   `json:"stale,omitempty"`
	Message  string `json:"message,omitempty"`
}

// RenewLeaseRequest is sent by a worker to refresh one distributed execution lease while it still owns the trace attempt.
type RenewLeaseRequest struct {
	TraceID  string `json:"trace_id"`
	WorkerID string `json:"worker_id"`
	Attempt  int64  `json:"attempt"`
}

// RenewLeaseResponse is returned by the orchestrator after validating and optionally refreshing one distributed execution lease.
type RenewLeaseResponse struct {
	Accepted bool   `json:"accepted"`
	Stale    bool   `json:"stale,omitempty"`
	Message  string `json:"message,omitempty"`
}

// CompletePlanRequest is sent by a worker after one distributed trace attempt reaches a terminal outcome.
type CompletePlanRequest struct {
	TraceID        string `json:"trace_id"`
	WorkerID       string `json:"worker_id"`
	Attempt        int64  `json:"attempt"`
	FinalStatus    string `json:"final_status"`
	TerminalReason string `json:"terminal_reason,omitempty"`
	Message        string `json:"message,omitempty"`
	ErrorCode      string `json:"error_code,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
}

// CompletePlanResponse is returned by the orchestrator after validating and optionally finalizing one distributed trace attempt.
type CompletePlanResponse struct {
	Accepted bool   `json:"accepted"`
	Stale    bool   `json:"stale,omitempty"`
	Message  string `json:"message,omitempty"`
}
