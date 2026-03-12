package rpc

import "encoding/json"

// --- Worker service messages ---

// ExecutePlanRequest is sent by the orchestrator dispatcher to a worker.
type ExecutePlanRequest struct {
	TraceID   string          `json:"trace_id"`
	SessionID string          `json:"session_id"`
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
