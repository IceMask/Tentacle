package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ExecuteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "execute_latency_seconds",
		Help: "Duration of plan execution",
	}, []string{"tenant", "project", "status"})

	PlanEventSeqGaps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "plan_event_seq_gaps",
		Help: "Total number of sequence gaps detected in plan events",
	}, []string{"tenant", "project"})

	ConcurrencyInUse = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "concurrency_in_use",
		Help: "Current number of concurrent executions",
	}, []string{"tenant", "project"})

	QueueWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "queue_wait_seconds",
		Help: "Time spent in queue before execution",
	}, []string{"tenant", "project"})

	AppiumRTT = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "appium_rtt_ms",
		Help:    "Round trip time to Appium server",
		Buckets: []float64{10, 50, 100, 200, 500, 1000, 2000},
	}, []string{"worker_id", "host"})

	WebsocketDrops = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "websocket_drops_total",
		Help: "Total number of dropped websocket messages due to backpressure",
	}, []string{"tenant", "trace_id"})

	StreamLogsBackpressure = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stream_logs_backpressure_total",
		Help: "Total backpressure events for stream logs",
	}, []string{"tenant", "trace_id"})

	WSQueueLen = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ws_queue_len_histogram",
		Help:    "Histogram of WebSocket queue lengths",
		Buckets: []float64{0, 10, 50, 100, 200, 256},
	})

	ArtifactUploadLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "artifact_upload_latency",
		Help: "Latency of artifact uploads",
	}, []string{"type", "size_bucket"})

	ADFLeaseSuccessRate = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "adf_lease_success_rate",
		Help: "Success rate of ADF lease acquisition",
	}, []string{"result"}) // success, failure

	ErrorCodeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "error_code_total",
		Help: "Total count of errors by code",
	}, []string{"code", "service"})

	HealthStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "health_status",
		Help: "Health status (0=down, 1=degraded, 2=healthy)",
	}, []string{"service"})
)

// InitMetrics initializes any extra metrics logic if needed.
func InitMetrics() {
	// Metrics are auto-registered by promauto
}
