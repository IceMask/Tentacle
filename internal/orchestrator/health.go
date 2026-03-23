// health.go implements orchestrator health reporting that covers storage, execution dependencies, worker capacity, and queue state.
package orchestrator

import (
	"context"
	"strings"
	"time"

	"mcp_for_appium/internal/startup"
	"mcp_for_appium/internal/telemetry"
)

const healthProbeTimeout = 2 * time.Second // healthProbeTimeout bounds each dependency probe so health requests fail fast instead of hanging indefinitely on one dependency.

// workerRegistryHealthSummary captures the current worker-state counts used by health reporting.
type workerRegistryHealthSummary struct {
	Total    int
	Healthy  int
	Degraded int
	Offline  int
	Unknown  int
}

// queueHealthSummary captures the queue and distributed in-flight counts used by health reporting.
type queueHealthSummary struct {
	Shards           int
	RetainedMessages int64
	PendingMessages  int64
	InflightTraces   int
}

// HealthCheck returns a richer orchestrator health snapshot that covers storage, execution dependencies, worker capacity, and queue state.
func (s *Service) HealthCheck(ctx context.Context) map[string]interface{} {
	status := "healthy"                                            // Start from the optimistic assumption that every dependency probe will succeed.
	issues := []string{}                                           // Accumulate stable issue keys so callers can alert on precise failing subsystems.
	checks := map[string]string{}                                  // Collect one status string per dependency check for compact client-side display.
	details := map[string]interface{}{"executionMode": s.execMode} // Return extra structured context so operators can inspect the health basis without scraping logs.
	criticalFailure := false                                       // Track whether any core execution dependency has failed badly enough to mark the service down.
	degradedFailure := false                                       // Track whether any non-critical dependency or partial-capacity issue should mark the service degraded.

	if s.dao == nil { // Treat a missing DAO as a hard-down condition because sessions, traces, and events cannot be persisted without PostgreSQL.
		checks["database"] = "error"                 // Report the database check as failed so clients can see the missing dependency directly.
		issues = appendIssueOnce(issues, "postgres") // Add the stable database issue key once for alerting and dashboards.
		criticalFailure = true                       // Mark the health snapshot down because the orchestrator cannot operate safely without persistence.
	} else {
		probeCtx, cancel := withHealthProbeTimeout(ctx) // Bound the database ping so the health endpoint never hangs indefinitely on PostgreSQL.
		err := s.dao.Ping(probeCtx)                     // Probe PostgreSQL connectivity using the live DAO pool so the result matches runtime behavior.
		cancel()                                        // Release the probe timeout resources immediately after the database check finishes.
		if err != nil {                                 // Treat a failed PostgreSQL ping as a hard-down condition for the orchestrator.
			checks["database"] = "error"                                       // Report the database probe failure directly in the compact check map.
			issues = appendIssueOnce(issues, "postgres")                       // Add the stable database issue key once for alerting and dashboards.
			details["database"] = map[string]interface{}{"error": err.Error()} // Preserve the exact database failure string for operator diagnosis.
			criticalFailure = true                                             // Mark the health snapshot down because core persistence is unavailable.
		} else {
			checks["database"] = "ok" // Report a healthy database probe in the compact check map.
		}
	}

	if s.cache == nil { // Treat a missing Redis cache as a hard-down condition because queueing, worker registry, and cancellation markers depend on it.
		checks["redis"] = "error"                 // Report the cache check as failed so clients can see the missing dependency directly.
		issues = appendIssueOnce(issues, "redis") // Add the stable Redis issue key once for alerting and dashboards.
		criticalFailure = true                    // Mark the health snapshot down because the orchestrator cannot coordinate work safely without Redis.
	} else {
		probeCtx, cancel := withHealthProbeTimeout(ctx) // Bound the Redis ping so the health endpoint never hangs indefinitely on cache availability checks.
		err := s.cache.Ping(probeCtx)                   // Probe Redis using the live cache wrapper so the result matches runtime queue behavior.
		cancel()                                        // Release the probe timeout resources immediately after the Redis check finishes.
		if err != nil {                                 // Treat a failed Redis ping as a hard-down condition for the orchestrator.
			checks["redis"] = "error"                                       // Report the Redis probe failure directly in the compact check map.
			issues = appendIssueOnce(issues, "redis")                       // Add the stable Redis issue key once for alerting and dashboards.
			details["redis"] = map[string]interface{}{"error": err.Error()} // Preserve the exact Redis failure string for operator diagnosis.
			criticalFailure = true                                          // Mark the health snapshot down because core queue and coordination state are unavailable.
		} else {
			checks["redis"] = "ok" // Report a healthy Redis probe in the compact check map.
		}
	}

	if s.s3 == nil { // Skip artifact-store probing when no S3 client is wired for the current process.
		checks["s3"] = "skipped"                                    // Report the artifact-store check as skipped because the client is not configured in this process.
		details["s3"] = map[string]interface{}{"configured": false} // Return explicit metadata so operators can distinguish intentional absence from silent omission.
	} else {
		probeCtx, cancel := withHealthProbeTimeout(ctx) // Bound the bucket probe so the health endpoint never hangs indefinitely on object-store metadata calls.
		err := s.s3.ValidateBucket(probeCtx)            // Probe the configured artifact bucket using the live S3 client so the result matches runtime uploads.
		cancel()                                        // Release the probe timeout resources immediately after the S3 check finishes.
		if err != nil {                                 // Treat artifact-store failure as degraded because trace execution can still proceed while uploads fail.
			checks["s3"] = "error"                                                           // Report the artifact-store probe failure directly in the compact check map.
			issues = appendIssueOnce(issues, "s3")                                           // Add the stable artifact-store issue key once for alerting and dashboards.
			details["s3"] = map[string]interface{}{"configured": true, "error": err.Error()} // Preserve the exact S3 failure string for operator diagnosis.
			degradedFailure = true                                                           // Mark the health snapshot degraded because artifact persistence is impaired but not fully fatal.
		} else {
			checks["s3"] = "ok"                                        // Report a healthy artifact-store probe in the compact check map.
			details["s3"] = map[string]interface{}{"configured": true} // Return explicit metadata so operators can see that object storage is wired and healthy.
		}
	}

	if s.execMode == ExecutionModeMonolith { // Probe the embedded Appium dependency only when this service executes plans directly in-process.
		if strings.TrimSpace(s.appiumURL) == "" { // Treat a missing Appium URL as a hard-down condition because monolith execution cannot create or attach sessions without it.
			checks["appium"] = "error"                 // Report the Appium dependency as failed so clients can see the execution dependency problem directly.
			issues = appendIssueOnce(issues, "appium") // Add the stable Appium issue key once for alerting and dashboards.
			criticalFailure = true                     // Mark the health snapshot down because direct execution is unavailable in monolith mode.
		} else {
			probeCtx, cancel := withHealthProbeTimeout(ctx)               // Bound the Appium status probe so the health endpoint fails fast on unreachable automation hosts.
			err := startup.CheckAppiumReachability(probeCtx, s.appiumURL) // Probe the configured Appium status endpoint using the same startup-time reachability check.
			cancel()                                                      // Release the probe timeout resources immediately after the Appium check finishes.
			if err != nil {                                               // Treat an unreachable Appium dependency as a hard-down condition in monolith mode.
				checks["appium"] = "error"                                                           // Report the Appium probe failure directly in the compact check map.
				issues = appendIssueOnce(issues, "appium")                                           // Add the stable Appium issue key once for alerting and dashboards.
				details["appium"] = map[string]interface{}{"url": s.appiumURL, "error": err.Error()} // Preserve the failing Appium URL and error string for operator diagnosis.
				criticalFailure = true                                                               // Mark the health snapshot down because direct execution is unavailable.
			} else {
				checks["appium"] = "ok"                                        // Report a healthy Appium probe in the compact check map.
				details["appium"] = map[string]interface{}{"url": s.appiumURL} // Return the probed Appium URL for operator confirmation.
			}
		}
		checks["workers"] = "skipped"                                   // Skip worker-fleet checks in monolith mode because this service does not depend on remote workers for execution.
		details["workers"] = map[string]interface{}{"mode": "monolith"} // Return explicit metadata so operators can see why worker health was skipped.
	} else {
		summary := s.registry.HealthSummary()        // Snapshot the current in-memory worker registry counts so health output can describe available distributed capacity.
		details["workers"] = map[string]interface{}{ // Return the raw worker counts so operators can inspect healthy, degraded, and offline capacity directly.
			"total":    summary.Total,
			"healthy":  summary.Healthy,
			"degraded": summary.Degraded,
			"offline":  summary.Offline,
			"unknown":  summary.Unknown,
		}
		if summary.Healthy == 0 { // Treat zero healthy workers as a hard-down condition because distributed execution has no assignable capacity.
			checks["workers"] = "error"                 // Report the worker-fleet check as failed so clients can see execution capacity is unavailable.
			issues = appendIssueOnce(issues, "workers") // Add the stable worker-capacity issue key once for alerting and dashboards.
			criticalFailure = true                      // Mark the health snapshot down because no distributed worker can execute incoming plans.
		} else if summary.Degraded > 0 || summary.Offline > 0 || summary.Unknown > 0 { // Treat partial worker degradation as a degraded-but-still-available condition.
			checks["workers"] = "degraded"              // Report partial worker degradation directly in the compact check map.
			issues = appendIssueOnce(issues, "workers") // Add the stable worker-capacity issue key once because some capacity is impaired.
			degradedFailure = true                      // Mark the health snapshot degraded because execution capacity is partially impaired.
		} else {
			checks["workers"] = "ok" // Report a fully healthy worker fleet in the compact check map.
		}
		checks["appium"] = "skipped"                                      // Skip Appium probing in distributed mode because execution happens on remote workers instead.
		details["appium"] = map[string]interface{}{"mode": "distributed"} // Return explicit metadata so operators can see why local Appium probing was skipped.
	}

	if s.dispatcher == nil { // Treat a missing dispatcher as a hard-down condition because queued execution cannot proceed without it.
		checks["queue"] = "error"                 // Report the queue check as failed so clients can see the missing execution coordinator directly.
		issues = appendIssueOnce(issues, "queue") // Add the stable queue issue key once for alerting and dashboards.
		criticalFailure = true                    // Mark the health snapshot down because execution scheduling is unavailable.
	} else {
		probeCtx, cancel := withHealthProbeTimeout(ctx)      // Bound the queue inspection so the health endpoint never hangs indefinitely on Redis stream introspection.
		summary, err := s.dispatcher.HealthSummary(probeCtx) // Inspect retained queue depth, pending claims, and in-flight distributed traces through the live dispatcher.
		cancel()                                             // Release the probe timeout resources immediately after queue inspection finishes.
		if err != nil {                                      // Treat queue-inspection failure as degraded because Redis reachability is already reported separately above.
			checks["queue"] = "error"                                       // Report the queue inspection failure directly in the compact check map.
			issues = appendIssueOnce(issues, "queue")                       // Add the stable queue issue key once for alerting and dashboards.
			details["queue"] = map[string]interface{}{"error": err.Error()} // Preserve the exact queue inspection error string for operator diagnosis.
			degradedFailure = true                                          // Mark the health snapshot degraded because execution backlog visibility is impaired.
		} else {
			checks["queue"] = "ok"                     // Report successful queue inspection in the compact check map.
			details["queue"] = map[string]interface{}{ // Return the raw queue counters so operators can inspect backlog and in-flight activity directly.
				"shards":           summary.Shards,
				"retainedMessages": summary.RetainedMessages,
				"pendingMessages":  summary.PendingMessages,
				"inflightTraces":   summary.InflightTraces,
			}
		}
	}

	healthValue := 2.0   // Default the exported numeric health metric to the healthy value unless a failure classification overrides it below.
	if criticalFailure { // Let hard dependency failures win over degraded classifications because they represent actual execution unavailability.
		status = "down"   // Report the overall service status as down when a core dependency or execution path is unavailable.
		healthValue = 0.0 // Export the down metric value so Prometheus and alerts can distinguish it from degraded conditions.
	} else if degradedFailure { // Fall back to degraded when no critical dependency failed but some partial-capacity or auxiliary issue remains.
		status = "degraded" // Report the overall service status as degraded when execution still works but some capability is impaired.
		healthValue = 1.0   // Export the degraded metric value so Prometheus and alerts can distinguish it from healthy and down.
	}

	telemetry.HealthStatus.WithLabelValues("orchestrator").Set(healthValue) // Publish the numeric health classification for monitoring and alerting systems.
	return map[string]interface{}{                                          // Return the final structured health snapshot used by REST, JSON-RPC, and MCP health endpoints.
		"status":  status,
		"issues":  issues,
		"checks":  checks,
		"details": details,
	}
}

// withHealthProbeTimeout derives a short-lived child context used to bound one dependency probe inside the health endpoint.
func withHealthProbeTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, healthProbeTimeout) // Wrap the caller context with the fixed health-probe timeout so every dependency check fails fast consistently.
}

// appendIssueOnce appends one issue key only when it is not already present in the health issue list.
func appendIssueOnce(issues []string, issue string) []string {
	for _, existing := range issues { // Scan the existing issue slice so duplicate subsystem failures do not bloat the health response.
		if existing == issue { // Skip duplicates so each failing subsystem appears only once in the final issue list.
			return issues // Return the original slice unchanged because the requested issue key is already present.
		}
	}

	return append(issues, issue) // Append the new issue key once when it has not been recorded yet.
}
