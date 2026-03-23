// progress.go provides MCP progress-notification context helpers shared by transports and long-running tool handlers.
package mcp

import "context"

// ProgressUpdate describes one outbound MCP progress notification payload for one in-flight tool call.
type ProgressUpdate struct {
	Token    interface{} `json:"progressToken"`     // Token carries the client-supplied correlation token that ties every notification back to one request.
	Progress int64       `json:"progress"`          // Progress reports the current completed-unit count for the long-running operation.
	Total    *int64      `json:"total,omitempty"`   // Total reports the optional expected total-unit count when the handler can determine it safely.
	Message  string      `json:"message,omitempty"` // Message carries one short human-readable status update suitable for client UIs and logs.
}

// ProgressReporter sends one asynchronous MCP progress notification through the active transport implementation.
type ProgressReporter func(context.Context, ProgressUpdate) error

// progressReporterContextKey isolates the stored progress reporter from unrelated context values.
type progressReporterContextKey struct{}

// progressTokenContextKey isolates the stored progress token from unrelated context values.
type progressTokenContextKey struct{}

// WithProgressReporter stores one transport-backed progress reporter on the request context.
func WithProgressReporter(ctx context.Context, reporter ProgressReporter) context.Context {
	return context.WithValue(ctx, progressReporterContextKey{}, reporter) // Persist the transport callback so long-running tool handlers can emit standardized MCP progress notifications.
}

// WithProgressToken stores one client-supplied progress token on the request context.
func WithProgressToken(ctx context.Context, token interface{}) context.Context {
	return context.WithValue(ctx, progressTokenContextKey{}, token) // Persist the request-scoped progress token so later notifications stay correlated to the originating request.
}

// ReportProgress emits one progress notification when both a reporter and token are available on the request context.
func ReportProgress(ctx context.Context, progress int64, total int64, message string) error {
	reporter, ok := ctx.Value(progressReporterContextKey{}).(ProgressReporter) // Load the transport-backed progress reporter so notifications can be emitted only when the active transport supports them.
	if !ok || reporter == nil {                                                // Skip emission silently when the active transport does not support MCP progress notifications.
		return nil // Return nil because missing progress support must not break the underlying tool execution path.
	}
	token, ok := ctx.Value(progressTokenContextKey{}).(interface{}) // Load the request-scoped progress token so emitted notifications can be correlated by the client.
	if !ok || token == nil {                                        // Skip emission silently when the client did not ask for progress notifications on this request.
		return nil // Return nil because progress notifications are optional from the tool handler perspective.
	}
	var totalValue *int64 // Allocate the optional total pointer lazily so zero or unknown totals omit the field cleanly.
	if total > 0 {        // Populate total only when the handler has one meaningful step count to expose.
		totalCopy := total      // Copy the total count so the notification can keep one stable pointer independent of later local mutations.
		totalValue = &totalCopy // Store the stable pointer so JSON omits the field automatically when no total exists.
	}
	return reporter(ctx, ProgressUpdate{Token: token, Progress: progress, Total: totalValue, Message: message}) // Emit one standardized progress notification through the active transport.
}
