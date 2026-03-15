package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"mcp_for_appium/internal/util"

	"go.opentelemetry.io/otel/trace"
)

var defaultLogger *slog.Logger
var stdLogWriter io.Writer = os.Stderr
var logFileHandle *os.File
var logWriterMu sync.Mutex

// init executes this operation.
func init() {
	// Default to text handler for dev, can be changed to JSON
	defaultLogger = slog.New(slog.NewJSONHandler(os.Stdout, nil)) // Keep stdout logging by default before config is loaded.
	slog.SetDefault(defaultLogger)                                 // Register the default logger globally.
	stdLogWriter = os.Stderr                                       // Route standard-library log package output to stderr by default.
}

// InitLogger initializes the global logger with the specified level and format.
func InitLogger(level string, logFilePath string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug // Enable debug and above log records.
	case "warn":
		l = slog.LevelWarn // Enable warning and above log records.
	case "error":
		l = slog.LevelError // Enable error and above log records.
	default:
		l = slog.LevelInfo // Default to info-level output.
	}

	writer := io.Writer(os.Stdout)                                 // Always keep console output for local troubleshooting.
	stdWriter := io.Writer(os.Stderr)                              // Keep standard log package bound to stderr for stdio safety.
	if resolvedFileWriter, err := buildLogFileWriter(logFilePath); err == nil && resolvedFileWriter != nil { // Attempt file logging only when configured and creatable.
		writer = io.MultiWriter(os.Stdout, resolvedFileWriter)     // Mirror structured logs to both stdout and file.
		stdWriter = io.MultiWriter(os.Stderr, resolvedFileWriter)   // Mirror stdlib logs to both stderr and file.
	}

	opts := &slog.HandlerOptions{
		Level: l, // Apply requested minimum log level to the handler.
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Mask sensitive values in top-level attributes
			if a.Value.Kind() == slog.KindString {
				a.Value = slog.StringValue(util.MaskSecrets(a.Value.String())) // Redact common secret patterns before writing logs.
			}
			return a // Return transformed attribute for final emission.
		},
	}

	handler := slog.NewJSONHandler(writer, opts) // Emit JSON logs to configured multi-destination writer.
	defaultLogger = slog.New(handler)            // Replace process-global logger with configured logger.
	slog.SetDefault(defaultLogger)               // Register updated logger as global default.
	setStdLogWriter(stdWriter)                   // Update stdlib log writer so stdio transport logs are persisted too.
}

// Logger returns the default logger.
func Logger() *slog.Logger {
	return defaultLogger // Return shared process logger instance.
}

// WithContext returns a logger enriched with traceId and spanId from the
// OpenTelemetry span in ctx, if one is present and valid.
func WithContext(ctx context.Context) *slog.Logger {
	span := trace.SpanFromContext(ctx) // Read active span from context when present.
	sc := span.SpanContext()           // Extract trace/span identifiers for correlation logging.
	if !sc.IsValid() {
		return defaultLogger // Fall back to base logger when no valid span context exists.
	}
	return defaultLogger.With(
		"trace_id", sc.TraceID().String(), // Attach trace id for request-level correlation.
		"span_id", sc.SpanID().String(),   // Attach span id for operation-level correlation.
	) // Return enriched logger with tracing metadata.
}

// StdLogWriter executes this operation.
func StdLogWriter() io.Writer {
	logWriterMu.Lock()         // Protect shared writer read against concurrent reconfiguration.
	defer logWriterMu.Unlock() // Ensure mutex is always released.
	return stdLogWriter        // Return current stdlib log writer destination.
}

// setStdLogWriter executes this operation.
func setStdLogWriter(w io.Writer) {
	logWriterMu.Lock()         // Guard shared writer mutation.
	defer logWriterMu.Unlock() // Ensure lock release after mutation.
	stdLogWriter = w           // Install new stdlib log destination.
}

// buildLogFileWriter executes this operation.
func buildLogFileWriter(logFilePath string) (io.Writer, error) {
	trimmedPath := filepath.Clean(logFilePath) // Normalize configured path before filesystem operations.
	if trimmedPath == "." || trimmedPath == "" { // Treat empty configuration as no file logging.
		return nil, nil // Skip file writer creation when path is not configured.
	}
	logWriterMu.Lock() // Serialize file handle updates.
	defer logWriterMu.Unlock() // Ensure lock release on every return path.
	if logFileHandle != nil { // Close previous file handle before reconfiguring to avoid descriptor leaks.
		_ = logFileHandle.Close() // Best-effort close; new handle creation result determines next writer state.
		logFileHandle = nil // Clear stale handle regardless of close result.
	}
	if err := os.MkdirAll(filepath.Dir(trimmedPath), 0o755); err != nil { // Ensure parent directory exists for log file.
		return nil, err // Propagate directory creation errors to caller.
	}
	f, err := os.OpenFile(trimmedPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644) // Open file in append mode to preserve history.
	if err != nil { // Handle open failure without breaking process startup.
		return nil, err // Return error so caller can keep console-only logging.
	}
	logFileHandle = f // Retain handle for future reconfiguration close.
	return f, nil // Return file writer for multi-writer composition.
}
