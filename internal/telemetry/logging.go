package telemetry

import (
	"context"
	"log/slog"
	"os"

	"mcp_for_appium/internal/util"

	"go.opentelemetry.io/otel/trace"
)

var defaultLogger *slog.Logger

func init() {
	// Default to text handler for dev, can be changed to JSON
	defaultLogger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(defaultLogger)
}

// InitLogger initializes the global logger with the specified level and format.
func InitLogger(level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: l,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Mask sensitive values in top-level attributes
			if a.Value.Kind() == slog.KindString {
				a.Value = slog.StringValue(util.MaskSecrets(a.Value.String()))
			}
			return a
		},
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	defaultLogger = slog.New(handler)
	slog.SetDefault(defaultLogger)
}

// Logger returns the default logger.
func Logger() *slog.Logger {
	return defaultLogger
}

// WithContext returns a logger enriched with traceId and spanId from the
// OpenTelemetry span in ctx, if one is present and valid.
func WithContext(ctx context.Context) *slog.Logger {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()
	if !sc.IsValid() {
		return defaultLogger
	}
	return defaultLogger.With(
		"trace_id", sc.TraceID().String(),
		"span_id", sc.SpanID().String(),
	)
}
