package telemetry

import (
	"context"
	"log/slog"
	"os"

	"mcp_for_appium/internal/util"
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

// WithContext returns a logger with context fields (e.g. traceId) if present.
// For now, it just returns the default logger as slog doesn't automatically extract from context without helper.
func WithContext(ctx context.Context) *slog.Logger {
	// TODO: Extract traceId/requestId from context and add to logger
	return defaultLogger
}
