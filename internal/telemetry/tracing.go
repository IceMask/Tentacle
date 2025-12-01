package telemetry

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// InitTracer initializes the global trace provider.
func InitTracer(serviceName string, endpoint string) func(context.Context) error {
	// For this implementation, we'll use a stdout exporter or no-op if endpoint is empty
	// In production, you'd configure OTLP exporter here.

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		log.Printf("failed to create resource: %v", err)
		return func(context.Context) error { return nil }
	}

	// TODO: Add OTLP exporter if endpoint is provided
	// exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.25))), // 25% sampling
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp.Shutdown
}
