package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	TraceParentHeader = "traceparent"
	TraceStateHeader  = "tracestate"

	instrumentationName    = "repo-watch"
	defaultShutdownTimeout = 5 * time.Second
)

type TracingConfig struct {
	ServiceName     string
	ShutdownTimeout time.Duration
	ErrorHandler    func(error)
}

var propagator = propagation.TraceContext{}

// Installs the global tracer provider and propagator. Configures sampler and
// exporter from env, uses gRPC exporter if exporter endpoint is set in env.
func SetupTracing(ctx context.Context, cfg TracingConfig) (shutdown func() error, err error) {
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = func(error) {}
	}

	provider, err := cfg.buildProvider(ctx)
	if err != nil {
		return nil, fmt.Errorf("build provider: %w", err)
	}

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagator)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(cfg.ErrorHandler))

	shutdown = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		return provider.Shutdown(ctx)
	}

	return shutdown, nil
}

func (cfg *TracingConfig) buildProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(Attr.schemaURL(), Attr.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
	}

	sampler := os.Getenv("OTEL_TRACES_SAMPLER_ARG")
	samplerArg := os.Getenv("OTEL_TRACES_SAMPLER")
	opts = append(opts, sdktrace.WithSampler(getSampler(sampler, samplerArg)))

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	tracesEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if otlpEndpoint != "" || tracesEndpoint != "" {
		exporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("create exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}

	return sdktrace.NewTracerProvider(opts...), nil
}

func getSampler(sampler, samplerArg string) sdktrace.Sampler {
	ratio := 1.0
	if value := strings.TrimSpace(samplerArg); value != "" {
		if parsed, err := strconv.ParseFloat(value, 64); err == nil && parsed >= 0 && parsed <= 1 {
			ratio = parsed
		}
	}

	switch strings.ToLower(strings.TrimSpace(sampler)) {
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio)
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))

	default:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}
}

// Returns the global tracer
func Tracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}

// Get the W3C traceparent and tracestate for the active span in ctx.
// Both are empty when ctx does not contain a valid span.
func GetTraceParent(ctx context.Context) (traceParent, traceState string) {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get(TraceParentHeader), carrier.Get(TraceStateHeader)
}

// Returns a new context with the W3C trace context attached.
// The next span started from this context will be a child of traceParent.
func WithTraceParent(ctx context.Context, traceParent, traceState string) context.Context {
	traceParent = strings.TrimSpace(traceParent)
	if traceParent == "" {
		return ctx
	}

	carrier := propagation.MapCarrier{
		TraceParentHeader: traceParent,
	}
	if traceState = strings.TrimSpace(traceState); traceState != "" {
		carrier[TraceStateHeader] = traceState
	}

	return propagator.Extract(ctx, carrier)
}

// WithTraceLink links the next span to the traceParent span without making it
// the parent.
func WithTraceLink(traceParent string) trace.SpanStartOption {
	originCtx := WithTraceParent(context.Background(), traceParent, "")
	originSpanContext := trace.SpanContextFromContext(originCtx)
	if originSpanContext.IsValid() {
		return trace.WithLinks(trace.Link{SpanContext: originSpanContext})
	}
	return trace.WithLinks()
}

// TraceLogAttrs returns slog attributes for the active span.
func TraceLogAttrs(ctx context.Context) []any {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return nil
	}

	return []any{
		slog.String("trace_id", spanContext.TraceID().String()),
		slog.String("span_id", spanContext.SpanID().String()),
	}
}

type attr struct{}

var Attr attr

func (attr) schemaURL() string { return semconv.SchemaURL }
func (attr) ServiceName(name string) attribute.KeyValue {
	return semconv.ServiceName(name)
}

func (attr) CorrelationID(id string) attribute.KeyValue {
	return attribute.String("app.correlation_id", id)
}
func (attr) NotificationType(name string) attribute.KeyValue {
	return attribute.String("app.notification.type", name)
}
func (attr) RepositoryID(id string) attribute.KeyValue {
	return attribute.String("app.repository.id", id)
}
func (attr) RepositoryOwner(owner string) attribute.KeyValue {
	return attribute.String("app.repository.owner", owner)
}
func (attr) RepositoryName(name string) attribute.KeyValue {
	return attribute.String("app.repository.name", name)
}
func (attr) ReleaseTag(tag string) attribute.KeyValue {
	return attribute.String("app.release.tag", tag)
}
func (attr) GithubAttempt(attempt int) attribute.KeyValue {
	return attribute.Int("app.github.attempt", attempt)
}
func (attr) ScannerBatchSize(size int) attribute.KeyValue {
	return attribute.Int("app.scanner.batch_size", size)
}

func (attr) HTTPRequestMethod(method string) attribute.KeyValue {
	return semconv.HTTPRequestMethodKey.String(method)
}
func (attr) HTTPRoute(path string) attribute.KeyValue {
	return semconv.HTTPRoute(path)
}
func (attr) HTTPResponseStatus(status int) attribute.KeyValue {
	return semconv.HTTPResponseStatusCode(status)
}
func (attr) URLPath(path string) attribute.KeyValue {
	return semconv.URLPath(path)
}

func (attr) MessagingSystem(system string) attribute.KeyValue {
	return semconv.MessagingSystemKey.String(system)
}
func (attr) MessagingOpSend() attribute.KeyValue {
	return semconv.MessagingOperationTypeSend
}
func (attr) MessagingOpProcess() attribute.KeyValue {
	return semconv.MessagingOperationTypeProcess
}
func (attr) MessagingDestination(queueName string) attribute.KeyValue {
	return semconv.MessagingDestinationNameKey.String(queueName)
}
func (attr) MessagingEventID(eventID string) attribute.KeyValue {
	return semconv.MessagingMessageIDKey.String(eventID)
}
func (attr) MessagingEventType(eventType string) attribute.KeyValue {
	return attribute.String("app.event.type", eventType)
}
