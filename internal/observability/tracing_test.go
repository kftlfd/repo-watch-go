package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	testTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	testTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID      = "00f067aa0ba902b7"
)

func newTestTracer(t *testing.T) (trace.Tracer, *tracetest.SpanRecorder) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	return provider.Tracer("test"), recorder
}

func TestWithTraceParentContinuesRemoteTrace(t *testing.T) {
	tracer, recorder := newTestTracer(t)

	ctx := WithTraceParent(context.Background(), testTraceParent, "vendor=value")

	_, span := tracer.Start(ctx, "child")
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	spanContext := spans[0].SpanContext()
	require.Equal(t, testTraceID, spanContext.TraceID().String())
	require.NotEqual(t, testSpanID, spanContext.SpanID().String())

	parent := spans[0].Parent()
	require.True(t, parent.IsRemote())
	require.Equal(t, testTraceID, parent.TraceID().String())
	require.Equal(t, testSpanID, parent.SpanID().String())
	require.Equal(t, "vendor=value", parent.TraceState().String())
}

func TestWithTraceParentIgnoresMissingOrInvalid(t *testing.T) {
	base := context.Background()

	require.Equal(t, base, WithTraceParent(base, "", "vendor=value"))
	require.False(t, trace.SpanContextFromContext(WithTraceParent(base, "invalid", "")).IsValid())
}

func TestGetTraceParentReturnsTraceState(t *testing.T) {
	ctx := WithTraceParent(context.Background(), testTraceParent, "vendor=value")

	traceParent, traceState := GetTraceParent(ctx)

	require.Equal(t, testTraceParent, traceParent)
	require.Equal(t, "vendor=value", traceState)
}

func TestGetTraceParentWithoutSpanIsEmpty(t *testing.T) {
	traceParent, traceState := GetTraceParent(context.Background())

	require.Empty(t, traceParent)
	require.Empty(t, traceState)
}

func TestWithTraceLinkStartsNewTraceWithLink(t *testing.T) {
	tracer, recorder := newTestTracer(t)

	_, span := tracer.Start(context.Background(), "publish",
		trace.WithNewRoot(),
		WithTraceLink(testTraceParent),
	)
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	spanContext := spans[0].SpanContext()
	require.NotEqual(t, testTraceID, spanContext.TraceID().String())
	require.False(t, spans[0].Parent().IsValid())

	links := spans[0].Links()
	require.Len(t, links, 1)
	require.Equal(t, testTraceID, links[0].SpanContext.TraceID().String())
	require.Equal(t, testSpanID, links[0].SpanContext.SpanID().String())
}

func TestWithTraceLinkWithoutTraceParentAddsNoLink(t *testing.T) {
	tracer, recorder := newTestTracer(t)

	_, span := tracer.Start(context.Background(), "publish",
		trace.WithNewRoot(),
		WithTraceLink(""),
	)
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Empty(t, spans[0].Links())
}
