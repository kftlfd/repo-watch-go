package httpserver

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"repo-watch/internal/observability"
)

const (
	RequestIDHeader = "X-Request-ID"
)

func ObservabilityMiddleware(logger *slog.Logger, m Metrics, allowTraceparent bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		start := time.Now()
		path := c.FullPath()
		query := c.Request.URL.RawQuery

		requestID := observability.NormalizeCorrelationID(c.GetHeader(RequestIDHeader))
		if requestID == "" {
			requestID = observability.NewCorrelationID()
		}
		c.Header(RequestIDHeader, requestID)
		ctx = observability.WithCorrelationID(ctx, requestID)

		if allowTraceparent {
			ctx = observability.WithTraceParent(ctx,
				c.GetHeader(observability.TraceParentHeader),
				c.GetHeader(observability.TraceStateHeader),
			)
		}

		ctx, span := observability.Tracer().Start(ctx, strings.TrimSpace(fmt.Sprintf("HTTP %v %v", c.Request.Method, path)),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				observability.Attr.HTTPRequestMethod(c.Request.Method),
				observability.Attr.HTTPRoute(path),
				observability.Attr.CorrelationID(requestID),
			),
		)
		defer span.End()

		c.Request = c.Request.WithContext(ctx)

		c.Next()

		duration := time.Since(start)
		status := c.Writer.Status()

		span.SetAttributes(observability.Attr.HTTPResponseStatus(status))
		if status >= 500 {
			span.SetStatus(codes.Error, "server error")
		}

		attrs := []any{
			slog.String("request_id", requestID),
			slog.String("correlation_id", requestID),
			slog.String("method", c.Request.Method),
			slog.String("path", path),
			slog.String("query", query),
			slog.Int("status", status),
			slog.String("latency", duration.String()),
			// slog.String("client_ip", c.ClientIP()),
			// slog.Int("body_size", c.Writer.Size()),
		}

		hasError := len(c.Errors) > 0
		if last := c.Errors.Last(); last != nil {
			if reqErr, ok := errors.AsType[*RequestError](last.Err); ok {
				attrs = append(attrs,
					slog.String("operation", reqErr.Operation),
					slog.Any("error", reqErr.Err),
				)
			} else {
				attrs = append(attrs, slog.Any("error", last.Err))
			}

			span.RecordError(last.Err)
			span.SetStatus(codes.Error, last.Err.Error())
		}

		attrs = append(attrs, observability.TraceLogAttrs(ctx)...)

		m.HTTPRequest(c.Request.Method, path, status, duration)

		switch {
		case hasError || status >= 500:
			logger.Error("request failed", attrs...)
		case status >= 400:
			logger.Warn("request", attrs...)
		default:
			logger.Info("request", attrs...)
		}
	}
}
