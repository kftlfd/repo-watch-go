package httpserver

import (
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"repo-watch/internal/observability"
)

const (
	RequestIDHeader = "X-Request-ID"
)

// ObservabilityConfig controls request metadata accepted from clients and the
// fixed query-key names that are safe to include in access logs.
type ObservabilityConfig struct {
	AllowTraceparent bool
	AllowedQueryKeys []string
}

func ObservabilityMiddleware(logger *slog.Logger, m Metrics, cfg ObservabilityConfig) gin.HandlerFunc {
	allowedQueryKeys := make(map[string]struct{}, len(cfg.AllowedQueryKeys))
	for _, key := range cfg.AllowedQueryKeys {
		allowedQueryKeys[key] = struct{}{}
	}

	return func(c *gin.Context) {
		ctx := c.Request.Context()
		start := time.Now()
		route := c.FullPath()
		var queryKeys []string
		for key := range c.Request.URL.Query() {
			if _, ok := allowedQueryKeys[key]; ok {
				queryKeys = append(queryKeys, key)
			}
		}
		slices.Sort(queryKeys)

		requestID := observability.NormalizeCorrelationID(c.GetHeader(RequestIDHeader))
		if requestID == "" {
			requestID = observability.NewCorrelationID()
		}
		c.Header(RequestIDHeader, requestID)
		ctx = observability.WithCorrelationID(ctx, requestID)

		if cfg.AllowTraceparent {
			ctx = observability.WithTraceParent(ctx,
				c.GetHeader(observability.TraceParentHeader),
				c.GetHeader(observability.TraceStateHeader),
			)
		}

		spanName := "HTTP " + c.Request.Method
		spanAttrs := []attribute.KeyValue{
			observability.Attr.HTTPRequestMethod(c.Request.Method),
			observability.Attr.CorrelationID(requestID),
		}
		if route != "" {
			spanName += " " + route
			spanAttrs = append(spanAttrs,
				observability.Attr.URLPath(route),
				observability.Attr.HTTPRoute(route),
			)
		} else {
			spanName += " unmatched"
			spanAttrs = append(spanAttrs, observability.Attr.URLPath("unmatched"))
		}
		ctx, span := observability.Tracer().Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(spanAttrs...),
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
			slog.String("path", route),
			slog.Int("status", status),
			slog.String("latency", duration.String()),
			// slog.String("client_ip", c.ClientIP()),
			// slog.Int("body_size", c.Writer.Size()),
		}
		if len(queryKeys) > 0 {
			attrs = append(attrs, slog.String("query_keys", strings.Join(queryKeys, ",")))
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

		m.HTTPRequest(c.Request.Method, route, status, duration)

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
