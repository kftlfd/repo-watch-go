package httpserver

import (
	"errors"
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/observability"
)

const (
	RequestIDHeader = "X-Request-ID"
)

func ObservabilityMiddleware(logger *slog.Logger, m Metrics) gin.HandlerFunc {
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

		c.Request = c.Request.WithContext(ctx)

		c.Next()

		duration := time.Since(start)
		status := c.Writer.Status()

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
		}

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
