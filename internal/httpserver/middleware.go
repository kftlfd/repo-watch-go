package httpserver

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	RequestIDHeader = "X-Request-ID"
	requestIDKey    = "request_id"
)

func ObservabilityMiddleware(logger *slog.Logger, m Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath()
		query := c.Request.URL.RawQuery

		requestID := strings.TrimSpace(c.GetHeader(RequestIDHeader))
		if requestID == "" {
			requestID = newRequestID()
		}
		c.Set(requestIDKey, requestID)
		c.Header(RequestIDHeader, requestID)

		c.Next()

		duration := time.Since(start)
		status := c.Writer.Status()

		attrs := []any{
			slog.String("request_id", requestID),
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

func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}

	return hex.EncodeToString(buf[:])
}
