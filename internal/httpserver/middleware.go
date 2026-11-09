package httpserver

import (
	"crypto/rand"
	"encoding/hex"
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

		attrs := []any{
			slog.String("request_id", requestID),
			slog.String("method", c.Request.Method),
			slog.String("path", path),
			slog.String("query", query),
			slog.Int("status", c.Writer.Status()),
			slog.Duration("latency", duration),
			// slog.String("client_ip", c.ClientIP()),
			// slog.Int("body_size", c.Writer.Size()),
		}

		if len(c.Errors) > 0 {
			attrs = append(attrs, slog.Any("error", c.Errors.Errors()))
		}

		m.HTTPRequest(c.Request.Method, c.FullPath(), c.Writer.Status(), duration)

		logger.Info("request", attrs...)
	}
}

func RequestID(c *gin.Context) string {
	return c.GetString(requestIDKey)
}

func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}

	return hex.EncodeToString(buf[:])
}
