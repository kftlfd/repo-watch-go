package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/subscription/api/docs"
)

const (
	requestIDHeader = "X-Request-ID"
	requestIDKey    = "request_id"
)

type subsService interface {
	ConfirmSubscription(ctx context.Context, token string) error
	CreateSubscription(ctx context.Context, email, repo string) error
	DeactivateSubscription(ctx context.Context, token string) error
	GetSubscriptionsList(ctx context.Context, email string) ([]string, error)
}

type HandlerDeps struct {
	Logger      *slog.Logger
	SubsService subsService
	Metrics     Metrics
	DocsHost    string
}

type handler struct {
	subsService subsService
	logger      *slog.Logger
}

func NewHandler(d HandlerDeps) http.Handler {
	docs.SwaggerInfo.Host = d.DocsHost

	g := gin.New()
	g.Use(observabilityMiddleware(d.Logger, d.Metrics))
	g.Use(gin.Recovery())

	h := &handler{
		subsService: d.SubsService,
		logger:      d.Logger,
	}

	registerApiRoutes(g.Group("/api"), h)

	return g
}

func observabilityMiddleware(logger *slog.Logger, m Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath()
		query := c.Request.URL.RawQuery

		requestID := strings.TrimSpace(c.GetHeader(requestIDHeader))
		if requestID == "" {
			requestID = newRequestID()
		}
		c.Set(requestIDKey, requestID)
		c.Header(requestIDHeader, requestID)

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

func requestID(c *gin.Context) string {
	return c.GetString(requestIDKey)
}

func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}

	return hex.EncodeToString(buf[:])
}

func registerApiRoutes(rg *gin.RouterGroup, h *handler) {
	rg.POST("/subscribe", h.handleCreateSubscription)

	rg.POST("/confirm/:token", h.handleConfirmSubscription)
	rg.GET("/confirm/:token", h.handleConfirmSubscription)

	rg.POST("/unsubscribe/:token", h.handleDeactivateSubscription)
	rg.GET("/unsubscribe/:token", h.handleDeactivateSubscription)

	rg.GET("/subscriptions", h.handleListSubscriptions)

	rg.GET("/docs", serveScalarDocs)
	rg.GET("/docs/openapi.json", serveOpenAPISpec)
}
