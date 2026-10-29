package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type subsService interface {
	ConfirmSubscription(ctx context.Context, token string) error
	CreateSubscription(ctx context.Context, email, repo string) error
	DeactivateSubscription(ctx context.Context, token string) error
	GetSubscriptionsList(ctx context.Context, email string) ([]string, error)
}

type metrics interface {
	HTTPRequest(method, path string, status int, duration time.Duration)
}

type HandlerDeps struct {
	Logger      *slog.Logger
	SubsService subsService
	Metrics     metrics
}

type handler struct {
	subsService subsService
	logger      *slog.Logger
}

func NewHandler(d HandlerDeps) http.Handler {
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

func observabilityMiddleware(logger *slog.Logger, m metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath()
		query := c.Request.URL.RawQuery

		c.Next()

		duration := time.Since(start)

		attrs := []any{
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

func registerApiRoutes(rg *gin.RouterGroup, h *handler) {
	rg.POST("/subscribe", h.handleCreateSubscription)

	rg.POST("/confirm/:token", h.handleConfirmSubscription)
	rg.GET("/confirm/:token", h.handleConfirmSubscription)

	rg.POST("/unsubscribe/:token", h.handleDeactivateSubscription)
	rg.GET("/unsubscribe/:token", h.handleDeactivateSubscription)

	rg.GET("/subscriptions", h.handleListSubscriptions)
}
