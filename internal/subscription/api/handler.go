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

type HandlerDeps struct {
	Logger      *slog.Logger
	SubsService subsService
}

type handler struct {
	subsService subsService
}

func NewHandler(d HandlerDeps) http.Handler {
	g := gin.New()
	g.Use(slogMiddleware(d.Logger))
	g.Use(gin.Recovery())

	h := &handler{
		subsService: d.SubsService,
	}

	registerApiRoutes(g.Group("/api"), h)

	return g
}

func slogMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		attrs := []any{
			slog.String("method", c.Request.Method),
			slog.String("path", path),
			slog.String("query", query),
			slog.Int("status", c.Writer.Status()),
			slog.Duration("latency", time.Since(start)),
			// slog.String("client_ip", c.ClientIP()),
			// slog.Int("body_size", c.Writer.Size()),
		}

		if len(c.Errors) > 0 {
			attrs = append(attrs, slog.Any("error", c.Errors.Errors()))
		}

		logger.Info("request", attrs...)
	}
}

func registerApiRoutes(rg *gin.RouterGroup, h *handler) {
	rg.POST("/subscribe", h.handleCreateSubscription)
	rg.POST("/confirm/:token", h.handleConfirmSubscription)
	rg.POST("/unsubscribe/:token", h.handleDeactivateSubscription)
	rg.GET("/subscriptions", h.handleListSubscriptions)
}
