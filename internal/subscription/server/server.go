package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"repo-watch/internal/subscription/subs"

	"github.com/gin-gonic/gin"
)

type ServerDeps struct {
	Logger      *slog.Logger
	SubsService subs.SubsService
	Host, Port  string
}

type server struct {
	logger     *slog.Logger
	httpServer *http.Server
}

func NewServer(d ServerDeps) *server {
	logger := d.Logger.WithGroup("server")

	return &server{
		logger: logger,
		httpServer: &http.Server{
			Addr: d.Host + ":" + d.Port,
			Handler: NewHandler(HandlerDeps{
				Logger:      logger,
				SubsService: d.SubsService,
			}),
		},
	}
}

func (s *server) Start() error {
	s.logger.Info("server starting",
		slog.String("addr", s.httpServer.Addr),
	)

	err := s.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

func (s *server) Shutdown() error {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(timeoutCtx); err != nil {
		return fmt.Errorf("server shutdown error: %w", err)
	}

	return nil
}

type HandlerDeps struct {
	Logger      *slog.Logger
	SubsService subs.SubsService
}

type handler struct {
	logger  *slog.Logger
	service subs.SubsService
	engine  *gin.Engine
}

func NewHandler(d HandlerDeps) http.Handler {
	engine := gin.New()
	engine.Use(slogMiddleware(d.Logger))
	engine.Use(gin.Recovery())

	s := &handler{
		logger:  d.Logger,
		service: d.SubsService,
		engine:  engine,
	}

	s.registerRoutes()

	return engine
}

func (h *handler) registerRoutes() {
	root := h.engine.Group("")
	root.GET("/healthz", h.handleHealth)
	root.GET("/ready", h.handleReady)

	api := h.engine.Group("/api")
	api.POST("/subscribe", h.handleCreateSubscription)
	api.POST("/confirm/:token", h.handleConfirmSubscription)
	api.POST("/unsubscribe/:token", h.handleDeactivateSubscription)
	api.GET("/subscriptions", h.handleListSubscriptions)
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
