package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
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

	handler := NewHandler(HandlerDeps{
		Logger:      logger,
		SubsService: d.SubsService,
	})

	srv := &http.Server{
		Addr:    d.Host + ":" + d.Port,
		Handler: handler,
	}

	return &server{
		logger:     logger,
		httpServer: srv,
	}
}

func (s *server) Start(ctx context.Context, cancelCtx context.CancelCauseFunc) {
	s.logger.Info("starting",
		slog.String("addr", s.httpServer.Addr),
	)

	errCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	defer wg.Wait()

	wg.Go(func() {
		errCh <- s.httpServer.ListenAndServe()
	})

	select {
	case err := <-errCh:
		s.logger.Error("unexpected exit", "error", err)
		cancelCtx(fmt.Errorf("server exit: %w", err))

	case <-ctx.Done():
		s.logger.Info("shutting down")
		err := s.httpServer.Shutdown(context.Background())
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("server shutdown error", "error", err)
		}
	}
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
