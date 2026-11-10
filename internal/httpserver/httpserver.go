package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

const (
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 15 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 60 * time.Second
	defaultMaxBodyBytes      = 1 << 20 // 1MB
)

type ServerDeps struct {
	Logger     *slog.Logger
	Handler    http.Handler
	Host, Port string

	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxBodyBytes      int64
}

type server struct {
	logger *slog.Logger
	srv    *http.Server
}

func NewServer(d ServerDeps) *server {
	srv := &http.Server{
		Addr:              d.Host + ":" + d.Port,
		Handler:           limitRequestBody(d.MaxBodyBytes, d.Handler),
		ReadHeaderTimeout: timeoutOr(d.ReadHeaderTimeout, defaultReadHeaderTimeout),
		ReadTimeout:       timeoutOr(d.ReadTimeout, defaultReadTimeout),
		WriteTimeout:      timeoutOr(d.WriteTimeout, defaultWriteTimeout),
		IdleTimeout:       timeoutOr(d.IdleTimeout, defaultIdleTimeout),
	}

	return &server{
		logger: d.Logger,
		srv:    srv,
	}
}

// LimitRequestBody rejects reads beyond maxBytes with http.MaxBytesError.
// Handlers can use errors.As with *http.MaxBytesError to return a clear 413.
func limitRequestBody(maxBytes int64, next http.Handler) http.Handler {
	if maxBytes <= 0 {
		maxBytes = defaultMaxBodyBytes
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func timeoutOr(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}

func (s *server) Start(ctx context.Context) error {
	s.logger.Info("starting server",
		slog.String("addr", s.srv.Addr),
	)

	errCh := make(chan error, 1)

	go func() {
		errCh <- s.srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		s.logger.Error("unexpected exit", "error", err)
		return fmt.Errorf("server error: %w", err)

	case <-ctx.Done():
		s.logger.Info("shutting down")
		if err := s.srv.Shutdown(context.Background()); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("server shutdown error")
		}
	}

	return nil
}
