package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
)

type ServerDeps struct {
	Logger     *slog.Logger
	Handler    http.Handler
	Host, Port string
}

type server struct {
	logger *slog.Logger
	srv    *http.Server
}

func NewServer(d ServerDeps) *server {
	srv := &http.Server{
		Addr:    d.Host + ":" + d.Port,
		Handler: d.Handler,
	}

	return &server{
		logger: d.Logger,
		srv:    srv,
	}
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
