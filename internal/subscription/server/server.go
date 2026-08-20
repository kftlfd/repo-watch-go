package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Deps struct {
	Logger *slog.Logger
}

type server struct {
	logger *slog.Logger
	server *http.Server
}

func New(d Deps) *server {
	return &server{
		logger: d.Logger.With(
			slog.String("module", "server"),
		),
		server: &http.Server{},
	}
}

func (s *server) Start() error {
	s.logger.Info("server starting")

	err := s.server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

func (s *server) Shutdown() error {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.server.Shutdown(timeoutCtx); err != nil {
		return fmt.Errorf("server shutdown error: %w", err)
	}

	return nil
}
