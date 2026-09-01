package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
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

func (s *server) Start(ctx context.Context, cancelCtx context.CancelCauseFunc) {
	s.logger.Info("starting server",
		slog.String("addr", s.srv.Addr),
	)

	errCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	defer wg.Wait()

	wg.Go(func() {
		errCh <- s.srv.ListenAndServe()
	})

	select {
	case err := <-errCh:
		s.logger.Error("unexpected exit", "error", err)
		cancelCtx(fmt.Errorf("server exit: %w", err))

	case <-ctx.Done():
		s.logger.Info("shutting down")
		err := s.srv.Shutdown(context.Background())
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("server shutdown error", "error", err)
		}
	}
}
