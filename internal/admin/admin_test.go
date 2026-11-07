package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestReadyReportsDependencyFailure(t *testing.T) {
	handler := NewHandler(Deps{
		Metrics: prometheus.NewRegistry(),
		Ready: func(context.Context) error {
			return errors.New("rabbitmq is not connected")
		},
	})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "rabbitmq is not connected")
}

func TestReadyWithoutChecker(t *testing.T) {
	handler := NewHandler(Deps{Metrics: prometheus.NewRegistry()})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	require.Equal(t, http.StatusOK, rec.Code)
}
