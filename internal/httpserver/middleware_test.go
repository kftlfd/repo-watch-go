package httpserver

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"repo-watch/internal/observability"
)

type fakeMetrics struct{}

func (fakeMetrics) HTTPRequest(string, string, int, time.Duration) {}

func newTestRouter(t *testing.T, register func(*gin.Engine)) (*gin.Engine, *bytes.Buffer) {
	t.Helper()

	gin.SetMode(gin.TestMode)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	router := gin.New()
	router.Use(ObservabilityMiddleware(logger, fakeMetrics{}))
	router.Use(Recovery())
	register(router)

	return router, logs
}

func TestObservabilityMiddlewarePropagatesCorrelationID(t *testing.T) {
	var got string

	router, _ := newTestRouter(t, func(r *gin.Engine) {
		r.GET("/correlation", func(c *gin.Context) {
			got = observability.GetCorrelationID(c.Request.Context())
			c.Status(http.StatusOK)
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/correlation", nil)
	req.Header.Set(RequestIDHeader, "client-request-1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "client-request-1", got)
}

func TestObservabilityMiddlewareLogsRecordedError(t *testing.T) {
	router, logs := newTestRouter(t, func(r *gin.Engine) {
		r.GET("/fail", func(c *gin.Context) {
			RecordError(c, "test operation", errors.New("boom"))
			c.Status(http.StatusInternalServerError)
		})
	})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/fail", nil))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, logs.String(), "level=ERROR")
	require.Contains(t, logs.String(), `msg="request failed"`)
	require.Contains(t, logs.String(), `operation="test operation"`)
	require.Contains(t, logs.String(), "boom")
}

func TestObservabilityMiddlewareLogsClientErrorAsWarning(t *testing.T) {
	router, logs := newTestRouter(t, func(r *gin.Engine) {
		r.GET("/bad", func(c *gin.Context) {
			c.Status(http.StatusBadRequest)
		})
	})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/bad", nil))

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, logs.String(), "level=WARN")
	require.Contains(t, logs.String(), "msg=request")
	require.NotContains(t, logs.String(), "request failed")
}

func TestRecoveryRecordsPanic(t *testing.T) {
	originalWriter := gin.DefaultErrorWriter
	gin.DefaultErrorWriter = io.Discard
	defer func() { gin.DefaultErrorWriter = originalWriter }()

	router, logs := newTestRouter(t, func(r *gin.Engine) {
		r.GET("/panic", func(*gin.Context) {
			panic("boom")
		})
	})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/panic", nil))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, logs.String(), "level=ERROR")
	require.Contains(t, logs.String(), "operation=panic")
	require.Contains(t, logs.String(), "boom")
}
