package httpserver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

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
	router.Use(ObservabilityMiddleware(logger, fakeMetrics{}, false))
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

func TestObservabilityMiddlewareRedactsQueryValuesAndPathParameters(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		_ = provider.Shutdown(context.Background())
	})

	router, logs := newTestRouter(t, func(r *gin.Engine) {
		r.GET("/confirm/:token", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})
		r.GET("/api/subscriptions", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})
		r.NoRoute(func(c *gin.Context) {
			c.Status(http.StatusNotFound)
		})
	})

	requests := []struct {
		target string
		status int
	}{
		{target: "/confirm/secret-confirm-token?email=alice%40example.com", status: http.StatusOK},
		{target: "/api/subscriptions?email=alice%40example.com", status: http.StatusOK},
		{target: "/unmatched/secret-path-token?email=alice%40example.com", status: http.StatusNotFound},
	}

	for _, request := range requests {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, request.target, nil))
		require.Equal(t, request.status, w.Code)
	}

	logOutput := logs.String()
	require.NotContains(t, logOutput, "secret-confirm-token")
	require.NotContains(t, logOutput, "secret-path-token")
	require.NotContains(t, logOutput, "alice@example.com")
	require.Contains(t, logOutput, "path=/confirm/:token")
	require.Contains(t, logOutput, "path=/api/subscriptions")
	require.Contains(t, logOutput, "path=")
	require.Contains(t, logOutput, "query_keys=email")

	paths := make(map[string]string)
	for _, span := range recorder.Ended() {
		for _, attr := range span.Attributes() {
			if attr.Key == semconv.URLPathKey {
				paths[span.Name()] = attr.Value.AsString()
			}
		}
	}

	require.Equal(t, "/confirm/:token", paths["HTTP GET /confirm/:token"])
	require.Equal(t, "/api/subscriptions", paths["HTTP GET /api/subscriptions"])
	require.Equal(t, "unmatched", paths["HTTP GET unmatched"])
}

func TestRecoveryDoesNotDumpRequestSecrets(t *testing.T) {
	originalWriter := gin.DefaultErrorWriter
	originalMode := gin.Mode()
	gin.DefaultErrorWriter = io.Discard
	gin.SetMode(gin.DebugMode)
	t.Cleanup(func() {
		gin.DefaultErrorWriter = originalWriter
		gin.SetMode(originalMode)
	})

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	router := gin.New()
	router.Use(ObservabilityMiddleware(logger, fakeMetrics{}, false))
	router.Use(Recovery())
	router.GET("/panic", func(*gin.Context) {
		panic("boom")
	})

	req := httptest.NewRequest(http.MethodGet, "/panic?email=alice%40example.com", nil)
	req.AddCookie(&http.Cookie{Name: "rw_access", Value: "secret-cookie"})
	req.Header.Set("Authorization", "Bearer secret-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.NotContains(t, logs.String(), "alice@example.com")
	require.NotContains(t, logs.String(), "secret-cookie")
	require.NotContains(t, logs.String(), "secret-token")
}
