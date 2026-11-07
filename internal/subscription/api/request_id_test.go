package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestIDEchoesClientValue(t *testing.T) {
	h, logs := newTestHandler(t, &fakeSubsService{})

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/subscriptions?email=alice%40example.com",
		nil,
	)
	req.Header.Set(requestIDHeader, "client-request-1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "client-request-1", w.Header().Get(requestIDHeader))
	require.Contains(t, logs.String(), "request_id=client-request-1")
}

func TestRequestIDGeneratedWhenMissing(t *testing.T) {
	h, logs := newTestHandler(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/api/subscriptions?email=alice%40example.com", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, w.Header().Get(requestIDHeader))
	require.Contains(t, logs.String(), "request_id=")
}

func TestInternalErrorsLogRequestID(t *testing.T) {
	svc := &fakeSubsService{createErr: errors.New("database unavailable")}
	h, logs := newTestHandler(t, svc)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/subscribe",
		strings.NewReader(`{"email":"alice@example.com","repo":"golang/go"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(requestIDHeader, "failure-request")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Equal(t, "failure-request", w.Header().Get(requestIDHeader))
	require.Contains(t, logs.String(), "request_id=failure-request")
	require.Contains(t, logs.String(), "database unavailable")
}
