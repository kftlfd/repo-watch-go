package web

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"repo-watch/internal/httpserver"
	"repo-watch/internal/subscription/subs"
)

const validToken = "AAAAAAAAAAAAAAAAAAAAAA"

type fakeSubsService struct {
	createErr       error
	createEmail     string
	createRepo      string
	confirmErr      error
	confirmToken    string
	deactivateErr   error
	deactivateToken string
}

func (f *fakeSubsService) CreateSubscription(_ context.Context, email, repo string) error {
	f.createEmail = email
	f.createRepo = repo
	return f.createErr
}

func (f *fakeSubsService) ConfirmSubscription(_ context.Context, token string) error {
	f.confirmToken = token
	return f.confirmErr
}

func (f *fakeSubsService) DeactivateSubscription(_ context.Context, token string) error {
	f.deactivateToken = token
	return f.deactivateErr
}

type fakeMetrics struct{}

func (fakeMetrics) HTTPRequest(string, string, int, time.Duration) {}

func newTestRouter(t *testing.T, svc subsService) (http.Handler, *bytes.Buffer) {
	t.Helper()

	gin.SetMode(gin.TestMode)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	router := gin.New()
	router.Use(httpserver.ObservabilityMiddleware(logger, fakeMetrics{}))
	router.Use(httpserver.Recovery())

	RegisterRoutes(router.Group("/"), Deps{
		SubsService: svc,
	})
	router.NoRoute(NotFound)

	return router, logs
}

func doRequest(t *testing.T, h http.Handler, method, target, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, target, reader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	return w
}

func TestSubscribePage(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestRouter(t, svc)

	w := doRequest(t, h, http.MethodGet, "/", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `action="/subscribe"`)
	require.Contains(t, w.Body.String(), `name="email"`)
	require.Contains(t, w.Body.String(), `name="repo"`)
	require.Empty(t, svc.createEmail)
}

func TestSubscribeSuccess(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestRouter(t, svc)

	form := url.Values{
		"email": {"alice@example.com"},
		"repo":  {"golang/go"},
	}.Encode()

	w := doRequest(t, h, http.MethodPost, "/subscribe", "application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), msgCheckEmail)
	require.Equal(t, "alice@example.com", svc.createEmail)
	require.Equal(t, "golang/go", svc.createRepo)
}

func TestSubscribeRejectsInvalidForm(t *testing.T) {
	tests := []struct {
		name string
		form url.Values
	}{
		{
			name: "missing repo",
			form: url.Values{"email": {"alice@example.com"}},
		},
		{
			name: "invalid email",
			form: url.Values{"email": {"not-an-email"}, "repo": {"golang/go"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{}
			h, _ := newTestRouter(t, svc)

			w := doRequest(t, h, http.MethodPost, "/subscribe",
				"application/x-www-form-urlencoded", tt.form.Encode())

			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), msgInvalidInput)
			require.Empty(t, svc.createEmail)
		})
	}
}

func TestSubscribeMapsServiceErrors(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantStatus   int
		wantBody     string
		wantErrorLog bool
	}{
		{
			name:       "invalid input",
			err:        subs.ErrInvalidInput,
			wantStatus: http.StatusBadRequest,
			wantBody:   msgInvalidInput,
		},
		{
			name:       "repository not found",
			err:        subs.ErrRepoNotFound,
			wantStatus: http.StatusNotFound,
			wantBody:   "That repository was not found on GitHub.",
		},
		{
			name:       "subscription conflict",
			err:        subs.ErrSubscriptionConflict,
			wantStatus: http.StatusConflict,
			wantBody:   "You already have an active subscription to this repository.",
		},
		{
			name:       "rate limit",
			err:        subs.ErrRateLimit,
			wantStatus: http.StatusTooManyRequests,
			wantBody:   "GitHub rate limit exceeded. Please try again later.",
		},
		{
			name:         "upstream",
			err:          subs.ErrUpstreamError,
			wantStatus:   http.StatusBadGateway,
			wantBody:     "GitHub is unavailable right now. Please try again later.",
			wantErrorLog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{createErr: tt.err}
			h, logs := newTestRouter(t, svc)

			form := url.Values{
				"email": {"alice@example.com"},
				"repo":  {"golang/go"},
			}.Encode()

			w := doRequest(t, h, http.MethodPost, "/subscribe",
				"application/x-www-form-urlencoded", form)

			require.Equal(t, tt.wantStatus, w.Code)
			require.Contains(t, w.Body.String(), tt.wantBody)
			if tt.wantErrorLog {
				require.Contains(t, logs.String(), "request failed")
			} else {
				require.NotContains(t, logs.String(), "request failed")
			}
		})
	}
}

func TestSubscribeHidesInternalErrors(t *testing.T) {
	svc := &fakeSubsService{createErr: errors.New("secret database failure")}
	h, logs := newTestRouter(t, svc)

	form := url.Values{
		"email": {"alice@example.com"},
		"repo":  {"golang/go"},
	}.Encode()

	w := doRequest(t, h, http.MethodPost, "/subscribe",
		"application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, w.Body.String(), msgInternal)
	require.NotContains(t, w.Body.String(), "secret database failure")

	require.Contains(t, logs.String(), "secret database failure")
	require.Contains(t, logs.String(), "operation=\"create subscription\"")
	require.Contains(t, logs.String(), "path=/subscribe")
}

func TestSubscribeEscapesFormValues(t *testing.T) {
	svc := &fakeSubsService{createErr: subs.ErrRepoNotFound}
	h, _ := newTestRouter(t, svc)

	form := url.Values{
		"email": {"alice@example.com"},
		"repo":  {`<script>alert("x")</script>`},
	}.Encode()

	w := doRequest(t, h, http.MethodPost, "/subscribe",
		"application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusNotFound, w.Code)
	require.NotContains(t, w.Body.String(), `<script>alert`)
	require.Contains(t, w.Body.String(), "&lt;script&gt;")
}

func TestConfirmPage(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestRouter(t, svc)

	w := doRequest(t, h, http.MethodGet, "/confirm/"+validToken, "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "Confirm subscription")
	require.Empty(t, svc.confirmToken)
}

func TestUnsubscribePage(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestRouter(t, svc)

	w := doRequest(t, h, http.MethodGet, "/unsubscribe/"+validToken, "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "Unsubscribe")
	require.Empty(t, svc.deactivateToken)
}

func TestMalformedToken(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "confirm page", method: http.MethodGet, path: "/confirm/not-a-token"},
		{name: "confirm action", method: http.MethodPost, path: "/confirm/not-a-token"},
		{name: "unsubscribe page", method: http.MethodGet, path: "/unsubscribe/not-a-token"},
		{name: "unsubscribe action", method: http.MethodPost, path: "/unsubscribe/not-a-token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{}
			h, _ := newTestRouter(t, svc)

			w := doRequest(t, h, tt.method, tt.path, "", "")

			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), msgInvalidToken)
			require.Empty(t, svc.confirmToken)
			require.Empty(t, svc.deactivateToken)
		})
	}
}

func TestConfirmSubscription(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestRouter(t, svc)

	w := doRequest(t, h, http.MethodPost, "/confirm/"+validToken, "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), msgConfirmed)
	require.Equal(t, validToken, svc.confirmToken)
}

func TestDeactivateSubscription(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestRouter(t, svc)

	w := doRequest(t, h, http.MethodPost, "/unsubscribe/"+validToken, "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), msgUnsubscribed)
	require.Equal(t, validToken, svc.deactivateToken)
}

func TestTokenErrors(t *testing.T) {
	t.Run("unknown token", func(t *testing.T) {
		svc := &fakeSubsService{
			confirmErr:    sql.ErrNoRows,
			deactivateErr: sql.ErrNoRows,
		}
		h, _ := newTestRouter(t, svc)

		for _, path := range []string{"/confirm/" + validToken, "/unsubscribe/" + validToken} {
			w := doRequest(t, h, http.MethodPost, path, "", "")
			require.Equal(t, http.StatusNotFound, w.Code)
			require.Contains(t, w.Body.String(), msgTokenUsed)
		}
	})

	t.Run("already settled", func(t *testing.T) {
		svc := &fakeSubsService{
			confirmErr:    subs.ErrSubscriptionAlreadyActive,
			deactivateErr: subs.ErrSubscriptionAlreadyInactive,
		}
		h, _ := newTestRouter(t, svc)

		w := doRequest(t, h, http.MethodPost, "/confirm/"+validToken, "", "")
		require.Equal(t, http.StatusConflict, w.Code)
		require.Contains(t, w.Body.String(), msgAlreadyConfirmed)

		w = doRequest(t, h, http.MethodPost, "/unsubscribe/"+validToken, "", "")
		require.Equal(t, http.StatusConflict, w.Code)
		require.Contains(t, w.Body.String(), msgAlreadyUnsubscribed)
	})

	t.Run("internal error", func(t *testing.T) {
		svc := &fakeSubsService{
			confirmErr:    errors.New("secret token lookup failure"),
			deactivateErr: errors.New("secret token lookup failure"),
		}
		h, logs := newTestRouter(t, svc)

		for _, path := range []string{"/confirm/" + validToken, "/unsubscribe/" + validToken} {
			w := doRequest(t, h, http.MethodPost, path, "", "")
			require.Equal(t, http.StatusInternalServerError, w.Code)
			require.Contains(t, w.Body.String(), msgInternal)
			require.NotContains(t, w.Body.String(), "secret token lookup failure")
		}

		require.Contains(t, logs.String(), "secret token lookup failure")
	})
}
