package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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
	"repo-watch/internal/subscription/identity"
	"repo-watch/internal/subscription/subs"
)

type fakeSubsService struct {
	createErr         error
	createEmail       string
	createRepo        string
	createVerified    bool
	confirmErr        error
	confirmToken      string
	deactivateErr     error
	deactivateToken   string
	deactivateRepo    string
	deactivateEmail   string
	deactivateRepoErr error
	listEmail         string
	listRepos         []string
	listErr           error
}

func (f *fakeSubsService) CreateSubscription(_ context.Context, email identity.Email, repo string, verified bool) error {
	f.createEmail = email.String()
	f.createRepo = repo
	f.createVerified = verified
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

func (f *fakeSubsService) DeactivateSubscriptionByRepo(_ context.Context, email identity.Email, repo string) error {
	f.deactivateEmail = email.String()
	f.deactivateRepo = repo
	return f.deactivateRepoErr
}

func (f *fakeSubsService) GetSubscriptionsList(_ context.Context, email identity.Email) ([]string, error) {
	f.listEmail = email.String()
	return f.listRepos, f.listErr
}

type fakeMetrics struct{}

func (fakeMetrics) HTTPRequest(string, string, int, time.Duration) {}

func newTestHandler(t *testing.T, svc subsService) (http.Handler, *bytes.Buffer) {
	t.Helper()

	return newTestHandlerWithAuth(t, svc, nil)
}

func newAuthenticatedTestHandler(t *testing.T, svc subsService) (http.Handler, *bytes.Buffer) {
	t.Helper()

	return newTestHandlerWithAuth(t, svc, &fakeAuthService{
		authenticated: authSessionFor("alice@example.com"),
	})
}

func newTestHandlerWithAuth(t *testing.T, svc subsService, authSvc authService) (http.Handler, *bytes.Buffer) {
	t.Helper()

	gin.SetMode(gin.TestMode)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	router := gin.New()
	router.Use(httpserver.ObservabilityMiddleware(logger, fakeMetrics{}, false))
	router.Use(httpserver.Recovery())

	RegisterRoutes(router.Group("/api"), Deps{
		SubsService: svc,
		AuthService: authSvc,
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

func doAuthenticatedRequest(t *testing.T, h http.Handler, method, target, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, target, reader)
	req.Header.Set("Authorization", "Bearer access-token")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	return w
}

func decodeError(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()

	var resp struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp.Error
}

func TestSubscribeAcceptsFormAndJSON(t *testing.T) {
	formBody := url.Values{
		"email": {"alice@example.com"},
		"repo":  {"golang/go"},
	}.Encode()

	tests := []struct {
		name        string
		contentType string
		body        string
	}{
		{
			name:        "form",
			contentType: "application/x-www-form-urlencoded",
			body:        formBody,
		},
		{
			name:        "json",
			contentType: "application/json",
			body:        `{"email":"alice@example.com","repo":"golang/go"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{}
			h, _ := newTestHandler(t, svc)

			w := doRequest(t, h, http.MethodPost, "/api/subscribe", tt.contentType, tt.body)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "alice@example.com", svc.createEmail)
			require.Equal(t, "golang/go", svc.createRepo)
			require.False(t, svc.createVerified)

			var resp struct {
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.Equal(t, msgCheckEmail, resp.Message)
		})
	}
}

func TestSubscribeRejectsInvalidBody(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
	}{
		{
			name:        "missing repo",
			contentType: "application/x-www-form-urlencoded",
			body:        url.Values{"email": {"alice@example.com"}}.Encode(),
		},
		{
			name:        "invalid email",
			contentType: "application/json",
			body:        `{"email":"not-an-email","repo":"golang/go"}`,
		},
		{
			name:        "malformed json",
			contentType: "application/json",
			body:        `{`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{}
			h, _ := newTestHandler(t, svc)

			w := doRequest(t, h, http.MethodPost, "/api/subscribe", tt.contentType, tt.body)

			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Equal(t, errInvalidInput, decodeError(t, w))
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
			wantBody:   errInvalidInput,
		},
		{
			name:       "repository not found",
			err:        subs.ErrRepoNotFound,
			wantStatus: http.StatusNotFound,
			wantBody:   errRepoNotFound,
		},
		{
			name:       "subscription conflict",
			err:        subs.ErrSubscriptionConflict,
			wantStatus: http.StatusConflict,
			wantBody:   errAlreadyExists,
		},
		{
			name:       "rate limit",
			err:        subs.ErrRateLimit,
			wantStatus: http.StatusTooManyRequests,
			wantBody:   errRateLimited,
		},
		{
			name:         "upstream",
			err:          subs.ErrUpstreamError,
			wantStatus:   http.StatusBadGateway,
			wantBody:     errUpstream,
			wantErrorLog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{createErr: tt.err}
			h, logs := newTestHandler(t, svc)

			w := doRequest(t, h, http.MethodPost, "/api/subscribe",
				"application/json", `{"email":"alice@example.com","repo":"golang/go"}`)

			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, tt.wantBody, decodeError(t, w))
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
	h, logs := newTestHandler(t, svc)

	w := doRequest(t, h, http.MethodPost, "/api/subscribe",
		"application/json", `{"email":"alice@example.com","repo":"golang/go"}`)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Equal(t, errInternal, decodeError(t, w))
	require.NotContains(t, w.Body.String(), "secret database failure")

	require.Contains(t, logs.String(), "secret database failure")
	require.Contains(t, logs.String(), "operation=\"create subscription\"")
	require.Contains(t, logs.String(), "path=/api/subscribe")
}

func TestSubscribeAuthenticatedUsesSessionEmailAndActivatesImmediately(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "email omitted",
			body: `{"repo":"golang/go"}`,
		},
		{
			name: "matching email supplied",
			body: `{"email":"ALICE@example.com","repo":"golang/go"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{}
			h, _ := newTestHandlerWithAuth(t, svc, &fakeAuthService{
				authenticated: authSessionFor("alice@example.com"),
			})

			w := doAuthenticatedRequest(t, h, http.MethodPost, "/api/subscribe", "application/json", tt.body)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, "alice@example.com", svc.createEmail)
			require.Equal(t, "golang/go", svc.createRepo)
			require.True(t, svc.createVerified)

			var resp struct {
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.Equal(t, msgSubscribed, resp.Message)
		})
	}
}

func TestSubscribeAuthenticatedRejectsDifferentOrInvalidEmail(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "different email",
			body: `{"email":"bob@example.com","repo":"golang/go"}`,
		},
		{
			name: "invalid email",
			body: `{"email":"not-an-email","repo":"golang/go"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{}
			h, _ := newTestHandlerWithAuth(t, svc, &fakeAuthService{
				authenticated: authSessionFor("alice@example.com"),
			})

			w := doAuthenticatedRequest(t, h, http.MethodPost, "/api/subscribe", "application/json", tt.body)

			require.Equal(t, http.StatusForbidden, w.Code)
			require.Equal(t, errEmailMismatch, decodeError(t, w))
			require.Empty(t, svc.createEmail)
		})
	}
}

func TestSubscribeAnonymousRequiresEmail(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestHandler(t, svc)

	w := doRequest(t, h, http.MethodPost, "/api/subscribe", "application/json",
		`{"repo":"golang/go"}`)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, errInvalidInput, decodeError(t, w))
	require.Empty(t, svc.createEmail)
}

func TestUnsubscribeByRepoRequiresBearer(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestHandler(t, svc)

	w := doRequest(t, h, http.MethodPost, "/api/subscriptions/unsubscribe",
		"application/json", `{"repo":"golang/go"}`)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
	require.Empty(t, svc.deactivateRepo)
}

func TestUnsubscribeByRepoUsesSessionEmail(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newAuthenticatedTestHandler(t, svc)

	w := doAuthenticatedRequest(t, h, http.MethodPost, "/api/subscriptions/unsubscribe",
		"application/json", `{"repo":"golang/go"}`)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "alice@example.com", svc.deactivateEmail)
	require.Equal(t, "golang/go", svc.deactivateRepo)

	var resp struct {
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, msgUnsubscribed, resp.Message)
}

func TestUnsubscribeByRepoMapsErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantError  string
	}{
		{
			name:       "invalid input",
			err:        subs.ErrInvalidInput,
			wantStatus: http.StatusBadRequest,
			wantError:  errInvalidInput,
		},
		{
			name:       "not found",
			err:        sql.ErrNoRows,
			wantStatus: http.StatusNotFound,
			wantError:  errSubscriptionNotFound,
		},
		{
			name:       "already inactive",
			err:        subs.ErrSubscriptionAlreadyInactive,
			wantStatus: http.StatusConflict,
			wantError:  errAlreadyUnsubscribed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{deactivateRepoErr: tt.err}
			h, _ := newAuthenticatedTestHandler(t, svc)

			w := doAuthenticatedRequest(t, h, http.MethodPost, "/api/subscriptions/unsubscribe",
				"application/json", `{"repo":"golang/go"}`)

			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, tt.wantError, decodeError(t, w))
		})
	}
}

func TestTokenEndpointsSupportGetAndPost(t *testing.T) {
	const token = "AAAAAAAAAAAAAAAAAAAAAA"

	tests := []struct {
		name        string
		method      string
		path        string
		confirm     bool
		wantMessage string
	}{
		{name: "confirm post", method: http.MethodPost, path: "/api/confirm/" + token, confirm: true, wantMessage: msgConfirmed},
		{name: "unsubscribe post", method: http.MethodPost, path: "/api/unsubscribe/" + token, wantMessage: msgUnsubscribed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{}
			h, _ := newTestHandler(t, svc)

			w := doRequest(t, h, tt.method, tt.path, "", "")

			require.Equal(t, http.StatusOK, w.Code)

			var resp struct {
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.Equal(t, tt.wantMessage, resp.Message)

			if tt.confirm {
				require.Equal(t, token, svc.confirmToken)
			} else {
				require.Equal(t, token, svc.deactivateToken)
			}
		})
	}
}

func TestTokenEndpointsMapErrors(t *testing.T) {
	const token = "AAAAAAAAAAAAAAAAAAAAAA"

	t.Run("unknown token", func(t *testing.T) {
		svc := &fakeSubsService{
			confirmErr:    sql.ErrNoRows,
			deactivateErr: sql.ErrNoRows,
		}
		h, _ := newTestHandler(t, svc)

		for _, path := range []string{"/api/confirm/" + token, "/api/unsubscribe/" + token} {
			w := doRequest(t, h, http.MethodPost, path, "", "")
			require.Equal(t, http.StatusNotFound, w.Code)
			require.Equal(t, errTokenNotFound, decodeError(t, w))
		}
	})

	t.Run("already settled", func(t *testing.T) {
		svc := &fakeSubsService{
			confirmErr:    subs.ErrSubscriptionAlreadyActive,
			deactivateErr: subs.ErrSubscriptionAlreadyInactive,
		}
		h, _ := newTestHandler(t, svc)

		w := doRequest(t, h, http.MethodPost, "/api/confirm/"+token, "", "")
		require.Equal(t, http.StatusConflict, w.Code)
		require.Equal(t, errAlreadyConfirmed, decodeError(t, w))

		w = doRequest(t, h, http.MethodPost, "/api/unsubscribe/"+token, "", "")
		require.Equal(t, http.StatusConflict, w.Code)
		require.Equal(t, errAlreadyUnsubscribed, decodeError(t, w))
	})

	t.Run("malformed token", func(t *testing.T) {
		svc := &fakeSubsService{}
		h, _ := newTestHandler(t, svc)

		for _, path := range []string{"/api/confirm/not-a-token", "/api/unsubscribe/not-a-token"} {
			w := doRequest(t, h, http.MethodPost, path, "", "")
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Equal(t, errInvalidToken, decodeError(t, w))
		}

		require.Empty(t, svc.confirmToken)
		require.Empty(t, svc.deactivateToken)
	})

	t.Run("internal error", func(t *testing.T) {
		svc := &fakeSubsService{
			confirmErr:    errors.New("secret token lookup failure"),
			deactivateErr: errors.New("secret token lookup failure"),
		}
		h, logs := newTestHandler(t, svc)

		for _, path := range []string{"/api/confirm/" + token, "/api/unsubscribe/" + token} {
			w := doRequest(t, h, http.MethodPost, path, "", "")
			require.Equal(t, http.StatusInternalServerError, w.Code)
			require.Equal(t, errInternal, decodeError(t, w))
			require.NotContains(t, w.Body.String(), "secret token lookup failure")
		}

		require.Contains(t, logs.String(), "secret token lookup failure")
	})
}

func TestListSubscriptionsRequiresBearer(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestHandler(t, svc)

	w := doRequest(t, h, http.MethodGet, "/api/subscriptions", "", "")

	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	require.Empty(t, svc.listEmail)
}

func TestListSubscriptionsUsesSessionEmail(t *testing.T) {
	svc := &fakeSubsService{listRepos: []string{"golang/go"}}
	h, _ := newAuthenticatedTestHandler(t, svc)

	w := doRequestWithHeader(t, h, http.MethodGet, "/api/subscriptions", "Authorization", "Bearer access-token")

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "alice@example.com", svc.listEmail)

	var resp []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "golang/go", resp[0]["repo"])
	require.NotContains(t, resp[0], "email")
}

func TestListSubscriptionsResponseShape(t *testing.T) {
	svc := &fakeSubsService{listRepos: []string{"golang/go", "owner/repository"}}
	h, _ := newAuthenticatedTestHandler(t, svc)

	w := doRequestWithHeader(t, h, http.MethodGet, "/api/subscriptions", "Authorization", "Bearer access-token")

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "alice@example.com", svc.listEmail)

	var resp []map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp, 2)

	for i, repo := range []string{"golang/go", "owner/repository"} {
		require.Equal(t, map[string]any{
			"repo":      repo,
			"confirmed": true,
		}, resp[i])
	}

	require.NotContains(t, w.Body.String(), "last_seen_tag")
}

func TestListSubscriptionsEmptyResultIsArray(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newAuthenticatedTestHandler(t, svc)

	w := doRequestWithHeader(t, h, http.MethodGet, "/api/subscriptions", "Authorization", "Bearer access-token")

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[]`, w.Body.String())
}

func TestListSubscriptionsHidesInternalErrors(t *testing.T) {
	svc := &fakeSubsService{listErr: errors.New("secret list failure")}
	h, logs := newAuthenticatedTestHandler(t, svc)

	w := doRequestWithHeader(t, h, http.MethodGet, "/api/subscriptions", "Authorization", "Bearer access-token")

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Equal(t, errInternal, decodeError(t, w))
	require.NotContains(t, w.Body.String(), "secret list failure")
	require.Contains(t, logs.String(), "secret list failure")
}

func TestProtectedHandlerWithoutSessionIsInternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &apiHandler{subsService: &fakeSubsService{}}
	router := gin.New()
	router.GET("/subscriptions", h.handleListSubscriptions)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/subscriptions", nil))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, w.Body.String(), errInternal)
}
