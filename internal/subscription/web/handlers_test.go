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
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/identity"
	"repo-watch/internal/subscription/subs"
)

const validToken = "AAAAAAAAAAAAAAAAAAAAAA"

type fakeSubsService struct {
	createErr       error
	createEmail     string
	createRepo      string
	createVerified  bool
	confirmErr      error
	confirmToken    string
	deactivateErr   error
	deactivateToken string

	byRepoEmail string
	byRepoRepo  string
	byRepoErr   error

	listEmail string
	listRepos []string
	listErr   error
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

func (f *fakeSubsService) DeactivateSubscriptionByRepo(
	_ context.Context,
	email identity.Email,
	repo string,
) error {
	f.byRepoEmail = email.String()
	f.byRepoRepo = repo
	return f.byRepoErr
}

func (f *fakeSubsService) GetSubscriptionsList(_ context.Context, email identity.Email) ([]string, error) {
	f.listEmail = email.String()
	return f.listRepos, f.listErr
}

type fakeMetrics struct{}

func (fakeMetrics) HTTPRequest(string, string, int, time.Duration) {}

type fakeAuthService struct {
	signupEmail    string
	signupPassword string
	signupErr      error

	verifyToken string
	verifyErr   error

	loginEmail    string
	loginPassword string
	loginPair     auth.TokenPair
	loginErr      error

	refreshToken string
	refreshPair  auth.TokenPair
	refreshErr   error

	authenticateToken string
	authenticated     auth.AuthenticatedSession
	authenticateErr   error
	authenticateFn    func(string) (auth.AuthenticatedSession, error)

	revokeSessionID int64
	revokeErr       error
}

func (f *fakeAuthService) Signup(_ context.Context, email identity.Email, password string) error {
	f.signupEmail = email.String()
	f.signupPassword = password
	return f.signupErr
}

func (f *fakeAuthService) VerifyEmail(_ context.Context, token string) error {
	f.verifyToken = token
	return f.verifyErr
}

func (f *fakeAuthService) Login(_ context.Context, email identity.Email, password string) (auth.TokenPair, error) {
	f.loginEmail = email.String()
	f.loginPassword = password
	return f.loginPair, f.loginErr
}

func (f *fakeAuthService) RefreshSession(_ context.Context, refreshToken string) (auth.TokenPair, error) {
	f.refreshToken = refreshToken
	return f.refreshPair, f.refreshErr
}

func (f *fakeAuthService) Authenticate(_ context.Context, accessToken string) (auth.AuthenticatedSession, error) {
	f.authenticateToken = accessToken
	if f.authenticateFn != nil {
		return f.authenticateFn(accessToken)
	}
	return f.authenticated, f.authenticateErr
}

func (f *fakeAuthService) RevokeSession(_ context.Context, sessionID int64) error {
	f.revokeSessionID = sessionID
	return f.revokeErr
}

func webSession(email string) auth.AuthenticatedSession {
	return auth.AuthenticatedSession{
		Session: dbGen.Session{ID: 42, UserEmail: email},
		User:    dbGen.User{Email: email},
	}
}

func webTokenPair() auth.TokenPair {
	return auth.TokenPair{
		UserEmail:        "alice@example.com",
		AccessToken:      "access-token",
		RefreshToken:     "refresh-token",
		AccessExpiresIn:  10 * time.Minute,
		RefreshExpiresIn: 30 * 24 * time.Hour,
	}
}

func newTestRouter(t *testing.T, svc subsService) (http.Handler, *bytes.Buffer) {
	t.Helper()

	h, logs, _, _ := newTestRouterWithAuth(t, svc, &fakeAuthService{})
	return h, logs
}

func newTestRouterWithAuth(
	t *testing.T,
	svc subsService,
	authSvc authService,
) (http.Handler, *bytes.Buffer, *auth.CookieManager, *httpserver.CSRF) {
	t.Helper()

	gin.SetMode(gin.TestMode)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cookies := auth.NewCookieManager(auth.CookieConfig{BaseURL: "http://localhost:3000"})
	csrf, err := httpserver.NewCSRF(httpserver.CSRFConfig{
		Secret:  []byte(strings.Repeat("s", 32)),
		BaseURL: "http://localhost:3000",
	})
	require.NoError(t, err)

	router := gin.New()
	router.Use(httpserver.SecurityHeaders())
	router.Use(httpserver.ObservabilityMiddleware(logger, fakeMetrics{}, httpserver.ObservabilityConfig{}))
	router.Use(httpserver.Recovery())

	RegisterRoutes(router.Group("/"), Deps{
		SubsService: svc,
		AuthService: authSvc,
		Cookies:     cookies,
		CSRF:        csrf,
	})
	router.NoRoute(NotFound)

	return router, logs, cookies, csrf
}

func doRequest(t *testing.T, h http.Handler, method, target, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()

	return doRequestWithOptions(t, h, method, target, contentType, body, true)
}

func doRequestWithoutCSRF(t *testing.T, h http.Handler, method, target, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()

	return doRequestWithOptions(t, h, method, target, contentType, body, false)
}

func doRequestWithOptions(
	t *testing.T,
	h http.Handler,
	method, target, contentType, body string,
	withCSRF bool,
) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, target, reader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if withCSRF && !isSafeMethod(method) {
		cookie, token := csrfToken(t, h)
		req.AddCookie(cookie)
		req.Header.Set(httpserver.CSRFHeader, token)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	return w
}

func csrfToken(t *testing.T, h http.Handler) (*http.Cookie, string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	for _, cookie := range w.Result().Cookies() {
		if strings.Contains(cookie.Name, "csrf") {
			return cookie, cookie.Value
		}
	}

	t.Fatal("csrf cookie not found")
	return nil, ""
}

func isSafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
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
	require.Contains(t, w.Body.String(), `name="csrf_token"`)
	require.Empty(t, svc.confirmToken)
}

func TestUnsubscribePage(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestRouter(t, svc)

	w := doRequest(t, h, http.MethodGet, "/unsubscribe/"+validToken, "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "Unsubscribe")
	require.Contains(t, w.Body.String(), `name="csrf_token"`)
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

func TestCSRFRequiredForUnsafeMethods(t *testing.T) {
	svc := &fakeSubsService{}
	h, _ := newTestRouter(t, svc)

	form := url.Values{
		"email": {"alice@example.com"},
		"repo":  {"golang/go"},
	}.Encode()

	w := doRequestWithoutCSRF(t, h, http.MethodPost, "/subscribe",
		"application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.Empty(t, svc.createEmail)
}

func TestSubscribePageIncludesCSRFToken(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `name="csrf_token"`)
	require.Contains(t, w.Body.String(), `value="`)
}

func TestLoginSetsCookiesAndRotatesCSRF(t *testing.T) {
	authSvc := &fakeAuthService{loginPair: webTokenPair()}
	h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

	oldCookie, oldToken := csrfToken(t, h)
	form := url.Values{
		"email":    {"alice@example.com"},
		"password": {"correct horse battery staple"},
	}.Encode()

	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(oldCookie)
	req.Header.Set(httpserver.CSRFHeader, oldToken)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusSeeOther, w.Code)
	require.Equal(t, "/subscriptions", w.Header().Get("Location"))
	require.Equal(t, "alice@example.com", authSvc.loginEmail)
	require.Equal(t, "correct horse battery staple", authSvc.loginPassword)

	access := responseCookie(t, w, "rw_access")
	require.Equal(t, "access-token", access.Value)
	require.True(t, access.HttpOnly)
	require.False(t, access.Secure)
	require.Equal(t, "/", access.Path)
	require.Equal(t, http.SameSiteLaxMode, access.SameSite)
	require.Equal(t, 600, access.MaxAge)

	refresh := responseCookie(t, w, "rw_refresh")
	require.Equal(t, "refresh-token", refresh.Value)
	require.True(t, refresh.HttpOnly)
	require.Equal(t, 2592000, refresh.MaxAge)

	csrf := responseCookie(t, w, "rw_csrf")
	require.NotEqual(t, oldToken, csrf.Value)
	require.False(t, csrf.HttpOnly)
}

func TestLogoutRevokesSessionClearsCookiesAndRotatesCSRF(t *testing.T) {
	authSvc := &fakeAuthService{authenticated: webSession("alice@example.com")}
	h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

	oldCookie, oldToken := csrfToken(t, h)
	req := httptest.NewRequest(http.MethodPost, "/logout", nil)
	req.AddCookie(oldCookie)
	req.AddCookie(&http.Cookie{Name: "rw_access", Value: "access-token"})
	req.Header.Set(httpserver.CSRFHeader, oldToken)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusSeeOther, w.Code)
	require.Equal(t, "/", w.Header().Get("Location"))
	require.Equal(t, int64(42), authSvc.revokeSessionID)

	require.Less(t, responseCookie(t, w, "rw_access").MaxAge, 0)
	require.Less(t, responseCookie(t, w, "rw_refresh").MaxAge, 0)
	require.NotEqual(t, oldToken, responseCookie(t, w, "rw_csrf").Value)
}

func TestCookieAuthRefreshesExpiredAccessToken(t *testing.T) {
	authSvc := &fakeAuthService{
		refreshPair: auth.TokenPair{
			UserEmail:        "alice@example.com",
			AccessToken:      "new-access-token",
			RefreshToken:     "new-refresh-token",
			AccessExpiresIn:  10 * time.Minute,
			RefreshExpiresIn: 30 * 24 * time.Hour,
		},
		authenticateFn: func(token string) (auth.AuthenticatedSession, error) {
			if token == "new-access-token" {
				return webSession("alice@example.com"), nil
			}
			return auth.AuthenticatedSession{}, auth.ErrInvalidToken
		},
	}
	cookies := auth.NewCookieManager(auth.CookieConfig{BaseURL: "http://localhost:3000"})

	router := gin.New()
	router.Use(auth.OptionalCookieAuth(authSvc, cookies, (&webHandler{}).handleCookieAuthError))
	router.GET("/private", func(c *gin.Context) {
		session, ok := auth.SessionFromContext(c)
		require.True(t, ok)
		c.String(http.StatusOK, session.User.Email)
	})

	accessName := cookies.AccessName()
	refreshName := cookies.RefreshName()
	req := httptest.NewRequest(http.MethodGet, "/private", nil)
	req.AddCookie(&http.Cookie{Name: accessName, Value: "old-access-token"})
	req.AddCookie(&http.Cookie{Name: refreshName, Value: "old-refresh-token"})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "alice@example.com", w.Body.String())
	require.Equal(t, "old-refresh-token", authSvc.refreshToken)
	require.Equal(t, "new-access-token", responseCookie(t, w, accessName).Value)
	require.Equal(t, "new-refresh-token", responseCookie(t, w, refreshName).Value)
}

func TestCookieAuthRedirectsWhenRefreshFails(t *testing.T) {
	authSvc := &fakeAuthService{
		authenticateErr: auth.ErrInvalidToken,
		refreshErr:      auth.ErrInvalidToken,
	}
	cookies := auth.NewCookieManager(auth.CookieConfig{BaseURL: "http://localhost:3000"})

	router := gin.New()
	router.Use(auth.CookieAuth(authSvc, cookies, (&webHandler{}).handleCookieAuthError))
	router.GET("/private", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	accessName := cookies.AccessName()
	refreshName := cookies.RefreshName()
	req := httptest.NewRequest(http.MethodGet, "/private", nil)
	req.AddCookie(&http.Cookie{Name: accessName, Value: "expired-access-token"})
	req.AddCookie(&http.Cookie{Name: refreshName, Value: "invalid-refresh-token"})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusSeeOther, w.Code)
	require.Equal(t, "/login?next=%2Fprivate", w.Header().Get("Location"))
	require.Less(t, responseCookie(t, w, accessName).MaxAge, 0)
	require.Less(t, responseCookie(t, w, refreshName).MaxAge, 0)
}

func TestCookieAuthInternalErrorReturns500AndPreservesCookies(t *testing.T) {
	authSvc := &fakeAuthService{authenticateErr: errors.New("database unavailable")}
	cookies := auth.NewCookieManager(auth.CookieConfig{BaseURL: "http://localhost:3000"})

	router := gin.New()
	router.Use(auth.CookieAuth(authSvc, cookies, (&webHandler{}).handleCookieAuthError))
	router.GET("/private", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/private", nil)
	req.AddCookie(&http.Cookie{Name: cookies.AccessName(), Value: "access-token"})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.NotEqual(t, http.StatusSeeOther, w.Code)
	for _, cookie := range w.Result().Cookies() {
		require.NotEqual(t, cookies.AccessName(), cookie.Name)
		require.NotEqual(t, cookies.RefreshName(), cookie.Name)
	}
}

func TestLogoutInternalAuthErrorDoesNotReportSuccess(t *testing.T) {
	authSvc := &fakeAuthService{authenticateErr: errors.New("database unavailable")}
	h, _, cookies, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

	csrfCookie, csrfToken := csrfToken(t, h)
	req := httptest.NewRequest(http.MethodPost, "/logout", nil)
	req.AddCookie(csrfCookie)
	req.AddCookie(&http.Cookie{Name: cookies.AccessName(), Value: "access-token"})
	req.Header.Set(httpserver.CSRFHeader, csrfToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Empty(t, w.Header().Get("Location"))
	require.Zero(t, authSvc.revokeSessionID)
	for _, cookie := range w.Result().Cookies() {
		require.NotEqual(t, cookies.AccessName(), cookie.Name)
		require.NotEqual(t, cookies.RefreshName(), cookie.Name)
	}
}

func responseCookie(t *testing.T, w *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()

	var found *http.Cookie
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == name {
			found = cookie
		}
	}
	if found == nil {
		t.Fatalf("cookie %q not found", name)
	}

	return found
}

func verificationToken(t *testing.T) string {
	t.Helper()

	token, _, err := auth.NewUserVerifyToken()
	require.NoError(t, err)
	return token
}

func addAccessCookie(req *http.Request, cookies *auth.CookieManager) {
	req.AddCookie(&http.Cookie{Name: cookies.AccessName(), Value: "access-token"})
}

func doAuthenticatedRequest(
	t *testing.T,
	h http.Handler,
	cookies *auth.CookieManager,
	method, target, contentType, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, target, reader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	addAccessCookie(req, cookies)
	if !isSafeMethod(method) {
		csrfCookie, token := csrfToken(t, h)
		req.AddCookie(csrfCookie)
		req.Header.Set(httpserver.CSRFHeader, token)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	return w
}

func TestSignupPage(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/signup", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `action="/signup"`)
	require.Contains(t, w.Body.String(), `name="email"`)
	require.Contains(t, w.Body.String(), `name="password"`)
	require.Contains(t, w.Body.String(), `name="csrf_token"`)
	require.Contains(t, w.Body.String(), `href="/login"`)
}

func TestSignupSuccess(t *testing.T) {
	authSvc := &fakeAuthService{}
	h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

	form := url.Values{
		"email":    {"alice@example.com"},
		"password": {"correct horse battery staple"},
	}.Encode()

	w := doRequest(t, h, http.MethodPost, "/signup", "application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), msgSignupCheckEmail)
	require.Equal(t, "alice@example.com", authSvc.signupEmail)
	require.Equal(t, "correct horse battery staple", authSvc.signupPassword)
}

func TestSignupValidationPreservesEmail(t *testing.T) {
	authSvc := &fakeAuthService{}
	h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

	form := url.Values{
		"email":    {"not-an-email"},
		"password": {"secret"},
	}.Encode()

	w := doRequest(t, h, http.MethodPost, "/signup", "application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), msgSignupInvalid)
	require.Contains(t, w.Body.String(), `value="not-an-email"`)
	require.NotContains(t, w.Body.String(), `value="secret"`)
	require.Empty(t, authSvc.signupEmail)
}

func TestSignupMapsErrors(t *testing.T) {
	t.Run("invalid password length", func(t *testing.T) {
		authSvc := &fakeAuthService{signupErr: auth.ErrInvalidPasswordLength}
		h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

		form := url.Values{
			"email":    {"alice@example.com"},
			"password": {"secret"},
		}.Encode()

		w := doRequest(t, h, http.MethodPost, "/signup", "application/x-www-form-urlencoded", form)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), msgSignupInvalid)
	})

	t.Run("internal error", func(t *testing.T) {
		authSvc := &fakeAuthService{signupErr: errors.New("secret signup failure")}
		h, logs, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

		form := url.Values{
			"email":    {"alice@example.com"},
			"password": {"secret"},
		}.Encode()

		w := doRequest(t, h, http.MethodPost, "/signup", "application/x-www-form-urlencoded", form)

		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Contains(t, w.Body.String(), msgInternal)
		require.NotContains(t, w.Body.String(), "secret signup failure")
		require.Contains(t, logs.String(), "secret signup failure")
	})
}

func TestLoginPage(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/login?next=%2Fsubscriptions&verified=1", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `action="/login"`)
	require.Contains(t, w.Body.String(), msgVerified)
	require.Contains(t, w.Body.String(), `name="next" value="/subscriptions"`)
	require.Contains(t, w.Body.String(), `name="csrf_token"`)
}

func TestLoginErrorPreservesEmail(t *testing.T) {
	authSvc := &fakeAuthService{loginErr: auth.ErrInvalidCredentials}
	h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

	form := url.Values{
		"email":    {"alice@example.com"},
		"password": {"wrong"},
		"next":     {"/subscriptions"},
	}.Encode()

	w := doRequest(t, h, http.MethodPost, "/login", "application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Contains(t, w.Body.String(), msgInvalidCredentials)
	require.Contains(t, w.Body.String(), `value="alice@example.com"`)
	require.NotContains(t, w.Body.String(), `value="wrong"`)
	require.Contains(t, w.Body.String(), `name="next" value="/subscriptions"`)
}

func TestLoginRedirectsOnlyToLocalPaths(t *testing.T) {
	tests := []struct {
		next     string
		expected string
	}{
		{next: "/subscriptions", expected: "/subscriptions"},
		{next: "/subscriptions?tab=all", expected: "/subscriptions?tab=all"},
		{next: "https://evil.example", expected: "/"},
		{next: "//evil.example", expected: "/"},
		{next: `/\evil.example`, expected: "/"},
		{next: "/\\evil.example", expected: "/"},
		{next: "/%5Cevil.example", expected: "/"},
		{next: "/%2F%2Fevil.example", expected: "/"},
		{next: "subscriptions", expected: "/"},
	}

	for _, tt := range tests {
		t.Run(tt.next, func(t *testing.T) {
			authSvc := &fakeAuthService{loginPair: webTokenPair()}
			h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

			form := url.Values{
				"email":    {"alice@example.com"},
				"password": {"correct horse battery staple"},
				"next":     {tt.next},
			}.Encode()

			w := doRequest(t, h, http.MethodPost, "/login", "application/x-www-form-urlencoded", form)

			require.Equal(t, http.StatusSeeOther, w.Code)
			require.Equal(t, tt.expected, w.Header().Get("Location"))
		})
	}
}

func TestVerifyPageDoesNotRenderToken(t *testing.T) {
	token := verificationToken(t)
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/verify/"+token, "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "Verify email")
	require.Contains(t, w.Body.String(), `name="csrf_token"`)
	require.NotContains(t, w.Body.String(), token)
}

func TestVerifyEmail(t *testing.T) {
	token := verificationToken(t)
	authSvc := &fakeAuthService{}
	h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

	w := doRequest(t, h, http.MethodPost, "/verify/"+token, "", "")

	require.Equal(t, http.StatusSeeOther, w.Code)
	require.Equal(t, "/login?verified=1", w.Header().Get("Location"))
	require.Equal(t, token, authSvc.verifyToken)
}

func TestVerifyEmailRejectsInvalidToken(t *testing.T) {
	t.Run("malformed token", func(t *testing.T) {
		authSvc := &fakeAuthService{}
		h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

		w := doRequest(t, h, http.MethodPost, "/verify/not-a-token", "", "")

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), msgInvalidVerifyToken)
		require.Empty(t, authSvc.verifyToken)
	})

	t.Run("unknown token", func(t *testing.T) {
		authSvc := &fakeAuthService{verifyErr: auth.ErrInvalidToken}
		h, _, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

		w := doRequest(t, h, http.MethodPost, "/verify/"+verificationToken(t), "", "")

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), msgInvalidVerifyToken)
	})

	t.Run("internal error", func(t *testing.T) {
		authSvc := &fakeAuthService{verifyErr: errors.New("secret verify failure")}
		h, logs, _, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

		w := doRequest(t, h, http.MethodPost, "/verify/"+verificationToken(t), "", "")

		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Contains(t, w.Body.String(), msgInternal)
		require.NotContains(t, w.Body.String(), "secret verify failure")
		require.Contains(t, logs.String(), "secret verify failure")
	})
}

func TestAuthenticatedSubscribeUsesSessionEmail(t *testing.T) {
	svc := &fakeSubsService{}
	authSvc := &fakeAuthService{authenticated: webSession("alice@example.com")}
	h, _, cookies, _ := newTestRouterWithAuth(t, svc, authSvc)

	w := doAuthenticatedRequest(t, h, cookies, http.MethodGet, "/", "", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "alice@example.com")
	require.NotContains(t, w.Body.String(), `name="email"`)

	form := url.Values{"repo": {"golang/go"}}.Encode()
	w = doAuthenticatedRequest(t, h, cookies, http.MethodPost, "/subscribe",
		"application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), msgSubscribed)
	require.Equal(t, "alice@example.com", svc.createEmail)
	require.Equal(t, "golang/go", svc.createRepo)
	require.True(t, svc.createVerified)
}

func TestAuthenticatedSubscribeRejectsMismatchedEmail(t *testing.T) {
	svc := &fakeSubsService{}
	authSvc := &fakeAuthService{authenticated: webSession("alice@example.com")}
	h, _, cookies, _ := newTestRouterWithAuth(t, svc, authSvc)

	form := url.Values{
		"email": {"bob@example.com"},
		"repo":  {"golang/go"},
	}.Encode()
	w := doAuthenticatedRequest(t, h, cookies, http.MethodPost, "/subscribe",
		"application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.Empty(t, svc.createEmail)
}

func TestNavigationReflectsSession(t *testing.T) {
	t.Run("anonymous", func(t *testing.T) {
		h, _ := newTestRouter(t, &fakeSubsService{})

		w := doRequest(t, h, http.MethodGet, "/", "", "")

		require.Contains(t, w.Body.String(), `href="/login"`)
		require.Contains(t, w.Body.String(), `href="/signup"`)
		require.NotContains(t, w.Body.String(), "/subscriptions")
	})

	t.Run("authenticated", func(t *testing.T) {
		authSvc := &fakeAuthService{authenticated: webSession("alice@example.com")}
		h, _, cookies, _ := newTestRouterWithAuth(t, &fakeSubsService{}, authSvc)

		w := doAuthenticatedRequest(t, h, cookies, http.MethodGet, "/", "", "")

		require.Contains(t, w.Body.String(), "alice@example.com")
		require.Contains(t, w.Body.String(), `href="/subscriptions"`)
		require.Contains(t, w.Body.String(), `action="/logout"`)
	})
}

func TestSubscriptionsRequiresLogin(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/subscriptions", "", "")

	require.Equal(t, http.StatusSeeOther, w.Code)
	require.Equal(t, "/login?next=%2Fsubscriptions", w.Header().Get("Location"))
}

func TestSubscriptionsPage(t *testing.T) {
	svc := &fakeSubsService{listRepos: []string{"golang/go", "owner/repo"}}
	authSvc := &fakeAuthService{authenticated: webSession("alice@example.com")}
	h, _, cookies, _ := newTestRouterWithAuth(t, svc, authSvc)

	w := doAuthenticatedRequest(t, h, cookies, http.MethodGet, "/subscriptions", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "alice@example.com", svc.listEmail)
	require.Contains(t, w.Body.String(), "golang/go")
	require.Contains(t, w.Body.String(), "owner/repo")
	require.Contains(t, w.Body.String(), `action="/subscriptions/unsubscribe"`)
	require.Contains(t, w.Body.String(), `name="repo" value="golang/go"`)
	require.Contains(t, w.Body.String(), `name="csrf_token"`)
}

func TestSubscriptionsPageEmpty(t *testing.T) {
	svc := &fakeSubsService{}
	authSvc := &fakeAuthService{authenticated: webSession("alice@example.com")}
	h, _, cookies, _ := newTestRouterWithAuth(t, svc, authSvc)

	w := doAuthenticatedRequest(t, h, cookies, http.MethodGet, "/subscriptions?unsubscribed=1", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), msgNoSubscriptions)
	require.Contains(t, w.Body.String(), msgUnsubscribed)
}

func TestUnsubscribeByRepo(t *testing.T) {
	svc := &fakeSubsService{}
	authSvc := &fakeAuthService{authenticated: webSession("alice@example.com")}
	h, _, cookies, _ := newTestRouterWithAuth(t, svc, authSvc)

	form := url.Values{"repo": {"golang/go"}}.Encode()
	w := doAuthenticatedRequest(t, h, cookies, http.MethodPost, "/subscriptions/unsubscribe",
		"application/x-www-form-urlencoded", form)

	require.Equal(t, http.StatusSeeOther, w.Code)
	require.Equal(t, "/subscriptions?unsubscribed=1", w.Header().Get("Location"))
	require.Equal(t, "alice@example.com", svc.byRepoEmail)
	require.Equal(t, "golang/go", svc.byRepoRepo)
}

func TestUnsubscribeByRepoErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{
			name:       "invalid input",
			err:        subs.ErrInvalidInput,
			wantStatus: http.StatusBadRequest,
			wantBody:   msgInvalidInput,
		},
		{
			name:       "not found",
			err:        sql.ErrNoRows,
			wantStatus: http.StatusNotFound,
			wantBody:   msgSubscriptionNotFound,
		},
		{
			name:       "already inactive",
			err:        subs.ErrSubscriptionAlreadyInactive,
			wantStatus: http.StatusConflict,
			wantBody:   msgAlreadyUnsubscribed,
		},
		{
			name:       "internal",
			err:        errors.New("secret unsubscribe failure"),
			wantStatus: http.StatusInternalServerError,
			wantBody:   msgInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeSubsService{byRepoErr: tt.err}
			authSvc := &fakeAuthService{authenticated: webSession("alice@example.com")}
			h, logs, cookies, _ := newTestRouterWithAuth(t, svc, authSvc)

			form := url.Values{"repo": {"golang/go"}}.Encode()
			w := doAuthenticatedRequest(t, h, cookies, http.MethodPost, "/subscriptions/unsubscribe",
				"application/x-www-form-urlencoded", form)

			require.Equal(t, tt.wantStatus, w.Code)
			require.Contains(t, w.Body.String(), tt.wantBody)
			if tt.wantStatus == http.StatusInternalServerError {
				require.NotContains(t, w.Body.String(), "secret unsubscribe failure")
				require.Contains(t, logs.String(), "secret unsubscribe failure")
			}
		})
	}
}

func TestLogoutPage(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/logout", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), msgLogoutConfirmation)
	require.Contains(t, w.Body.String(), `action="/logout"`)
	require.Contains(t, w.Body.String(), `name="csrf_token"`)
}

func TestProtectedPageWithoutSessionIsInternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &webHandler{subsService: &fakeSubsService{}}
	router := gin.New()
	router.GET("/subscriptions", h.handleSubscriptionsPage)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/subscriptions", nil))

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Contains(t, w.Body.String(), msgInternal)
}
