package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"repo-watch/internal/subscription/db/dbGen"
)

type fakeAuthenticator struct {
	token         string
	authenticated AuthenticatedSession
	err           error
}

func (f *fakeAuthenticator) Authenticate(_ context.Context, accessToken string) (AuthenticatedSession, error) {
	f.token = accessToken
	return f.authenticated, f.err
}

func testAuthRouter(authenticator Authenticator) *gin.Engine {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(BearerAuth(authenticator))
	router.GET("/me", func(c *gin.Context) {
		authenticated, ok := SessionFromContext(c)
		if !ok {
			c.Status(http.StatusInternalServerError)
			return
		}
		c.JSON(http.StatusOK, gin.H{"email": authenticated.User.Email})
	})

	return router
}

func TestParseBearerToken(t *testing.T) {
	tests := []struct {
		header string
		token  string
		ok     bool
	}{
		{header: "Bearer abc", token: "abc", ok: true},
		{header: "bearer   abc", token: "abc", ok: true},
		{header: "", ok: false},
		{header: "Basic abc", ok: false},
		{header: "Bearer", ok: false},
		{header: "Bearer ", ok: false},
		{header: "Bearer abc def", ok: false},
	}

	for _, tt := range tests {
		token, ok := ParseBearerToken(tt.header)
		require.Equal(t, tt.ok, ok, tt.header)
		require.Equal(t, tt.token, token, tt.header)
	}
}

func TestBearerAuthAllowsAuthenticatedSession(t *testing.T) {
	authenticator := &fakeAuthenticator{
		authenticated: AuthenticatedSession{
			Session: dbGen.Session{ID: 42, UserEmail: "alice@example.com"},
			User:    dbGen.User{Email: "alice@example.com"},
		},
	}
	router := testAuthRouter(authenticator)

	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.Header.Set("Authorization", "Bearer access-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "access-token", authenticator.token)
	require.JSONEq(t, `{"email":"alice@example.com"}`, w.Body.String())
}

func TestBearerAuthRejectsMissingInvalidAndInternalErrors(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		router := testAuthRouter(&fakeAuthenticator{})

		req := httptest.NewRequest(http.MethodGet, "/me", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
	})

	t.Run("invalid", func(t *testing.T) {
		router := testAuthRouter(&fakeAuthenticator{err: ErrInvalidToken})

		req := httptest.NewRequest(http.MethodGet, "/me", nil)
		req.Header.Set("Authorization", "Bearer expired-token")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
	})

	t.Run("internal", func(t *testing.T) {
		router := testAuthRouter(&fakeAuthenticator{err: context.Canceled})

		req := httptest.NewRequest(http.MethodGet, "/me", nil)
		req.Header.Set("Authorization", "Bearer access-token")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
