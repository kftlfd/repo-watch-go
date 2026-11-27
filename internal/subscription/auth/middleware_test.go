package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"repo-watch/internal/subscription/db/dbGen"
)

func testAuthErrorHandler(c *gin.Context, err error) {
	if errors.Is(err, ErrInvalidToken) {
		c.Header("WWW-Authenticate", "Bearer")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
}

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
	router.Use(BearerAuth(authenticator, testAuthErrorHandler))
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

func testOptionalAuthRouter(authenticator Authenticator) *gin.Engine {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(OptionalBearerAuth(authenticator, testAuthErrorHandler))
	router.GET("/me", func(c *gin.Context) {
		authenticated, ok := SessionFromContext(c)
		if !ok {
			c.JSON(http.StatusOK, gin.H{"anonymous": true})
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

func TestOptionalBearerAuth(t *testing.T) {
	authenticated := AuthenticatedSession{
		Session: dbGen.Session{ID: 42, UserEmail: "alice@example.com"},
		User:    dbGen.User{Email: "alice@example.com"},
	}

	t.Run("anonymous", func(t *testing.T) {
		router := testOptionalAuthRouter(&fakeAuthenticator{})

		req := httptest.NewRequest(http.MethodGet, "/me", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.JSONEq(t, `{"anonymous":true}`, w.Body.String())
	})

	t.Run("authenticated", func(t *testing.T) {
		authenticator := &fakeAuthenticator{authenticated: authenticated}
		router := testOptionalAuthRouter(authenticator)

		req := httptest.NewRequest(http.MethodGet, "/me", nil)
		req.Header.Set("Authorization", "Bearer access-token")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "access-token", authenticator.token)
		require.JSONEq(t, `{"email":"alice@example.com"}`, w.Body.String())
	})

	t.Run("invalid token", func(t *testing.T) {
		router := testOptionalAuthRouter(&fakeAuthenticator{err: ErrInvalidToken})

		req := httptest.NewRequest(http.MethodGet, "/me", nil)
		req.Header.Set("Authorization", "Bearer expired-token")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
	})

	t.Run("malformed header", func(t *testing.T) {
		router := testOptionalAuthRouter(&fakeAuthenticator{})

		req := httptest.NewRequest(http.MethodGet, "/me", nil)
		req.Header.Set("Authorization", "Basic credentials")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
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

func TestAuthErrorHandlerReceivesFailure(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		authErr error
		wantErr error
	}{
		{
			name:    "missing token",
			wantErr: ErrInvalidToken,
		},
		{
			name:    "invalid token",
			header:  "Bearer expired-token",
			authErr: ErrInvalidToken,
			wantErr: ErrInvalidToken,
		},
		{
			name:    "internal error",
			header:  "Bearer access-token",
			authErr: context.Canceled,
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var handled error
			router := gin.New()
			router.Use(BearerAuth(&fakeAuthenticator{err: tt.authErr}, func(c *gin.Context, err error) {
				handled = err
				c.Status(http.StatusUnauthorized)
			}))
			router.GET("/me", func(c *gin.Context) {
				c.Status(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/me", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusUnauthorized, w.Code)
			require.ErrorIs(t, handled, tt.wantErr)
		})
	}
}

func TestRequireSession(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	_, err := RequireSession(c)
	require.ErrorIs(t, err, ErrNoSession)

	want := AuthenticatedSession{
		Session: dbGen.Session{ID: 42},
		User:    dbGen.User{Email: "alice@example.com"},
	}
	setSession(c, want)

	got, err := RequireSession(c)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
