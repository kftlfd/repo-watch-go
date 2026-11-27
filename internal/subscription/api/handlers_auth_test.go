package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/identity"
)

type fakeAuthService struct {
	signupEmail    string
	signupPassword string
	signupErr      error

	verifyToken string
	verifyErr   error

	resendEmail string
	resendErr   error

	loginEmail    string
	loginPassword string
	loginPair     auth.TokenPair
	loginErr      error

	refreshToken string
	refreshPair  auth.TokenPair
	refreshErr   error

	revokeSessionID int64
	revokeErr       error

	revokeRefreshToken string
	revokeRefreshErr   error

	authenticateToken string
	authenticated     auth.AuthenticatedSession
	authenticateErr   error
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

func (f *fakeAuthService) ResendVerification(_ context.Context, email identity.Email) error {
	f.resendEmail = email.String()
	return f.resendErr
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

func (f *fakeAuthService) RevokeSession(_ context.Context, sessionID int64) error {
	f.revokeSessionID = sessionID
	return f.revokeErr
}

func (f *fakeAuthService) RevokeSessionByRefreshToken(_ context.Context, refreshToken string) error {
	f.revokeRefreshToken = refreshToken
	return f.revokeRefreshErr
}

func (f *fakeAuthService) Authenticate(_ context.Context, accessToken string) (auth.AuthenticatedSession, error) {
	f.authenticateToken = accessToken
	return f.authenticated, f.authenticateErr
}

func authSessionFor(email string) auth.AuthenticatedSession {
	return auth.AuthenticatedSession{
		Session: dbGen.Session{ID: 42, UserEmail: email},
		User:    dbGen.User{Email: email},
	}
}

func testTokenPair() auth.TokenPair {
	return auth.TokenPair{
		UserEmail:        "alice@example.com",
		AccessToken:      "access-token",
		RefreshToken:     "refresh-token",
		AccessExpiresIn:  10 * time.Minute,
		RefreshExpiresIn: 30 * 24 * time.Hour,
	}
}

func TestSignupReturnsGenericAcceptedResponse(t *testing.T) {
	svc := &fakeAuthService{}
	h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

	w := doRequest(t, h, http.MethodPost, "/api/auth/signup", "application/json",
		`{"email":"alice@example.com","password":"correct horse battery staple"}`)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, "alice@example.com", svc.signupEmail)
	require.Equal(t, "correct horse battery staple", svc.signupPassword)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	require.JSONEq(t, `{"message":"check email to verify your account"}`, w.Body.String())
}

func TestSignupRejectsInvalidPassword(t *testing.T) {
	svc := &fakeAuthService{signupErr: auth.ErrInvalidPasswordLength}
	h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

	w := doRequest(t, h, http.MethodPost, "/api/auth/signup", "application/json",
		`{"email":"alice@example.com","password":"short"}`)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, errInvalidAuthInput, decodeError(t, w))
}

func TestVerifyEmailAndResendDispatch(t *testing.T) {
	svc := &fakeAuthService{}
	h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

	w := doRequest(t, h, http.MethodPost, "/api/auth/verify/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotEmpty(t, svc.verifyToken)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	svc.verifyToken = ""

	w = doRequest(t, h, http.MethodPost, "/api/auth/verify-resend", "application/json",
		`{"email":"alice@example.com"}`)
	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, "alice@example.com", svc.resendEmail)
	require.Empty(t, svc.verifyToken)
}

func TestVerifyEmailRejectsInvalidToken(t *testing.T) {
	svc := &fakeAuthService{verifyErr: auth.ErrInvalidToken}
	h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

	w := doRequest(t, h, http.MethodPost, "/api/auth/verify/bad-token", "", "")

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, errInvalidVerifyToken, decodeError(t, w))
}

func TestLoginReturnsTokenPair(t *testing.T) {
	svc := &fakeAuthService{loginPair: testTokenPair()}
	h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

	w := doRequest(t, h, http.MethodPost, "/api/auth/login", "application/json",
		`{"email":"alice@example.com","password":"correct horse battery staple"}`)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "alice@example.com", svc.loginEmail)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))

	var resp tokenResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "alice@example.com", resp.User.Email)
	require.Equal(t, "access-token", resp.AccessToken)
	require.Equal(t, "Bearer", resp.TokenType)
	require.Equal(t, int64(600), resp.ExpiresIn)
	require.Equal(t, "refresh-token", resp.RefreshToken)
	require.Equal(t, int64(2592000), resp.RefreshExpiresIn)
}

func TestLoginMapsAuthErrors(t *testing.T) {
	t.Run("invalid credentials", func(t *testing.T) {
		svc := &fakeAuthService{loginErr: auth.ErrInvalidCredentials}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequest(t, h, http.MethodPost, "/api/auth/login", "application/json",
			`{"email":"alice@example.com","password":"wrong password"}`)

		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
		require.Equal(t, codeInvalidCredentials, decodeErrorCode(t, w))
	})

	t.Run("email unverified", func(t *testing.T) {
		svc := &fakeAuthService{loginErr: auth.ErrEmailUnverified}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequest(t, h, http.MethodPost, "/api/auth/login", "application/json",
			`{"email":"alice@example.com","password":"correct horse battery staple"}`)

		require.Equal(t, http.StatusForbidden, w.Code)
		require.Equal(t, codeEmailUnverified, decodeErrorCode(t, w))
	})
}

func TestRefreshReturnsRotatedPairAndMapsInvalidToken(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		svc := &fakeAuthService{refreshPair: testTokenPair()}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequest(t, h, http.MethodPost, "/api/auth/refresh", "application/json",
			`{"refresh_token":"old-refresh"}`)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "old-refresh", svc.refreshToken)
	})

	t.Run("already used", func(t *testing.T) {
		svc := &fakeAuthService{refreshErr: auth.ErrInvalidToken}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequest(t, h, http.MethodPost, "/api/auth/refresh", "application/json",
			`{"refresh_token":"used-refresh"}`)

		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
		require.Equal(t, codeUnauthorized, decodeErrorCode(t, w))
	})
}

func TestMeRequiresBearerAndReturnsSessionEmail(t *testing.T) {
	authenticated := auth.AuthenticatedSession{
		Session: dbGen.Session{ID: 42, UserEmail: "alice@example.com"},
		User:    dbGen.User{Email: "alice@example.com"},
	}

	t.Run("missing token", func(t *testing.T) {
		svc := &fakeAuthService{}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequest(t, h, http.MethodGet, "/api/auth/me", "", "")

		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
		require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		require.Empty(t, svc.authenticateToken)
	})

	t.Run("invalid token", func(t *testing.T) {
		svc := &fakeAuthService{authenticateErr: auth.ErrInvalidToken}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequestWithHeader(t, h, http.MethodGet, "/api/auth/me", "Authorization", "Bearer expired-token")

		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
	})

	t.Run("valid token", func(t *testing.T) {
		svc := &fakeAuthService{authenticated: authenticated}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequestWithHeader(t, h, http.MethodGet, "/api/auth/me", "Authorization", "Bearer access-token")

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "access-token", svc.authenticateToken)
		require.JSONEq(t, `{"email":"alice@example.com"}`, w.Body.String())
	})
}

func TestLogoutRevokesBearerOrRefreshSession(t *testing.T) {
	authenticated := auth.AuthenticatedSession{
		Session: dbGen.Session{ID: 42, UserEmail: "alice@example.com"},
		User:    dbGen.User{Email: "alice@example.com"},
	}

	t.Run("bearer", func(t *testing.T) {
		svc := &fakeAuthService{authenticated: authenticated}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequestWithHeader(t, h, http.MethodPost, "/api/auth/logout", "Authorization", "Bearer access-token")

		require.Equal(t, http.StatusNoContent, w.Code)
		require.Equal(t, int64(42), svc.revokeSessionID)
		require.Empty(t, svc.revokeRefreshToken)
	})

	t.Run("refresh fallback after expired access token", func(t *testing.T) {
		svc := &fakeAuthService{authenticateErr: auth.ErrInvalidToken}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequestWithHeader(t, h, http.MethodPost, "/api/auth/logout", "Authorization", "Bearer expired-token")
		require.Equal(t, http.StatusUnauthorized, w.Code)

		w = doRequest(t, h, http.MethodPost, "/api/auth/logout", "application/json",
			`{"refresh_token":"refresh-token"}`)

		require.Equal(t, http.StatusNoContent, w.Code)
		require.Equal(t, "refresh-token", svc.revokeRefreshToken)
	})

	t.Run("missing credentials", func(t *testing.T) {
		svc := &fakeAuthService{}
		h, _ := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

		w := doRequest(t, h, http.MethodPost, "/api/auth/logout", "", "")

		require.Equal(t, http.StatusUnauthorized, w.Code)
		require.Equal(t, "Bearer", w.Header().Get("WWW-Authenticate"))
	})
}

func TestProtectedAuthRejectsInternalFailures(t *testing.T) {
	svc := &fakeAuthService{authenticateErr: errors.New("secret session failure")}
	h, logs := newTestHandlerWithAuth(t, &fakeSubsService{}, svc)

	w := doRequestWithHeader(t, h, http.MethodGet, "/api/auth/me", "Authorization", "Bearer access-token")

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.NotContains(t, w.Body.String(), "secret session failure")
	require.Contains(t, logs.String(), "secret session failure")
}

func doRequestWithHeader(t *testing.T, h http.Handler, method, target, name, value string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, target, nil)
	req.Header.Set(name, value)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	return w
}

func decodeErrorCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()

	var resp struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp.Code
}
