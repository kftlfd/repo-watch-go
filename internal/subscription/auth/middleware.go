package auth

import (
	"context"
	"errors"
	"strings"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
)

const authSessionKey = "auth.authenticated_session"

// ErrNoSession indicates that a protected handler ran without authentication
// middleware installing a session. It signals a routing or middleware
// configuration error rather than an authentication failure.
var ErrNoSession = errors.New("authenticated session missing from request context")

// setSession stores an authenticated session in the request context.
func setSession(c *gin.Context, session AuthenticatedSession) {
	c.Set(authSessionKey, session)
}

// SessionFromContext returns the authenticated session installed by
// BearerAuth, OptionalBearerAuth, CookieAuth, or OptionalCookieAuth.
func SessionFromContext(c *gin.Context) (AuthenticatedSession, bool) {
	value, ok := c.Get(authSessionKey)
	if !ok {
		return AuthenticatedSession{}, false
	}

	authenticated, ok := value.(AuthenticatedSession)
	return authenticated, ok
}

// RequireSession returns the session installed by authentication middleware.
// A missing session returns ErrNoSession, which protected handlers should treat
// as an internal configuration error.
func RequireSession(c *gin.Context) (AuthenticatedSession, error) {
	session, ok := SessionFromContext(c)
	if !ok {
		return AuthenticatedSession{}, ErrNoSession
	}

	return session, nil
}

// AuthErrorHandler handles a request that authentication middleware could not
// authenticate. It is responsible for writing the response; the middleware
// aborts the request afterwards.
type AuthErrorHandler func(c *gin.Context, err error)

// Authenticator validates an access token against the session store.
type Authenticator interface {
	Authenticate(ctx context.Context, accessToken string) (AuthenticatedSession, error)
}

// BearerAuth requires a valid Bearer access token and stores the authenticated
// session in the request context. onError handles missing, invalid, and
// internal authentication failures.
func BearerAuth(authenticator Authenticator, onError AuthErrorHandler) gin.HandlerFunc {
	return func(c *gin.Context) {
		session, err := authenticateBearer(c, authenticator)
		if err != nil {
			onError(c, err)
			c.Abort()
			return
		}

		setSession(c, session)
		c.Next()
	}
}

// OptionalBearerAuth authenticates a request when an Authorization header is
// present but allows requests without one to continue anonymously. A present
// but invalid token is rejected via onError.
func OptionalBearerAuth(authenticator Authenticator, onError AuthErrorHandler) gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.TrimSpace(c.GetHeader("Authorization")) != "" {
			session, err := authenticateBearer(c, authenticator)
			if err != nil {
				onError(c, err)
				c.Abort()
				return
			}
			setSession(c, session)
		}
		c.Next()
	}
}

// ParseBearerToken extracts a single token from an Authorization header. The
// scheme is case-insensitive, and no credentials other than a Bearer token are
// accepted.
func ParseBearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}

	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", false
	}

	return token, true
}

func authenticateBearer(
	c *gin.Context,
	authenticator Authenticator,
) (AuthenticatedSession, error) {
	token, ok := ParseBearerToken(c.GetHeader("Authorization"))
	if !ok {
		return AuthenticatedSession{}, ErrInvalidToken
	}

	return authenticator.Authenticate(c.Request.Context(), token)
}

// CookieAuthenticator validates access tokens and refreshes web sessions.
type CookieAuthenticator interface {
	Authenticator
	RefreshSession(ctx context.Context, refreshToken string) (TokenPair, error)
}

// CookieAuth requires a valid cookie-backed session. It authenticates web
// requests from access and refresh cookies, transparently refreshes expired
// access tokens, and calls onError when no session can be established.
func CookieAuth(
	authenticator CookieAuthenticator,
	cookies *CookieManager,
	onError AuthErrorHandler,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		session, err := authenticateCookies(c, authenticator, cookies)
		if err != nil {
			if isInvalidCookieSession(err) {
				cookies.ClearSession(c)
			}
			onError(c, err)
			c.Abort()
			return
		}

		setSession(c, session)
		c.Next()
	}
}

// OptionalCookieAuth authenticates web requests when a valid cookie-backed
// session exists. Invalid credentials continue anonymously; internal failures
// are passed to onError and abort the request.
func OptionalCookieAuth(
	authenticator CookieAuthenticator,
	cookies *CookieManager,
	onError AuthErrorHandler,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		session, err := authenticateCookies(c, authenticator, cookies)
		if err != nil {
			if isInvalidCookieSession(err) {
				cookies.ClearSession(c)
				c.Next()
				return
			}
			onError(c, err)
			c.Abort()
			return
		}

		setSession(c, session)
		c.Next()
	}
}

func isInvalidCookieSession(err error) bool {
	return errors.Is(err, ErrInvalidToken)
}

func authenticateCookies(
	c *gin.Context,
	authenticator CookieAuthenticator,
	cookies *CookieManager,
) (AuthenticatedSession, error) {
	ctx := c.Request.Context()

	if accessToken, ok := cookies.AccessToken(c); ok {
		session, err := authenticator.Authenticate(ctx, accessToken)
		if err == nil {
			return session, nil
		}
		if !errors.Is(err, ErrInvalidToken) {
			httpserver.RecordError(c, "authenticate session cookie", err)
			return AuthenticatedSession{}, err
		}
	}

	refreshToken, ok := cookies.RefreshToken(c)
	if !ok {
		return AuthenticatedSession{}, ErrInvalidToken
	}

	pair, err := authenticator.RefreshSession(ctx, refreshToken)
	if err != nil {
		if !errors.Is(err, ErrInvalidToken) {
			httpserver.RecordError(c, "refresh session cookie", err)
		}
		return AuthenticatedSession{}, err
	}

	cookies.SetSession(c, pair)

	session, err := authenticator.Authenticate(ctx, pair.AccessToken)
	if err != nil {
		if !errors.Is(err, ErrInvalidToken) {
			httpserver.RecordError(c, "authenticate refreshed session cookie", err)
		}
		if isInvalidCookieSession(err) {
			cookies.ClearSession(c)
		}
		return AuthenticatedSession{}, err
	}

	return session, nil
}
