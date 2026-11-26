package auth

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
)

const (
	authSessionKey = "auth.authenticated_session"
	unauthorized   = "unauthorized"
	internalError  = "internal server error"
)

// setSession stores an authenticated session in the request context.
func setSession(c *gin.Context, session AuthenticatedSession) {
	c.Set(authSessionKey, session)
}

// SessionFromContext returns the authenticated session installed by
// BearerAuth, OptionalBearerAuth, or CookieAuth.
func SessionFromContext(c *gin.Context) (AuthenticatedSession, bool) {
	value, ok := c.Get(authSessionKey)
	if !ok {
		return AuthenticatedSession{}, false
	}

	authenticated, ok := value.(AuthenticatedSession)
	return authenticated, ok
}

// Authenticator validates an access token against the session store.
type Authenticator interface {
	Authenticate(ctx context.Context, accessToken string) (AuthenticatedSession, error)
}

// BearerAuth requires a valid Bearer access token and stores the authenticated
// session in the request context.
func BearerAuth(authenticator Authenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !requireBearerAuth(c, authenticator) {
			return
		}
		c.Next()
	}
}

// OptionalBearerAuth authenticates a request when an Authorization header is
// present but allows requests without one to continue anonymously.
func OptionalBearerAuth(authenticator Authenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.TrimSpace(c.GetHeader("Authorization")) != "" {
			if !requireBearerAuth(c, authenticator) {
				return
			}
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

func requireBearerAuth(c *gin.Context, authenticator Authenticator) bool {
	token, ok := ParseBearerToken(c.GetHeader("Authorization"))
	if !ok {
		abortUnauthorized(c)
		return false
	}

	authenticated, err := authenticator.Authenticate(c.Request.Context(), token)
	if errors.Is(err, ErrInvalidToken) {
		abortUnauthorized(c)
		return false
	}
	if err != nil {
		httpserver.RecordError(c, "authenticate request", err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": internalError})
		return false
	}

	setSession(c, authenticated)
	return true
}

func abortUnauthorized(c *gin.Context) {
	c.Header("WWW-Authenticate", "Bearer")
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": unauthorized, "code": unauthorized})
}

// CookieAuthenticator validates access tokens and refreshes web sessions.
type CookieAuthenticator interface {
	Authenticator
	RefreshSession(ctx context.Context, refreshToken string) (TokenPair, error)
}

// CookieAuthConfig controls cookie session behavior for web routes.
type CookieAuthConfig struct {
	Required  bool
	LoginPath string
}

// CookieAuth authenticates web requests from access and refresh cookies and
// transparently refreshes expired access tokens. Required routes redirect to
// LoginPath when no valid session can be established.
func CookieAuth(authenticator CookieAuthenticator, cookies *CookieManager, cfg CookieAuthConfig) gin.HandlerFunc {
	if cfg.LoginPath == "" {
		cfg.LoginPath = "/login"
	}

	return func(c *gin.Context) {
		if session, ok := authenticateCookies(c, authenticator, cookies); ok {
			setSession(c, session)
			c.Next()
			return
		}

		cookies.ClearSession(c)
		if cfg.Required {
			next := c.Request.URL.Path
			c.Redirect(http.StatusSeeOther, cfg.LoginPath+"?next="+url.QueryEscape(next))
			c.Abort()
			return
		}

		c.Next()
	}
}

func authenticateCookies(
	c *gin.Context,
	authenticator CookieAuthenticator,
	cookies *CookieManager,
) (AuthenticatedSession, bool) {
	ctx := c.Request.Context()

	if accessToken, ok := cookies.AccessToken(c); ok {
		session, err := authenticator.Authenticate(ctx, accessToken)
		if err == nil {
			return session, true
		}
		if !errors.Is(err, ErrInvalidToken) {
			httpserver.RecordError(c, "authenticate session cookie", err)
		}
	}

	refreshToken, ok := cookies.RefreshToken(c)
	if !ok {
		return AuthenticatedSession{}, false
	}

	pair, err := authenticator.RefreshSession(ctx, refreshToken)
	if err != nil {
		if !errors.Is(err, ErrInvalidToken) && !errors.Is(err, ErrTokenReused) {
			httpserver.RecordError(c, "refresh session cookie", err)
		}
		return AuthenticatedSession{}, false
	}

	cookies.SetSession(c, pair)

	session, err := authenticator.Authenticate(ctx, pair.AccessToken)
	if err != nil {
		if !errors.Is(err, ErrInvalidToken) {
			httpserver.RecordError(c, "authenticate refreshed session cookie", err)
		}
		cookies.ClearSession(c)
		return AuthenticatedSession{}, false
	}

	return session, true
}
