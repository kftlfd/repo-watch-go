package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
)

const (
	authSessionKey = "auth.authenticated_session"
	unauthorized   = "unauthorized"
	internalError  = "internal server error"
)

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

	c.Set(authSessionKey, authenticated)
	return true
}

// SessionFromContext returns the authenticated session installed by
// BearerAuth or OptionalBearerAuth.
func SessionFromContext(c *gin.Context) (AuthenticatedSession, bool) {
	value, ok := c.Get(authSessionKey)
	if !ok {
		return AuthenticatedSession{}, false
	}

	authenticated, ok := value.(AuthenticatedSession)
	return authenticated, ok
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

func abortUnauthorized(c *gin.Context) {
	c.Header("WWW-Authenticate", "Bearer")
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": unauthorized, "code": unauthorized})
}
