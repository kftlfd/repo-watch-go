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
		token, ok := ParseBearerToken(c.GetHeader("Authorization"))
		if !ok {
			abortUnauthorized(c)
			return
		}

		authenticated, err := authenticator.Authenticate(c.Request.Context(), token)
		if errors.Is(err, ErrInvalidToken) {
			abortUnauthorized(c)
			return
		}
		if err != nil {
			httpserver.RecordError(c, "authenticate request", err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": internalError})
			return
		}

		c.Set(authSessionKey, authenticated)
		c.Next()
	}
}

// SessionFromContext returns the authenticated session installed by
// BearerAuth.
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
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"error": unauthorized,
		"code":  unauthorized,
	})
}
