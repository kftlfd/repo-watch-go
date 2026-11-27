package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/identity"
)

type subsService interface {
	ConfirmSubscription(ctx context.Context, token string) error
	CreateSubscription(ctx context.Context, email identity.Email, repo string, verified bool) error
	DeactivateSubscription(ctx context.Context, token string) error
	DeactivateSubscriptionByRepo(ctx context.Context, email identity.Email, repo string) error
	GetSubscriptionsList(ctx context.Context, email identity.Email) ([]string, error)
}

type authService interface {
	Signup(ctx context.Context, email identity.Email, password string) error
	VerifyEmail(ctx context.Context, token string) error
	ResendVerification(ctx context.Context, email identity.Email) error
	Login(ctx context.Context, email identity.Email, password string) (auth.TokenPair, error)
	RefreshSession(ctx context.Context, refreshToken string) (auth.TokenPair, error)
	RevokeSession(ctx context.Context, sessionID int64) error
	RevokeSessionByRefreshToken(ctx context.Context, refreshToken string) error
	Authenticate(ctx context.Context, accessToken string) (auth.AuthenticatedSession, error)
}

type Deps struct {
	SubsService subsService
	AuthService authService
	DocsHost    string
}

type apiHandler struct {
	subsService subsService
	authService authService
}

func RegisterRoutes(rg *gin.RouterGroup, d Deps) error {
	docsPath := "/docs"
	specPath := "/docs/openapi.json"
	docs, err := newDocsHandler(d.DocsHost, rg.BasePath()+specPath)
	if err != nil {
		return fmt.Errorf("create docs handler: %w", err)
	}
	rg.GET(docsPath, docs.serveScalarDocs)
	rg.HEAD(docsPath, docs.serveScalarDocs)
	rg.GET(specPath, docs.serveOpenAPISpec)
	rg.HEAD(specPath, docs.serveOpenAPISpec)

	h := &apiHandler{
		subsService: d.SubsService,
		authService: d.AuthService,
	}

	subsRoutes := rg.Group("", noStore)
	subsRoutes.POST("/subscribe", auth.OptionalBearerAuth(d.AuthService, handleAuthError), h.handleCreateSubscription)
	subsRoutes.POST("/confirm/:token", h.handleConfirmSubscription)
	subsRoutes.POST("/unsubscribe/:token", h.handleDeactivateSubscription)

	subsProtectedRoutes := rg.Group("", noStore, auth.BearerAuth(d.AuthService, handleAuthError))
	subsProtectedRoutes.GET("/subscriptions", h.handleListSubscriptions)
	subsProtectedRoutes.POST("/subscriptions/unsubscribe", h.handleUnsubscribeByRepo)

	authRoutes := rg.Group("/auth", noStore)
	authRoutes.POST("/signup", h.handleSignup)
	authRoutes.POST("/verify-resend", h.handleResendVerification)
	authRoutes.POST("/verify/:token", h.handleVerifyEmail)
	authRoutes.POST("/login", h.handleLogin)
	authRoutes.POST("/refresh", h.handleRefresh)
	authRoutes.POST("/logout", h.handleLogout)
	authRoutes.GET("/me", auth.BearerAuth(d.AuthService, handleAuthError), h.handleMe)

	return nil
}

// handleAuthError maps authentication middleware failures to API responses.
func handleAuthError(c *gin.Context, err error) {
	if errors.Is(err, auth.ErrInvalidToken) {
		respondUnauthorized(c, errUnauthorized, codeUnauthorized)
		return
	}

	httpserver.RecordError(c, "authenticate request", err)
	c.JSON(http.StatusInternalServerError, errorResponse{Error: errInternal})
}

// prevents caches from retaining auth responses, tokens, or user-specific data.
func noStore(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Next()
}
