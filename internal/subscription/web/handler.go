package web

import (
	"context"

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
	Login(ctx context.Context, email identity.Email, password string) (auth.TokenPair, error)
	RefreshSession(ctx context.Context, refreshToken string) (auth.TokenPair, error)
	Authenticate(ctx context.Context, accessToken string) (auth.AuthenticatedSession, error)
	RevokeSession(ctx context.Context, sessionID int64) error
}

type Deps struct {
	SubsService subsService
	AuthService authService
	Cookies     *auth.CookieManager
	CSRF        *httpserver.CSRF
}

type webHandler struct {
	subsService subsService
	authService authService
	cookies     *auth.CookieManager
	csrf        *httpserver.CSRF
}

func RegisterRoutes(rg *gin.RouterGroup, d Deps) {
	h := &webHandler{
		subsService: d.SubsService,
		authService: d.AuthService,
		cookies:     d.Cookies,
		csrf:        d.CSRF,
	}

	base := rg.Group("")
	base.Use(noStore)
	base.Use(d.CSRF.Middleware())

	pages := base.Group("")
	pages.Use(auth.OptionalCookieAuth(h.authService, h.cookies))

	pages.GET("/", h.handleSubscribePage)
	pages.POST("/subscribe", h.handleCreateSubscription)

	pages.GET("/confirm/:token", h.handleConfirmPage)
	pages.POST("/confirm/:token", h.handleConfirmSubscription)

	pages.GET("/unsubscribe/:token", h.handleUnsubscribePage)
	pages.POST("/unsubscribe/:token", h.handleDeactivateSubscription)

	pages.GET("/signup", h.handleSignupPage)
	pages.POST("/signup", h.handleSignup)

	pages.GET("/login", h.handleLoginPage)
	pages.POST("/login", h.handleLogin)

	pages.GET("/logout", h.handleLogoutPage)
	pages.POST("/logout", h.handleLogout)

	pages.GET("/verify/:token", h.handleVerifyPage)
	pages.POST("/verify/:token", h.handleVerifyEmail)

	protected := base.Group("")
	protected.Use(auth.CookieAuth(h.authService, h.cookies, h.redirectToLogin))

	protected.GET("/subscriptions", h.handleSubscriptionsPage)
	protected.POST("/subscriptions/unsubscribe", h.handleUnsubscribeByRepo)

	static := rg.Group("/static")
	static.GET("/*filepath", h.handleStatic)
	static.HEAD("/*filepath", h.handleStatic)
}

func noStore(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Next()
}
