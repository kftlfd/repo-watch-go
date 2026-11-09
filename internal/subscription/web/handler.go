package web

import (
	"context"

	"github.com/gin-gonic/gin"
)

type subsService interface {
	ConfirmSubscription(ctx context.Context, token string) error
	CreateSubscription(ctx context.Context, email, repo string) error
	DeactivateSubscription(ctx context.Context, token string) error
}

type Deps struct {
	SubsService subsService
}

type webHandler struct {
	subsService subsService
}

func RegisterRoutes(rg *gin.RouterGroup, d Deps) {
	h := &webHandler{
		subsService: d.SubsService,
	}

	pages := rg.Group("")
	pages.Use(noStore)

	pages.GET("/", h.handleSubscribePage)
	pages.POST("/subscribe", h.handleCreateSubscription)

	pages.GET("/confirm/:token", h.handleConfirmPage)
	pages.POST("/confirm/:token", h.handleConfirmSubscription)

	pages.GET("/unsubscribe/:token", h.handleUnsubscribePage)
	pages.POST("/unsubscribe/:token", h.handleDeactivateSubscription)

	static := rg.Group("/static")
	static.GET("/*filepath", h.handleStatic)
}

func noStore(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Next()
}
