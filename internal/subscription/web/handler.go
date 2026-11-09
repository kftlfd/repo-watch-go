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

	rg.GET("/", h.handleSubscribePage)
	rg.POST("/subscribe", h.handleCreateSubscription)

	rg.GET("/confirm/:token", h.handleConfirmPage)
	rg.POST("/confirm/:token", h.handleConfirmSubscription)

	rg.GET("/unsubscribe/:token", h.handleUnsubscribePage)
	rg.POST("/unsubscribe/:token", h.handleDeactivateSubscription)
}
