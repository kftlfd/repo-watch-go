package api

import (
	"context"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/subscription/api/docs"
	"repo-watch/internal/subscription/identity"
)

type subsService interface {
	ConfirmSubscription(ctx context.Context, token string) error
	CreateSubscription(ctx context.Context, email identity.Email, repo string) error
	DeactivateSubscription(ctx context.Context, token string) error
	GetSubscriptionsList(ctx context.Context, email identity.Email) ([]string, error)
}

type Deps struct {
	SubsService subsService
	DocsHost    string
}

type apiHandler struct {
	subsService subsService
}

func RegisterRoutes(rg *gin.RouterGroup, d Deps) {
	docs.SwaggerInfo.Host = d.DocsHost

	h := &apiHandler{
		subsService: d.SubsService,
	}

	rg.POST("/subscribe", h.handleCreateSubscription)
	rg.POST("/confirm/:token", h.handleConfirmSubscription)
	rg.POST("/unsubscribe/:token", h.handleDeactivateSubscription)
	rg.GET("/subscriptions", h.handleListSubscriptions)

	rg.GET("/docs", serveScalarDocs)
	rg.GET("/docs/openapi.json", serveOpenAPISpec)
}
