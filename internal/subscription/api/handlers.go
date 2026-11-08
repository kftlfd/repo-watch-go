package api

import (
	"database/sql"
	"errors"
	"net/http"
	"net/mail"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/subscription/subs"
)

const (
	errInvalidEmail  = "invalid email"
	errInvalidInput  = "invalid email or repository"
	errRepoNotFound  = "repository not found"
	errAlreadyExists = "subscription already exists"
	errRateLimited   = "github rate limit exceeded"
	errUpstream      = "github upstream error"
	errInvalidToken  = "invalid token"
	errTokenNotFound = "token not found"
	errInternal      = "internal server error"
	errBodyTooLarge  = "request body too large"
	msgCheckEmail    = "check email to confirm subscription"
	msgConfirmed     = "subscription confirmed"
	msgUnsubscribed  = "unsubscribed"
)

type subscriptionResponse struct {
	Email     string `json:"email"`
	Repo      string `json:"repo"`
	Confirmed bool   `json:"confirmed"`
}

func (h *handler) handleCreateSubscription(c *gin.Context) {
	req := struct {
		Email string `json:"email" form:"email" binding:"required,email"`
		Repo  string `json:"repo" form:"repo" binding:"required"`
	}{}
	if err := c.ShouldBind(&req); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": errBodyTooLarge})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": errInvalidInput})
		return
	}

	if err := h.subsService.CreateSubscription(c.Request.Context(), req.Email, req.Repo); err != nil {
		h.respondServiceError(c, "create subscription", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": msgCheckEmail})
}

func (h *handler) handleConfirmSubscription(c *gin.Context) {
	token := c.Param("token")
	if !subs.ValidateToken(token) {
		c.JSON(http.StatusBadRequest, gin.H{"error": errInvalidToken})
		return
	}

	if err := h.subsService.ConfirmSubscription(c.Request.Context(), token); err != nil {
		h.respondTokenError(c, "confirm subscription", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": msgConfirmed})
}

func (h *handler) handleDeactivateSubscription(c *gin.Context) {
	token := c.Param("token")
	if !subs.ValidateToken(token) {
		c.JSON(http.StatusBadRequest, gin.H{"error": errInvalidToken})
		return
	}

	if err := h.subsService.DeactivateSubscription(c.Request.Context(), token); err != nil {
		h.respondTokenError(c, "unsubscribe", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": msgUnsubscribed})
}

func (h *handler) handleListSubscriptions(c *gin.Context) {
	email := c.Query("email")
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address != email {
		c.JSON(http.StatusBadRequest, gin.H{"error": errInvalidEmail})
		return
	}

	repos, err := h.subsService.GetSubscriptionsList(c.Request.Context(), email)
	if err != nil {
		h.respondInternalError(c, "list subscriptions", err)
		return
	}

	resp := make([]subscriptionResponse, len(repos))
	for i, repo := range repos {
		resp[i] = subscriptionResponse{
			Email:     email,
			Repo:      repo,
			Confirmed: true,
		}
	}

	c.JSON(http.StatusOK, resp)
}

func (h *handler) respondServiceError(c *gin.Context, operation string, err error) {
	switch {
	case errors.Is(err, subs.ErrInvalidInput):
		c.JSON(http.StatusBadRequest, gin.H{"error": errInvalidInput})
	case errors.Is(err, subs.ErrRepoNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": errRepoNotFound})
	case errors.Is(err, subs.ErrSubscriptionConflict):
		c.JSON(http.StatusConflict, gin.H{"error": errAlreadyExists})
	case errors.Is(err, subs.ErrRateLimit):
		c.JSON(http.StatusTooManyRequests, gin.H{"error": errRateLimited})
	case errors.Is(err, subs.ErrUpstreamError):
		c.JSON(http.StatusBadGateway, gin.H{"error": errUpstream})
	default:
		h.respondInternalError(c, operation, err)
	}
}

func (h *handler) respondTokenError(c *gin.Context, operation string, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": errTokenNotFound})
		return
	}

	h.respondInternalError(c, operation, err)
}

func (h *handler) respondInternalError(c *gin.Context, operation string, err error) {
	if h.logger != nil {
		h.logger.Error("request failed",
			"request_id", requestID(c),
			"operation", operation,
			"method", c.Request.Method,
			"path", c.FullPath(),
			"error", err,
		)
	}

	c.JSON(http.StatusInternalServerError, gin.H{"error": errInternal})
}
