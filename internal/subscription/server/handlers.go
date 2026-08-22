package server

import (
	"errors"
	"net/http"

	"repo-watch/internal/subscription/subs"

	"github.com/gin-gonic/gin"
)

func (h *handler) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

func (h *handler) handleReady(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (h *handler) handleCreateSubscription(c *gin.Context) {
	req := struct {
		Email string `json:"email" binding:"required,email"`
		Repo  string `json:"repo" binding:"required"`
	}{}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := h.service.CreateSubscription(c.Request.Context(), req.Email, req.Repo)
	if err != nil {
		if errors.Is(err, subs.ErrRepoNotFound) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "GitHub repo not found"})
			return
		}

		if errors.Is(err, subs.ErrSubscriptionConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "subscription already exists"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "check email to confirm subscription"})
}

func (h *handler) handleConfirmSubscription(c *gin.Context) {
	token := c.Param("token")

	err := h.service.ConfirmSubscription(c.Request.Context(), token)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "subscription confirmed"})
}

func (h *handler) handleDeactivateSubscription(c *gin.Context) {
	token := c.Param("token")

	err := h.service.DeactivateSubscription(c.Request.Context(), token)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "unsubscribed"})
}

func (h *handler) handleListSubscriptions(c *gin.Context) {
	email := c.Query("email")
	if email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email query parameter is required"})
		return
	}

	results, err := h.service.GetSubscriptionsList(c.Request.Context(), email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, results)
}
