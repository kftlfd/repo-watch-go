package web

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
	"repo-watch/internal/subscription/subs"
)

const (
	msgInvalidInput = "Enter a valid email address and repository in owner/name format."
	msgCheckEmail   = "Check your email to confirm your subscription."
	msgConfirmed    = "Subscription confirmed."
	msgUnsubscribed = "You have been unsubscribed."
	msgInvalidToken = "This link is invalid."
	msgTokenUsed    = "This link is invalid or has already been used."
	msgInternal     = "Something went wrong. Please try again later."
)

type pageData struct {
	Title   string
	Message string
	Button  string
	Error   string
	Email   string
	Repo    string
}

type subscribeForm struct {
	Email string `form:"email" binding:"required,email"`
	Repo  string `form:"repo" binding:"required"`
}

func (h *webHandler) handleSubscribePage(c *gin.Context) {
	render(c, http.StatusOK, "subscribe.html", pageData{
		Title: "Subscribe",
	})
}

func (h *webHandler) handleCreateSubscription(c *gin.Context) {
	form := subscribeForm{}
	if err := c.ShouldBind(&form); err != nil {
		h.renderSubscribeError(c, http.StatusBadRequest, form, msgInvalidInput)
		return
	}

	if err := h.subsService.CreateSubscription(c.Request.Context(), form.Email, form.Repo); err != nil {
		status, message := subscribeErrorMessage(err)
		if status == http.StatusInternalServerError {
			httpserver.RecordError(c, "create subscription", err)
		}
		h.renderSubscribeError(c, status, form, message)
		return
	}

	render(c, http.StatusOK, "result.html", pageData{
		Title:   "Check your email",
		Message: msgCheckEmail,
	})
}

func (h *webHandler) handleConfirmPage(c *gin.Context) {
	if _, ok := h.validToken(c); !ok {
		return
	}

	render(c, http.StatusOK, "token.html", pageData{
		Title:   "Confirm subscription",
		Message: "Click the button below to confirm your subscription.",
		Button:  "Confirm subscription",
	})
}

func (h *webHandler) handleConfirmSubscription(c *gin.Context) {
	token, ok := h.validToken(c)
	if !ok {
		return
	}

	if err := h.subsService.ConfirmSubscription(c.Request.Context(), token); err != nil {
		status, message := tokenErrorMessage(err)
		if status == http.StatusInternalServerError {
			httpserver.RecordError(c, "confirm subscription", err)
		}
		render(c, status, "result.html", pageData{
			Title:   "Could not confirm subscription",
			Message: message,
		})
		return
	}

	render(c, http.StatusOK, "result.html", pageData{
		Title:   "Subscription confirmed",
		Message: msgConfirmed,
	})
}

func (h *webHandler) handleUnsubscribePage(c *gin.Context) {
	if _, ok := h.validToken(c); !ok {
		return
	}

	render(c, http.StatusOK, "token.html", pageData{
		Title:   "Unsubscribe",
		Message: "Click the button below to stop receiving notifications for this repository.",
		Button:  "Unsubscribe",
	})
}

func (h *webHandler) handleDeactivateSubscription(c *gin.Context) {
	token, ok := h.validToken(c)
	if !ok {
		return
	}

	if err := h.subsService.DeactivateSubscription(c.Request.Context(), token); err != nil {
		status, message := tokenErrorMessage(err)
		if status == http.StatusInternalServerError {
			httpserver.RecordError(c, "unsubscribe", err)
		}
		render(c, status, "result.html", pageData{
			Title:   "Could not unsubscribe",
			Message: message,
		})
		return
	}

	render(c, http.StatusOK, "result.html", pageData{
		Title:   "Unsubscribed",
		Message: msgUnsubscribed,
	})
}

func (h *webHandler) validToken(c *gin.Context) (string, bool) {
	token := c.Param("token")
	if !subs.ValidateToken(token) {
		render(c, http.StatusBadRequest, "result.html", pageData{
			Title:   "Invalid link",
			Message: msgInvalidToken,
		})
		return "", false
	}

	return token, true
}

func (h *webHandler) renderSubscribeError(c *gin.Context, status int, form subscribeForm, message string) {
	render(c, status, "subscribe.html", pageData{
		Title: "Subscribe",
		Error: message,
		Email: form.Email,
		Repo:  form.Repo,
	})
}

func render(c *gin.Context, status int, name string, data pageData) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(status)

	if err := templates.ExecuteTemplate(c.Writer, name, data); err != nil {
		httpserver.RecordError(c, "render template", err)
	}
}

func NotFound(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	render(c, http.StatusNotFound, "notfound.html", pageData{
		Title:   "Page not found",
		Message: "The page you requested does not exist.",
	})
}
