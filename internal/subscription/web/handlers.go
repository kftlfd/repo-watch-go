package web

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/identity"
	"repo-watch/internal/subscription/subs"
)

const (
	msgInvalidInput         = "Enter a valid email address and repository in owner/name format."
	msgCheckEmail           = "Check your email to confirm your subscription."
	msgSubscribed           = "Your subscription is active."
	msgConfirmed            = "Subscription confirmed."
	msgUnsubscribed         = "You have been unsubscribed."
	msgInvalidToken         = "This link is invalid."
	msgTokenUsed            = "This link is invalid or has already been used."
	msgAlreadyConfirmed     = "This subscription is already confirmed."
	msgAlreadyUnsubscribed  = "This subscription is already unsubscribed."
	msgSubscriptionNotFound = "You do not have a subscription for that repository."
	msgInternal             = "Something went wrong. Please try again later."
	msgNoSubscriptions      = "You do not have any active subscriptions."
)

type pageData struct {
	Title         string
	Message       string
	Button        string
	Error         string
	Email         string
	Repo          string
	CSRFToken     string
	SessionEmail  string
	Next          string
	Subscriptions []string
}

type subscribeForm struct {
	Email string `form:"email"`
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

	if session, ok := auth.SessionFromContext(c); ok {
		email := identity.Email(session.User.Email)
		if form.Email != "" {
			requested, err := identity.ParseEmail(form.Email)
			if err != nil || requested != email {
				h.renderSubscribeError(c, http.StatusForbidden, form, msgInvalidInput)
				return
			}
		}

		h.createSubscription(c, form, email, true)
		return
	}

	email, err := identity.ParseEmail(form.Email)
	if err != nil {
		h.renderSubscribeError(c, http.StatusBadRequest, form, msgInvalidInput)
		return
	}

	h.createSubscription(c, form, email, false)
}

func (h *webHandler) createSubscription(
	c *gin.Context,
	form subscribeForm,
	email identity.Email,
	verified bool,
) {
	if err := h.subsService.CreateSubscription(c.Request.Context(), email, form.Repo, verified); err != nil {
		status, message := subscribeErrorMessage(err)
		if status == http.StatusInternalServerError {
			httpserver.RecordError(c, "create subscription", err)
		}
		h.renderSubscribeError(c, status, form, message)
		return
	}

	if verified {
		render(c, http.StatusOK, "result.html", pageData{
			Title:   "Subscribed",
			Message: msgSubscribed,
		})
		return
	}

	render(c, http.StatusOK, "result.html", pageData{
		Title:   "Check your email",
		Message: msgCheckEmail,
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

// requireSession loads a session that required-auth middleware should have
// installed. A missing session is a middleware or route configuration error,
// not an unauthenticated user, so it is logged and rendered as a server error.
func (h *webHandler) requireSession(c *gin.Context) (auth.AuthenticatedSession, bool) {
	session, err := auth.RequireSession(c)
	if err != nil {
		httpserver.RecordError(c, "load authenticated session", err)
		render(c, http.StatusInternalServerError, "result.html", pageData{
			Title:   "Something went wrong",
			Message: msgInternal,
		})
		return auth.AuthenticatedSession{}, false
	}

	return session, true
}

func (h *webHandler) handleSubscriptionsPage(c *gin.Context) {
	h.renderSubscriptions(c, http.StatusOK, "")
}

func (h *webHandler) renderSubscriptions(c *gin.Context, status int, message string) {
	session, ok := h.requireSession(c)
	if !ok {
		return
	}

	email := identity.Email(session.User.Email)
	subscriptions, err := h.subsService.GetSubscriptionsList(c.Request.Context(), email)
	if err != nil {
		httpserver.RecordError(c, "list subscriptions", err)
		render(c, http.StatusInternalServerError, "subscriptions.html", pageData{
			Title: "Subscriptions",
			Error: msgInternal,
		})
		return
	}

	data := pageData{
		Title:         "Subscriptions",
		Error:         message,
		Subscriptions: subscriptions,
	}
	if message == "" && c.Query("unsubscribed") == "1" {
		data.Message = msgUnsubscribed
	}

	render(c, status, "subscriptions.html", data)
}

func (h *webHandler) handleUnsubscribeByRepo(c *gin.Context) {
	session, ok := h.requireSession(c)
	if !ok {
		return
	}

	email := identity.Email(session.User.Email)
	if err := h.subsService.DeactivateSubscriptionByRepo(c.Request.Context(), email, c.PostForm("repo")); err != nil {
		switch {
		case errors.Is(err, subs.ErrInvalidInput):
			h.renderSubscriptions(c, http.StatusBadRequest, msgInvalidInput)
		case errors.Is(err, sql.ErrNoRows):
			h.renderSubscriptions(c, http.StatusNotFound, msgSubscriptionNotFound)
		case errors.Is(err, subs.ErrSubscriptionAlreadyInactive):
			h.renderSubscriptions(c, http.StatusConflict, msgAlreadyUnsubscribed)
		default:
			httpserver.RecordError(c, "unsubscribe by repository", err)
			h.renderSubscriptions(c, http.StatusInternalServerError, msgInternal)
		}
		return
	}

	c.Redirect(http.StatusSeeOther, "/subscriptions?unsubscribed=1")
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
	data.CSRFToken = httpserver.GetCSRFToken(c)
	if session, ok := auth.SessionFromContext(c); ok {
		data.SessionEmail = session.User.Email
	}

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
