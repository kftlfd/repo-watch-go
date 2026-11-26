package api

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
	errNotFound             = "not found"
	errInvalidInput         = "invalid email or repository"
	errRepoNotFound         = "repository not found"
	errAlreadyExists        = "subscription already exists"
	errRateLimited          = "github rate limit exceeded"
	errUpstream             = "github upstream error"
	errInvalidToken         = "invalid token"
	errTokenNotFound        = "token not found"
	errAlreadyConfirmed     = "subscription already confirmed"
	errAlreadyUnsubscribed  = "subscription already unsubscribed"
	errEmailMismatch        = "email does not match authenticated user"
	errSubscriptionNotFound = "subscription not found"
	errInternal             = "internal server error"
	errBodyTooLarge         = "request body too large"
	msgCheckEmail           = "check email to confirm subscription"
	msgSubscribed           = "subscription active"
	msgConfirmed            = "subscription confirmed"
	msgUnsubscribed         = "unsubscribed"
)

func NotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, errorResponse{Error: errNotFound})
}

// handleCreateSubscription creates a subscription for a GitHub repository.
// Anonymous requests queue a confirmation email; Bearer requests activate the
// session user's subscription immediately.
//
// @Summary      Create a subscription
// @Description  Anonymous requests create a pending subscription and queue a confirmation email. Requests with a Bearer token subscribe the session user and activate immediately.
// @Tags         Subscriptions
// @Accept       json,x-www-form-urlencoded
// @Produce      json
// @Param        request       body    createSubscriptionRequest  true   "Email for anonymous requests and GitHub repository; authenticated requests may omit email"
// @Success      200  {object}  messageResponse  "Confirmation email queued or subscription activated"
// @Failure      400  {object}  errorResponse    "Malformed request body or invalid email/repository"
// @Failure      401  {object}  errorResponse    "Present but invalid Bearer token"
// @Failure      403  {object}  errorResponse    "Supplied email is invalid or does not match the authenticated user"
// @Failure      404  {object}  errorResponse    "Repository not found on GitHub"
// @Failure      409  {object}  errorResponse    "An active subscription already exists"
// @Failure      413  {object}  errorResponse    "Request body exceeds the 1 MiB limit"
// @Failure      429  {object}  errorResponse    "GitHub rate limit exceeded"
// @Failure      500  {object}  errorResponse    "Internal server error"
// @Failure      502  {object}  errorResponse    "GitHub upstream error"
// @Router       /subscribe [post]
func (h *apiHandler) handleCreateSubscription(c *gin.Context) {
	req := createSubscriptionRequest{}
	if err := c.ShouldBind(&req); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			c.JSON(http.StatusRequestEntityTooLarge, errorResponse{Error: errBodyTooLarge})
			return
		}
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidInput})
		return
	}

	if session, ok := auth.SessionFromContext(c); ok {
		email := identity.Email(session.User.Email)
		if req.Email != "" {
			requested, _ := identity.ParseEmail(req.Email)
			if requested != email {
				c.JSON(http.StatusForbidden, errorResponse{Error: errEmailMismatch})
				return
			}
		}

		if err := h.subsService.CreateSubscription(c.Request.Context(), email, req.Repo, true); err != nil {
			h.respondServiceError(c, "create subscription", err)
			return
		}

		c.JSON(http.StatusOK, messageResponse{Message: msgSubscribed})
		return
	}

	email, err := identity.ParseEmail(req.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidInput})
		return
	}

	if err := h.subsService.CreateSubscription(c.Request.Context(), email, req.Repo, false); err != nil {
		h.respondServiceError(c, "create subscription", err)
		return
	}

	c.JSON(http.StatusOK, messageResponse{Message: msgCheckEmail})
}

// handleConfirmSubscription activates a subscription using the token from the
// confirmation email
//
// @Summary      Confirm a subscription
// @Description  Confirms a subscription using the token from the confirmation email.
// @Tags         Subscriptions
// @Produce      json
// @Param        token         path    string  true   "Confirmation token from the email"
// @Success      200  {object}  messageResponse  "Subscription confirmed"
// @Failure      400  {object}  errorResponse    "Malformed token"
// @Failure      404  {object}  errorResponse    "Token not found or expired"
// @Failure      409  {object}  errorResponse    "Subscription is already active"
// @Failure      500  {object}  errorResponse    "Internal server error"
// @Router       /confirm/{token} [post]
func (h *apiHandler) handleConfirmSubscription(c *gin.Context) {
	token := c.Param("token")
	if !subs.ValidateToken(token) {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidToken})
		return
	}

	if err := h.subsService.ConfirmSubscription(c.Request.Context(), token); err != nil {
		h.respondTokenError(c, "confirm subscription", err)
		return
	}

	c.JSON(http.StatusOK, messageResponse{Message: msgConfirmed})
}

// handleDeactivateSubscription deactivates a subscription using the
// unsubscribe token
//
// @Summary      Unsubscribe from a repository
// @Description  Deactivates a subscription using the unsubscribe token.
// @Tags         Subscriptions
// @Produce      json
// @Param        token         path    string  true   "Unsubscribe token from the email (base64url-encoded, 16 bytes)"
// @Success      200  {object}  messageResponse  "Subscription deactivated"
// @Failure      400  {object}  errorResponse    "Malformed token"
// @Failure      404  {object}  errorResponse    "Token not found"
// @Failure      409  {object}  errorResponse    "Subscription is already inactive"
// @Failure      500  {object}  errorResponse    "Internal server error"
// @Router       /unsubscribe/{token} [post]
func (h *apiHandler) handleDeactivateSubscription(c *gin.Context) {
	token := c.Param("token")
	if !subs.ValidateToken(token) {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidToken})
		return
	}

	if err := h.subsService.DeactivateSubscription(c.Request.Context(), token); err != nil {
		h.respondTokenError(c, "unsubscribe", err)
		return
	}

	c.JSON(http.StatusOK, messageResponse{Message: msgUnsubscribed})
}

// handleListSubscriptions returns the active subscriptions for the
// authenticated user.
//
// @Summary      List active subscriptions
// @Description  Returns the authenticated user's active subscriptions.
// @Tags         Subscriptions
// @Produce      json
// @Security     BearerAuth
// @Success      200  {array}   subscriptionResponse  "Active subscriptions; an empty array is returned when none exist"
// @Failure      401  {object}  errorResponse          "Missing, invalid, expired, or revoked access token"
// @Failure      500  {object}  errorResponse          "Internal server error"
// @Router       /subscriptions [get]
func (h *apiHandler) handleListSubscriptions(c *gin.Context) {
	session, ok := auth.SessionFromContext(c)
	if !ok {
		respondUnauthorized(c, errUnauthorized, codeUnauthorized)
		return
	}

	email := identity.Email(session.User.Email)
	repos, err := h.subsService.GetSubscriptionsList(c.Request.Context(), email)
	if err != nil {
		h.respondInternalError(c, "list subscriptions", err)
		return
	}

	resp := make([]subscriptionResponse, len(repos))
	for i, repo := range repos {
		resp[i] = subscriptionResponse{
			Repo:      repo,
			Confirmed: true,
		}
	}

	c.JSON(http.StatusOK, resp)
}

// handleUnsubscribeByRepo deactivates the authenticated user's subscription to
// a repository.
//
// @Summary      Unsubscribe from a repository
// @Description  Deactivates the authenticated user's active subscription to the given repository.
// @Tags         Subscriptions
// @Accept       json,x-www-form-urlencoded
// @Produce      json
// @Security     BearerAuth
// @Param        request       body    unsubscribeRequest  true   "GitHub repository"
// @Success      200  {object}  messageResponse  "Subscription deactivated"
// @Failure      400  {object}  errorResponse    "Malformed request or invalid repository"
// @Failure      401  {object}  errorResponse    "Missing, invalid, expired, or revoked access token"
// @Failure      404  {object}  errorResponse    "Subscription not found"
// @Failure      409  {object}  errorResponse    "Subscription is already inactive"
// @Failure      500  {object}  errorResponse    "Internal server error"
// @Router       /subscriptions/unsubscribe [post]
func (h *apiHandler) handleUnsubscribeByRepo(c *gin.Context) {
	session, ok := auth.SessionFromContext(c)
	if !ok {
		respondUnauthorized(c, errUnauthorized, codeUnauthorized)
		return
	}

	req := unsubscribeRequest{}
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidInput})
		return
	}

	email := identity.Email(session.User.Email)
	if err := h.subsService.DeactivateSubscriptionByRepo(c.Request.Context(), email, req.Repo); err != nil {
		switch {
		case errors.Is(err, subs.ErrInvalidInput):
			c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidInput})
		case errors.Is(err, sql.ErrNoRows):
			c.JSON(http.StatusNotFound, errorResponse{Error: errSubscriptionNotFound})
		case errors.Is(err, subs.ErrSubscriptionAlreadyInactive):
			c.JSON(http.StatusConflict, errorResponse{Error: errAlreadyUnsubscribed})
		default:
			h.respondInternalError(c, "unsubscribe", err)
		}
		return
	}

	c.JSON(http.StatusOK, messageResponse{Message: msgUnsubscribed})
}

func (h *apiHandler) respondServiceError(c *gin.Context, operation string, err error) {
	switch {
	case errors.Is(err, subs.ErrInvalidInput):
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidInput})
	case errors.Is(err, subs.ErrRepoNotFound):
		c.JSON(http.StatusNotFound, errorResponse{Error: errRepoNotFound})
	case errors.Is(err, subs.ErrSubscriptionConflict):
		c.JSON(http.StatusConflict, errorResponse{Error: errAlreadyExists})
	case errors.Is(err, subs.ErrRateLimit):
		c.JSON(http.StatusTooManyRequests, errorResponse{Error: errRateLimited})
	case errors.Is(err, subs.ErrUpstreamError):
		c.JSON(http.StatusBadGateway, errorResponse{Error: errUpstream})
	default:
		h.respondInternalError(c, operation, err)
	}
}

func (h *apiHandler) respondTokenError(c *gin.Context, operation string, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		c.JSON(http.StatusNotFound, errorResponse{Error: errTokenNotFound})
	case errors.Is(err, subs.ErrSubscriptionAlreadyActive):
		c.JSON(http.StatusConflict, errorResponse{Error: errAlreadyConfirmed})
	case errors.Is(err, subs.ErrSubscriptionAlreadyInactive):
		c.JSON(http.StatusConflict, errorResponse{Error: errAlreadyUnsubscribed})
	default:
		h.respondInternalError(c, operation, err)
	}
}

func (h *apiHandler) respondInternalError(c *gin.Context, operation string, err error) {
	httpserver.RecordError(c, operation, err)
	c.JSON(http.StatusInternalServerError, errorResponse{Error: errInternal})
}
