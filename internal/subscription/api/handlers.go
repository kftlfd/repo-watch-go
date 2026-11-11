package api

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
	"repo-watch/internal/subscription/subs"
)

const (
	errNotFound            = "not found"
	errInvalidEmail        = "invalid email"
	errInvalidInput        = "invalid email or repository"
	errRepoNotFound        = "repository not found"
	errAlreadyExists       = "subscription already exists"
	errRateLimited         = "github rate limit exceeded"
	errUpstream            = "github upstream error"
	errInvalidToken        = "invalid token"
	errTokenNotFound       = "token not found"
	errAlreadyConfirmed    = "subscription already confirmed"
	errAlreadyUnsubscribed = "subscription already unsubscribed"
	errInternal            = "internal server error"
	errBodyTooLarge        = "request body too large"
	msgCheckEmail          = "check email to confirm subscription"
	msgConfirmed           = "subscription confirmed"
	msgUnsubscribed        = "unsubscribed"
)

func NotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, errorResponse{Error: errNotFound})
}

// handleCreateSubscription creates a subscription for an email address and
// GitHub repository, then queues a confirmation email
//
// @Summary      Create a subscription
// @Description  Creates a pending subscription for a GitHub repository and queues a confirmation email.
// @Tags         Subscriptions
// @Accept       json,x-www-form-urlencoded
// @Produce      json
// @Param        request       body    createSubscriptionRequest  true   "Email address and GitHub repository to subscribe to"
// @Success      200  {object}  messageResponse  "Confirmation email queued"
// @Failure      400  {object}  errorResponse    "Malformed request body or invalid email/repository"
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

	if err := h.subsService.CreateSubscription(c.Request.Context(), req.Email, req.Repo); err != nil {
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

// handleListSubscriptions returns the active subscriptions for an email
//
// @Summary      List active subscriptions
// @Description  Returns all active subscriptions for the email address (case-insensitive).
// @Tags         Subscriptions
// @Produce      json
// @Param        email         query   string  true   "Email address (bare address only, for example alice@example.com)"
// @Success      200  {array}   subscriptionResponse  "Active subscriptions; an empty array is returned when none exist"
// @Failure      400  {object}  errorResponse          "Missing or invalid email"
// @Failure      500  {object}  errorResponse          "Internal server error"
// @Router       /subscriptions [get]
func (h *apiHandler) handleListSubscriptions(c *gin.Context) {
	email, err := subs.NormalizeEmail(c.Query("email"))
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidEmail})
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
