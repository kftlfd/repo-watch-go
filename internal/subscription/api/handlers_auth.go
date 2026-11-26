package api

import (
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/identity"
)

const (
	errInvalidAuthInput   = "invalid email or password"
	errInvalidCredentials = "invalid email or password"
	errEmailUnverified    = "email address is not verified"
	errInvalidVerifyToken = "invalid or expired verification token"
	errUnauthorized       = "unauthorized"
	msgVerificationSent   = "check email to verify your account"
	msgEmailVerified      = "email verified"

	codeInvalidCredentials = "invalid_credentials"
	codeEmailUnverified    = "email_unverified"
	codeTokenReused        = "token_reused"
	codeUnauthorized       = "unauthorized"
)

// handleSignup creates an unverified account and queues a verification email.
// The response is generic for new, unverified, and already-verified users.
//
// @Summary      Create an account
// @Description  Creates an unverified account and queues a verification email. The response does not reveal whether the email already has an account.
// @Tags         Auth
// @Accept       json,x-www-form-urlencoded
// @Produce      json
// @Param        request       body    signupRequest  true   "Email address and password"
// @Success      202  {object}  messageResponse  "Verification email queued"
// @Failure      400  {object}  errorResponse    "Malformed request or invalid email/password"
// @Failure      500  {object}  errorResponse    "Internal server error"
// @Router       /auth/signup [post]
func (h *apiHandler) handleSignup(c *gin.Context) {
	req := signupRequest{}
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	}

	email, err := identity.ParseEmail(req.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	}

	err = h.authService.Signup(c.Request.Context(), email, req.Password)
	switch {
	case err == nil:
	case errors.Is(err, auth.ErrInvalidPasswordLength):
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	default:
		h.respondInternalError(c, "signup", err)
		return
	}

	c.JSON(http.StatusAccepted, messageResponse{Message: msgVerificationSent})
}

// handleVerifyEmail verifies an account using the token from the verification
// email.
//
// @Summary      Verify an email address
// @Description  Marks the account as verified using the token from the verification email.
// @Tags         Auth
// @Produce      json
// @Param        token         path    string  true   "Verification token from the email"
// @Success      200  {object}  messageResponse  "Email verified"
// @Failure      400  {object}  errorResponse    "Invalid or expired verification token"
// @Failure      500  {object}  errorResponse    "Internal server error"
// @Router       /auth/verify/{token} [post]
func (h *apiHandler) handleVerifyEmail(c *gin.Context) {
	token := c.Param("token")

	err := h.authService.VerifyEmail(c.Request.Context(), token)
	switch {
	case err == nil:
	case errors.Is(err, auth.ErrInvalidToken):
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidVerifyToken})
		return
	default:
		h.respondInternalError(c, "verify email", err)
		return
	}

	c.JSON(http.StatusOK, messageResponse{Message: msgEmailVerified})
}

// handleResendVerification rotates and resends a verification email. The
// response is generic for unknown and already-verified accounts.
//
// @Summary      Resend a verification email
// @Description  Rotates and resends the verification email for an unverified account. The response is generic for unknown and verified accounts.
// @Tags         Auth
// @Accept       json,x-www-form-urlencoded
// @Produce      json
// @Param        request       body    resendVerificationRequest  true   "Email address"
// @Success      202  {object}  messageResponse  "Verification email queued when needed"
// @Failure      400  {object}  errorResponse    "Malformed request or invalid email"
// @Failure      500  {object}  errorResponse    "Internal server error"
// @Router       /auth/verify-resend [post]
func (h *apiHandler) handleResendVerification(c *gin.Context) {
	req := resendVerificationRequest{}
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	}

	email, err := identity.ParseEmail(req.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	}

	err = h.authService.ResendVerification(c.Request.Context(), email)
	switch {
	case err == nil:
	default:
		h.respondInternalError(c, "resend verification", err)
		return
	}

	c.JSON(http.StatusAccepted, messageResponse{Message: msgVerificationSent})
}

// handleLogin verifies credentials and returns an access/refresh token pair.
//
// @Summary      Log in
// @Description  Verifies the password and returns a short-lived Bearer access token plus a rotating refresh token.
// @Tags         Auth
// @Accept       json,x-www-form-urlencoded
// @Produce      json
// @Param        request       body    loginRequest  true   "Email address and password"
// @Success      200  {object}  tokenResponse  "Authenticated"
// @Failure      400  {object}  errorResponse  "Malformed request"
// @Failure      401  {object}  errorResponse  "Invalid credentials"
// @Failure      403  {object}  errorResponse  "Email address is not verified"
// @Failure      500  {object}  errorResponse  "Internal server error"
// @Router       /auth/login [post]
func (h *apiHandler) handleLogin(c *gin.Context) {
	req := loginRequest{}
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	}

	email, err := identity.ParseEmail(req.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	}

	pair, err := h.authService.Login(c.Request.Context(), email, req.Password)
	switch {
	case err == nil:
	case errors.Is(err, auth.ErrInvalidCredentials):
		respondUnauthorized(c, errInvalidCredentials, codeInvalidCredentials)
		return
	case errors.Is(err, auth.ErrEmailUnverified):
		c.JSON(http.StatusForbidden, errorResponse{Error: errEmailUnverified, Code: codeEmailUnverified})
		return
	default:
		h.respondInternalError(c, "login", err)
		return
	}

	c.JSON(http.StatusOK, tokenResponseFromPair(pair))
}

// handleRefresh rotates a refresh token and returns a new token pair.
//
// @Summary      Refresh a session
// @Description  Rotates the refresh token and returns a new access/refresh pair. Reuse of an old refresh token revokes the session.
// @Tags         Auth
// @Accept       json,x-www-form-urlencoded
// @Produce      json
// @Param        request       body    refreshRequest  true   "Refresh token"
// @Success      200  {object}  tokenResponse  "Rotated token pair"
// @Failure      400  {object}  errorResponse  "Malformed request"
// @Failure      401  {object}  errorResponse  "Invalid, expired, or reused refresh token"
// @Failure      500  {object}  errorResponse  "Internal server error"
// @Router       /auth/refresh [post]
func (h *apiHandler) handleRefresh(c *gin.Context) {
	req := refreshRequest{}
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	}

	pair, err := h.authService.RefreshSession(c.Request.Context(), req.RefreshToken)
	switch {
	case err == nil:
	case errors.Is(err, auth.ErrTokenReused):
		respondUnauthorized(c, errUnauthorized, codeTokenReused)
		return
	case errors.Is(err, auth.ErrInvalidToken):
		respondUnauthorized(c, errUnauthorized, codeUnauthorized)
		return
	default:
		h.respondInternalError(c, "refresh session", err)
		return
	}

	c.JSON(http.StatusOK, tokenResponseFromPair(pair))
}

// handleLogout revokes the current session. It accepts a Bearer access token,
// or a refresh token when the access token has already expired.
//
// @Summary      Log out
// @Description  Revokes the current session. Send a Bearer access token, or a refresh token in the body when the access token has expired.
// @Tags         Auth
// @Accept       json,x-www-form-urlencoded
// @Produce      json
// @Security     BearerAuth
// @Param        request       body    logoutRequest  false  "Optional refresh token when the access token has expired"
// @Success      204  "Session revoked"
// @Failure      400  {object}  errorResponse  "Malformed request"
// @Failure      401  {object}  errorResponse  "Missing or invalid credentials"
// @Failure      500  {object}  errorResponse  "Internal server error"
// @Router       /auth/logout [post]
func (h *apiHandler) handleLogout(c *gin.Context) {
	if token, ok := auth.ParseBearerToken(c.GetHeader("Authorization")); ok {
		authenticated, err := h.authService.Authenticate(c.Request.Context(), token)
		if err == nil {
			if err = h.authService.RevokeSession(c.Request.Context(), authenticated.Session.ID); err != nil {
				h.respondInternalError(c, "logout", err)
				return
			}
			c.Status(http.StatusNoContent)
			return
		}
		if !errors.Is(err, auth.ErrInvalidToken) {
			h.respondInternalError(c, "authenticate logout", err)
			return
		}
	}

	req := logoutRequest{}
	if err := c.ShouldBind(&req); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, errorResponse{Error: errInvalidAuthInput})
		return
	}

	if req.RefreshToken == "" {
		respondUnauthorized(c, errUnauthorized, codeUnauthorized)
		return
	}

	err := h.authService.RevokeSessionByRefreshToken(c.Request.Context(), req.RefreshToken)
	switch {
	case err == nil:
	case errors.Is(err, auth.ErrInvalidToken):
		respondUnauthorized(c, errUnauthorized, codeUnauthorized)
		return
	default:
		h.respondInternalError(c, "logout by refresh token", err)
		return
	}

	c.Status(http.StatusNoContent)
}

// handleMe returns the authenticated account.
//
// @Summary      Get the current account
// @Description  Returns the email address of the authenticated account.
// @Tags         Auth
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  userResponse  "Authenticated account"
// @Failure      401  {object}  errorResponse  "Missing, invalid, expired, or revoked access token"
// @Failure      500  {object}  errorResponse  "Internal server error"
// @Router       /auth/me [get]
func (h *apiHandler) handleMe(c *gin.Context) {
	authenticated, ok := auth.SessionFromContext(c)
	if !ok {
		respondUnauthorized(c, errUnauthorized, codeUnauthorized)
		return
	}

	c.JSON(http.StatusOK, userResponse{Email: authenticated.User.Email})
}

func tokenResponseFromPair(pair auth.TokenPair) tokenResponse {
	return tokenResponse{
		User:             userResponse{Email: pair.UserEmail},
		AccessToken:      pair.AccessToken,
		TokenType:        "Bearer",
		ExpiresIn:        int64(pair.AccessExpiresIn / time.Second),
		RefreshToken:     pair.RefreshToken,
		RefreshExpiresIn: int64(pair.RefreshExpiresIn / time.Second),
	}
}

func respondUnauthorized(c *gin.Context, message, code string) {
	c.Header("WWW-Authenticate", "Bearer")
	c.JSON(http.StatusUnauthorized, errorResponse{Error: message, Code: code})
}
