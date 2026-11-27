package web

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/identity"
)

const (
	msgInvalidCredentials = "Invalid email or password."
	msgSignupCheckEmail   = "Check your email to finish creating your account."
	msgSignupInvalid      = "Enter a valid email address and password."
	msgVerified           = "Your email has been verified. Log in to continue."
	msgInvalidVerifyToken = "This verification link is invalid or has expired."
	msgLogoutConfirmation = "Are you sure you want to log out?"
)

type loginForm struct {
	Email    string `form:"email" binding:"required,email"`
	Password string `form:"password" binding:"required"`
	Next     string `form:"next"`
}

type signupForm struct {
	Email    string `form:"email" binding:"required,email"`
	Password string `form:"password" binding:"required"`
}

// handleCookieAuthError redirects expected authentication failures to login and
// renders an error for internal failures without disguising them as logged out.
func (h *webHandler) handleCookieAuthError(c *gin.Context, err error) {
	if errors.Is(err, auth.ErrInvalidToken) {
		next := c.Request.URL.Path
		c.Redirect(http.StatusSeeOther, "/login?next="+url.QueryEscape(next))
		return
	}

	render(c, http.StatusInternalServerError, "result.html", pageData{
		Title:   "Could not authenticate",
		Message: msgInternal,
	})
}

func (h *webHandler) handleLoginPage(c *gin.Context) {
	data := pageData{
		Title: "Log in",
		Next:  localRedirectPath(c.Query("next")),
	}
	if c.Query("verified") == "1" {
		data.Message = msgVerified
	}

	render(c, http.StatusOK, "login.html", data)
}

func (h *webHandler) handleLogin(c *gin.Context) {
	form := loginForm{}
	if err := c.ShouldBind(&form); err != nil {
		h.renderLoginError(c, http.StatusBadRequest, form.Email, form.Next, msgInvalidCredentials)
		return
	}

	email, err := identity.ParseEmail(form.Email)
	if err != nil {
		h.renderLoginError(c, http.StatusUnauthorized, form.Email, form.Next, msgInvalidCredentials)
		return
	}

	pair, err := h.authService.Login(c.Request.Context(), email, form.Password)
	switch {
	case err == nil:
	case errors.Is(err, auth.ErrInvalidCredentials), errors.Is(err, auth.ErrEmailUnverified):
		h.renderLoginError(c, http.StatusUnauthorized, form.Email, form.Next, msgInvalidCredentials)
		return
	default:
		httpserver.RecordError(c, "web login", err)
		h.renderLoginError(c, http.StatusInternalServerError, form.Email, form.Next, msgInternal)
		return
	}

	h.cookies.SetSession(c, pair)
	if _, err = h.csrf.Rotate(c); err != nil {
		httpserver.RecordError(c, "rotate csrf token after login", err)
		h.cookies.ClearSession(c)
		h.renderLoginError(c, http.StatusInternalServerError, form.Email, form.Next, msgInternal)
		return
	}

	c.Redirect(http.StatusSeeOther, localRedirectPath(form.Next))
}

func (h *webHandler) renderLoginError(c *gin.Context, status int, email, next, message string) {
	render(c, status, "login.html", pageData{
		Title: "Log in",
		Error: message,
		Email: email,
		Next:  localRedirectPath(next),
	})
}

func (h *webHandler) handleSignupPage(c *gin.Context) {
	render(c, http.StatusOK, "signup.html", pageData{
		Title: "Sign up",
	})
}

func (h *webHandler) handleSignup(c *gin.Context) {
	form := signupForm{}
	if err := c.ShouldBind(&form); err != nil {
		h.renderSignupError(c, http.StatusBadRequest, form.Email, msgSignupInvalid)
		return
	}

	email, err := identity.ParseEmail(form.Email)
	if err != nil {
		h.renderSignupError(c, http.StatusBadRequest, form.Email, msgSignupInvalid)
		return
	}

	if err = h.authService.Signup(c.Request.Context(), email, form.Password); err != nil {
		if errors.Is(err, auth.ErrInvalidPasswordLength) {
			h.renderSignupError(c, http.StatusBadRequest, form.Email, msgSignupInvalid)
			return
		}

		httpserver.RecordError(c, "web signup", err)
		h.renderSignupError(c, http.StatusInternalServerError, form.Email, msgInternal)
		return
	}

	render(c, http.StatusOK, "result.html", pageData{
		Title:   "Check your email",
		Message: msgSignupCheckEmail,
	})
}

func (h *webHandler) renderSignupError(c *gin.Context, status int, email, message string) {
	render(c, status, "signup.html", pageData{
		Title: "Sign up",
		Error: message,
		Email: email,
	})
}

func (h *webHandler) handleLogoutPage(c *gin.Context) {
	render(c, http.StatusOK, "logout.html", pageData{
		Title:   "Log out",
		Message: msgLogoutConfirmation,
	})
}

func (h *webHandler) handleLogout(c *gin.Context) {
	if session, ok := auth.SessionFromContext(c); ok {
		if err := h.authService.RevokeSession(c.Request.Context(), session.Session.ID); err != nil {
			httpserver.RecordError(c, "web logout", err)
			render(c, http.StatusInternalServerError, "result.html", pageData{
				Title:   "Could not log out",
				Message: msgInternal,
			})
			return
		}
	}

	h.cookies.ClearSession(c)
	if _, err := h.csrf.Rotate(c); err != nil {
		httpserver.RecordError(c, "rotate csrf token after logout", err)
		render(c, http.StatusInternalServerError, "result.html", pageData{
			Title:   "Could not log out",
			Message: msgInternal,
		})
		return
	}

	c.Redirect(http.StatusSeeOther, "/")
}

func (h *webHandler) handleVerifyPage(c *gin.Context) {
	if _, ok := h.validVerificationToken(c); !ok {
		return
	}

	render(c, http.StatusOK, "token.html", pageData{
		Title:   "Verify your email",
		Message: "Click the button below to verify your email address.",
		Button:  "Verify email",
	})
}

func (h *webHandler) handleVerifyEmail(c *gin.Context) {
	token, ok := h.validVerificationToken(c)
	if !ok {
		return
	}

	if err := h.authService.VerifyEmail(c.Request.Context(), token); err != nil {
		if errors.Is(err, auth.ErrInvalidToken) {
			render(c, http.StatusBadRequest, "result.html", pageData{
				Title:   "Invalid link",
				Message: msgInvalidVerifyToken,
			})
			return
		}

		httpserver.RecordError(c, "verify email", err)
		render(c, http.StatusInternalServerError, "result.html", pageData{
			Title:   "Could not verify email",
			Message: msgInternal,
		})
		return
	}

	c.Redirect(http.StatusSeeOther, "/login?verified=1")
}

func (h *webHandler) validVerificationToken(c *gin.Context) (string, bool) {
	token := c.Param("token")
	if !auth.ValidUserVerifyToken(token) {
		render(c, http.StatusBadRequest, "result.html", pageData{
			Title:   "Invalid link",
			Message: msgInvalidVerifyToken,
		})
		return "", false
	}

	return token, true
}

// localRedirectPath returns next when it is a same-site path, /subscriptions
// when next is empty, and / for invalid paths.
func localRedirectPath(next string) string {
	if next == "" {
		return "/subscriptions"
	}
	if !strings.HasPrefix(next, "/") ||
		strings.HasPrefix(next, "//") ||
		strings.ContainsAny(next, "\\\r\n\t") {
		return "/"
	}

	u, err := url.Parse(next)
	if err != nil || u.Scheme != "" || u.Host != "" || u.Opaque != "" {
		return "/"
	}
	if !strings.HasPrefix(u.Path, "/") ||
		strings.HasPrefix(u.Path, "//") ||
		strings.ContainsAny(u.Path, "\\\r\n\t") {
		return "/"
	}

	return next
}
