package web

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"repo-watch/internal/httpserver"
	"repo-watch/internal/subscription/auth"
	"repo-watch/internal/subscription/identity"
)

const msgInvalidCredentials = "Invalid email or password."

type loginForm struct {
	Email    string `form:"email" binding:"required,email"`
	Password string `form:"password" binding:"required"`
}

func (h *webHandler) handleLogin(c *gin.Context) {
	form := loginForm{}
	if err := c.ShouldBind(&form); err != nil {
		render(c, http.StatusUnauthorized, "result.html", pageData{
			Title:   "Could not log in",
			Message: msgInvalidCredentials,
		})
		return
	}

	email, err := identity.ParseEmail(form.Email)
	if err != nil {
		render(c, http.StatusUnauthorized, "result.html", pageData{
			Title:   "Could not log in",
			Message: msgInvalidCredentials,
		})
		return
	}

	pair, err := h.authService.Login(c.Request.Context(), email, form.Password)
	switch {
	case err == nil:
	case errors.Is(err, auth.ErrInvalidCredentials), errors.Is(err, auth.ErrEmailUnverified):
		render(c, http.StatusUnauthorized, "result.html", pageData{
			Title:   "Could not log in",
			Message: msgInvalidCredentials,
		})
		return
	default:
		httpserver.RecordError(c, "web login", err)
		render(c, http.StatusInternalServerError, "result.html", pageData{
			Title:   "Could not log in",
			Message: msgInternal,
		})
		return
	}

	h.cookies.SetSession(c, pair)
	if _, err = h.csrf.Rotate(c); err != nil {
		httpserver.RecordError(c, "rotate csrf token after login", err)
		h.cookies.ClearSession(c)
		render(c, http.StatusInternalServerError, "result.html", pageData{
			Title:   "Could not log in",
			Message: msgInternal,
		})
		return
	}

	c.Redirect(http.StatusSeeOther, "/")
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
