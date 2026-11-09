package web

import (
	"database/sql"
	"errors"
	"net/http"

	"repo-watch/internal/subscription/subs"
)

func subscribeErrorMessage(err error) (int, string) {
	switch {
	case errors.Is(err, subs.ErrInvalidInput):
		return http.StatusBadRequest, msgInvalidInput
	case errors.Is(err, subs.ErrRepoNotFound):
		return http.StatusNotFound, "That repository was not found on GitHub."
	case errors.Is(err, subs.ErrSubscriptionConflict):
		return http.StatusConflict, "You already have an active subscription to this repository."
	case errors.Is(err, subs.ErrRateLimit):
		return http.StatusTooManyRequests, "GitHub rate limit exceeded. Please try again later."
	case errors.Is(err, subs.ErrUpstreamError):
		return http.StatusBadGateway, "GitHub is unavailable right now. Please try again later."
	default:
		return http.StatusInternalServerError, msgInternal
	}
}

func tokenErrorMessage(err error) (int, string) {
	if errors.Is(err, sql.ErrNoRows) {
		return http.StatusNotFound, msgTokenUsed
	}

	return http.StatusInternalServerError, msgInternal
}
