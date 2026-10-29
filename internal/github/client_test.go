package github_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/github"
)

func TestClient(t *testing.T) {
	t.Run("gets release", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/owner/repo/releases/latest" {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"tag_name":"v1.0.0"}`)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := github.NewClient(github.WithBaseURL(server.URL))

		tag, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
		require.Nil(t, err)
		require.Equal(t, "v1.0.0", tag)
	})

	t.Run("gets tag", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/owner/repo/tags" {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `[{"name":"v1.0.0"}]`)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := github.NewClient(github.WithBaseURL(server.URL))

		tag, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
		require.Nil(t, err)
		require.Equal(t, "v1.0.0", tag)
	})

	t.Run("returns NotFound", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := github.NewClient(github.WithBaseURL(server.URL))

		tag, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
		require.ErrorIs(t, err, github.ErrNotFound)
		require.Equal(t, "", tag)
	})
}

func TestClientSendsToken(t *testing.T) {
	var gotAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"tag_name":"v1.0.0"}`)
	}))
	defer server.Close()

	client := github.NewClient(
		github.WithBaseURL(server.URL),
		github.WithApiKey("secret-token"),
	)

	_, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
	require.NoError(t, err)
	require.Equal(t, "Bearer secret-token", gotAuth)
}

func TestClientRateLimitResponses(t *testing.T) {
	t.Run("429 with Retry-After", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer server.Close()

		client := github.NewClient(github.WithBaseURL(server.URL))

		_, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
		require.ErrorIs(t, err, github.ErrRateLimit)

		var rateErr *github.RateLimitError
		require.ErrorAs(t, err, &rateErr)
		require.Equal(t, http.StatusTooManyRequests, rateErr.StatusCode)
		require.Equal(t, 60*time.Second, rateErr.RetryAfter)
	})

	t.Run("403 primary limit with reset header", func(t *testing.T) {
		reset := time.Now().Add(90 * time.Second).Unix()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
			w.WriteHeader(http.StatusForbidden)
		}))
		defer server.Close()

		client := github.NewClient(github.WithBaseURL(server.URL))

		_, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
		require.ErrorIs(t, err, github.ErrRateLimit)

		var rateErr *github.RateLimitError
		require.ErrorAs(t, err, &rateErr)
		require.Equal(t, http.StatusForbidden, rateErr.StatusCode)
		require.Equal(t, reset, rateErr.ResetAt.Unix())
		require.InDelta(t, 90, rateErr.RetryAfter.Seconds(), 3)
	})

	t.Run("403 secondary limit with Retry-After", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusForbidden)
		}))
		defer server.Close()

		client := github.NewClient(github.WithBaseURL(server.URL))

		_, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
		require.ErrorIs(t, err, github.ErrRateLimit)

		var rateErr *github.RateLimitError
		require.ErrorAs(t, err, &rateErr)
		require.Equal(t, 5*time.Second, rateErr.RetryAfter)
	})

	t.Run("403 secondary limit with only a body message", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`)
		}))
		defer server.Close()

		client := github.NewClient(github.WithBaseURL(server.URL))

		_, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
		require.ErrorIs(t, err, github.ErrRateLimit)
	})

	t.Run("403 forbidden is not a rate limit", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"message":"Resource not accessible by integration"}`)
		}))
		defer server.Close()

		client := github.NewClient(github.WithBaseURL(server.URL))

		_, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
		require.NotErrorIs(t, err, github.ErrRateLimit)
		require.NotErrorIs(t, err, github.ErrTransientError)
		require.ErrorContains(t, err, "status 403")
	})
}

func TestClientTransientErrors(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()

			client := github.NewClient(github.WithBaseURL(server.URL))

			_, err := client.GetLatestReleaseTag(context.Background(), "owner", "repo")
			require.ErrorIs(t, err, github.ErrTransientError)
		})
	}
}
