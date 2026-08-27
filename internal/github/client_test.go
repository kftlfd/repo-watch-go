package github_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"repo-watch/pkg/github"
)

func TestClient(t *testing.T) {
	t.Run("client is consistent with interface", func(t *testing.T) {
		var client github.Client = github.NewClient()
		require.NotNil(t, client)
	})

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
