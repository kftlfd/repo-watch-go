package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrRateLimit      = errors.New("rate limit")
	ErrTransientError = errors.New("transient error")
)

const defaultBaseURL = "https://api.github.com"

type githubClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

type clientOption func(*githubClient)

func WithBaseURL(baseURL string) clientOption {
	return func(c *githubClient) {
		c.baseURL = baseURL
	}
}

func WithApiKey(apiKey string) clientOption {
	return func(c *githubClient) {
		c.apiKey = apiKey
	}
}

func NewClient(opts ...clientOption) *githubClient {
	c := &githubClient{
		baseURL: defaultBaseURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *githubClient) setAuth(req *http.Request) {
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
}

func (c *githubClient) httpGet(ctx context.Context, reqUrl string, respData any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqUrl, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	c.setAuth(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("github request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= http.StatusInternalServerError {
			return ErrTransientError
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			return ErrRateLimit
		}

		if resp.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}

		return fmt.Errorf("github returned status %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(respData); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func (c *githubClient) getLatestRelease(ctx context.Context, owner, repo string) (string, error) {
	u, err := url.JoinPath(c.baseURL, "repos", url.PathEscape(owner), url.PathEscape(repo), "releases", "latest")
	if err != nil {
		return "", fmt.Errorf("build URL: %w", err)
	}

	tag := struct {
		TagName string `json:"tag_name"`
	}{}

	if err = c.httpGet(ctx, u, &tag); err != nil {
		return "", err
	}

	return tag.TagName, nil
}

func (c *githubClient) getLatestTag(ctx context.Context, owner, repo string) (string, error) {
	u, err := url.JoinPath(c.baseURL, "repos", url.PathEscape(owner), url.PathEscape(repo), "tags")
	if err != nil {
		return "", fmt.Errorf("build URL: %w", err)
	}

	tags := []struct {
		Name string `json:"name"`
	}{}

	if err := c.httpGet(ctx, u, &tags); err != nil {
		return "", err
	}

	if len(tags) < 1 {
		return "", nil
	}

	return tags[0].Name, nil
}

func (c *githubClient) GetLatestReleaseTag(ctx context.Context, owner, repo string) (string, error) {
	tag, err := c.getLatestRelease(ctx, owner, repo)
	if err == nil {
		return tag, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return "", err
	}

	tag, err = c.getLatestTag(ctx, owner, repo)
	if err != nil {
		return "", err
	}

	return tag, nil
}
