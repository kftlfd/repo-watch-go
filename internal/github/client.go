package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrRateLimit      = errors.New("rate limit")
	ErrTransientError = errors.New("transient error")
)

const defaultBaseURL = "https://api.github.com"

// RateLimitError describes a GitHub rate-limit response. It wraps ErrRateLimit
// so callers can either use errors.Is for classification or errors.As to read
// the retry hint.
type RateLimitError struct {
	StatusCode int
	RetryAfter time.Duration
	ResetAt    time.Time
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("github rate limit (status %d, retry after %s)",
			e.StatusCode, e.RetryAfter.Round(time.Second))
	}
	if !e.ResetAt.IsZero() {
		return fmt.Sprintf("github rate limit (status %d, reset at %s)",
			e.StatusCode, e.ResetAt.UTC().Format(time.RFC3339))
	}
	return fmt.Sprintf("github rate limit (status %d)", e.StatusCode)
}

func (e *RateLimitError) Unwrap() error {
	return ErrRateLimit
}

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
	if c.baseURL == "" {
		c.baseURL = defaultBaseURL
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
		return classifyHTTPError(resp)
	}

	if err := json.NewDecoder(resp.Body).Decode(respData); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func classifyHTTPError(resp *http.Response) error {
	switch resp.StatusCode {
	case http.StatusNotFound:
		return ErrNotFound

	case http.StatusTooManyRequests:
		return newRateLimitError(resp)

	case http.StatusForbidden:
		if isRateLimitResponse(resp) {
			return newRateLimitError(resp)
		}
		return fmt.Errorf("github returned status %d", resp.StatusCode)

	default:
		if resp.StatusCode >= http.StatusInternalServerError {
			return fmt.Errorf("%w: status %d", ErrTransientError, resp.StatusCode)
		}
		return fmt.Errorf("github returned status %d", resp.StatusCode)
	}
}

// isRateLimitResponse recognizes both primary and secondary rate limits. GitHub
// reports primary limits with X-RateLimit-Remaining: 0 and secondary limits
// with 403, sometimes only with a message in the body.
func isRateLimitResponse(resp *http.Response) bool {
	if resp.Header.Get("Retry-After") != "" {
		return true
	}
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		return true
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return false
	}

	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}

	return strings.Contains(strings.ToLower(payload.Message), "rate limit")
}

func newRateLimitError(resp *http.Response) *RateLimitError {
	retryErr := &RateLimitError{StatusCode: resp.StatusCode}
	now := time.Now()

	if value := resp.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
			retryErr.RetryAfter = time.Duration(seconds) * time.Second
		} else if retryAt, err := http.ParseTime(value); err == nil {
			retryErr.RetryAfter = retryAt.Sub(now)
		}
	}

	if value := resp.Header.Get("X-RateLimit-Reset"); value != "" {
		if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
			retryErr.ResetAt = time.Unix(seconds, 0)
			if retryErr.RetryAfter <= 0 {
				retryErr.RetryAfter = retryErr.ResetAt.Sub(now)
			}
		}
	}

	if retryErr.RetryAfter < 0 {
		retryErr.RetryAfter = 0
	}

	return retryErr
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
