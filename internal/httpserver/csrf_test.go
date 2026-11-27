package httpserver

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func testCSRF(t *testing.T) *CSRF {
	t.Helper()

	csrf, err := NewCSRF(CSRFConfig{
		Secret:     []byte(strings.Repeat("s", 32)),
		CookieName: "rw_csrf",
		AppOrigin:  "http://example.test",
	})
	require.NoError(t, err)
	return csrf
}

func testCSRFRouter(csrf *CSRF) *gin.Engine {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(csrf.Middleware())
	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, GetCSRFToken(c))
	})
	router.POST("/", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	return router
}

func csrfTokenFromResponse(t *testing.T, w *httptest.ResponseRecorder) (*http.Cookie, string) {
	t.Helper()

	cookie := findCSRFCookie(t, w)
	return cookie, cookie.Value
}

func findCSRFCookie(t *testing.T, w *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()

	for _, cookie := range w.Result().Cookies() {
		if strings.Contains(cookie.Name, "csrf") {
			return cookie
		}
	}

	t.Fatal("csrf cookie not found")
	return nil
}

func TestNewCSRFRejectsInvalidConfig(t *testing.T) {
	_, err := NewCSRF(CSRFConfig{Secret: []byte("short"), AppOrigin: "http://example.test"})
	require.ErrorIs(t, err, ErrWeakCSRFSecret)

	_, err = NewCSRF(CSRFConfig{Secret: []byte(strings.Repeat("s", 32))})
	require.ErrorContains(t, err, "app origin")

	_, err = NewCSRF(CSRFConfig{
		Secret: []byte(strings.Repeat("s", 32)), AppOrigin: "ftp://example.test",
	})
	require.ErrorContains(t, err, "invalid csrf app origin")

	_, err = NewCSRF(CSRFConfig{
		Secret: []byte(strings.Repeat("s", 32)), AppOrigin: "https://example.test/path",
	})
	require.ErrorContains(t, err, "invalid csrf app origin")
}

func TestNewCSRFDerivesSecureConfigFromBaseURL(t *testing.T) {
	csrf, err := NewCSRF(CSRFConfig{
		Secret:  []byte(strings.Repeat("s", 32)),
		BaseURL: "https://Example.test/some/path",
	})
	require.NoError(t, err)
	require.Equal(t, "__Host-rw_csrf", csrf.CookieName())
	require.True(t, csrf.secure)
	require.Equal(t, "https://example.test:443", csrf.appOrigin)
}

func TestNewCSRFRejectsInvalidBaseURL(t *testing.T) {
	for _, baseURL := range []string{"not-a-url", "ftp://example.test", "https://user@example.test"} {
		_, err := NewCSRF(CSRFConfig{
			Secret:  []byte(strings.Repeat("s", 32)),
			BaseURL: baseURL,
		})
		require.ErrorContains(t, err, "invalid csrf base URL")
	}
}

func TestCSRFOriginCanonicalizesDefaultPorts(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		origin  string
		want    bool
	}{
		{name: "https config explicit port", baseURL: "https://example.test:443", origin: "https://example.test", want: true},
		{name: "https origin explicit port", baseURL: "https://example.test", origin: "https://example.test:443", want: true},
		{name: "http config explicit port", baseURL: "http://example.test:80", origin: "http://example.test", want: true},
		{name: "case insensitive", baseURL: "HTTPS://EXAMPLE.TEST", origin: "https://example.test:443", want: true},
		{name: "wrong port", baseURL: "https://example.test:444", origin: "https://example.test", want: false},
		{name: "wrong scheme", baseURL: "https://example.test", origin: "http://example.test", want: false},
		{name: "origin path", baseURL: "https://example.test", origin: "https://example.test/path", want: false},
		{name: "origin userinfo", baseURL: "https://example.test", origin: "https://user@example.test", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			csrf, err := NewCSRF(CSRFConfig{
				Secret: []byte(strings.Repeat("s", 32)), BaseURL: tt.baseURL,
			})
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("Origin", tt.origin)
			require.Equal(t, tt.want, csrf.validRequestOrigin(req))
		})
	}
}

func TestCSRFMiddlewareSetsTokenOnSafeRequest(t *testing.T) {
	csrf := testCSRF(t)
	router := testCSRFRouter(csrf)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Equal(t, http.StatusOK, w.Code)
	cookie, token := csrfTokenFromResponse(t, w)
	require.NotEmpty(t, token)
	require.True(t, csrf.Validate(token))

	bodyToken := strings.TrimSpace(w.Body.String())
	require.Equal(t, token, bodyToken)

	require.False(t, cookie.HttpOnly)
	require.False(t, cookie.Secure)
	require.Equal(t, "/", cookie.Path)
	require.Equal(t, http.SameSiteLaxMode, cookie.SameSite)
}

func TestCSRFMiddlewareSetsSecureCookie(t *testing.T) {
	csrf, err := NewCSRF(CSRFConfig{
		Secret:     []byte(strings.Repeat("s", 32)),
		CookieName: "__Host-rw_csrf",
		Secure:     true,
		AppOrigin:  "https://example.test",
	})
	require.NoError(t, err)

	router := testCSRFRouter(csrf)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Equal(t, http.StatusOK, w.Code)
	cookie := findCSRFCookie(t, w)
	require.Equal(t, "__Host-rw_csrf", cookie.Name)
	require.True(t, cookie.Secure)
	require.False(t, cookie.HttpOnly)
}

func TestCSRFMiddlewareRejectsInvalidUnsafeRequests(t *testing.T) {
	csrf := testCSRF(t)
	router := testCSRFRouter(csrf)

	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/", nil))
	cookie, token := csrfTokenFromResponse(t, getRec)

	tests := []struct {
		name      string
		cookie    *http.Cookie
		token     string
		origin    string
		fetchSite string
		want      int
	}{
		{name: "missing everything", want: http.StatusForbidden},
		{name: "cookie only", cookie: cookie, want: http.StatusForbidden},
		{name: "mismatched token", cookie: cookie, token: "wrong", want: http.StatusForbidden},
		{name: "cross origin", cookie: cookie, token: token, origin: "http://evil.test", want: http.StatusForbidden},
		{name: "cross site", cookie: cookie, token: token, fetchSite: "cross-site", want: http.StatusForbidden},
		{name: "valid header", cookie: cookie, token: token, want: http.StatusOK},
		{name: "valid same origin", cookie: cookie, token: token, origin: "http://example.test", fetchSite: "same-origin", want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.cookie != nil {
				req.AddCookie(tt.cookie)
			}
			if tt.token != "" {
				req.Header.Set(CSRFHeader, tt.token)
			}
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			if tt.fetchSite != "" {
				req.Header.Set("Sec-Fetch-Site", tt.fetchSite)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, tt.want, w.Code)
		})
	}
}

func TestCSRFMiddlewareAcceptsFormField(t *testing.T) {
	csrf := testCSRF(t)
	router := testCSRFRouter(csrf)

	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/", nil))
	cookie, token := csrfTokenFromResponse(t, getRec)

	form := url.Values{CSRFFormField: {token}}.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestCSRFRotateChangesToken(t *testing.T) {
	csrf := testCSRF(t)
	router := testCSRFRouter(csrf)

	first := httptest.NewRecorder()
	router.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/", nil))
	_, firstToken := csrfTokenFromResponse(t, first)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	secondToken, err := csrf.Rotate(c)
	require.NoError(t, err)
	require.NotEqual(t, firstToken, secondToken)
	require.True(t, csrf.Validate(secondToken))
}
