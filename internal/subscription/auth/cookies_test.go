package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestCookieNamesAndSecureFlags(t *testing.T) {
	tests := []struct {
		name                    string
		baseURL                 string
		access, refresh         string
		secure                  bool
		accessName, refreshName string
	}{
		{
			name:        "development defaults",
			baseURL:     "http://localhost:3000",
			secure:      false,
			accessName:  "rw_access",
			refreshName: "rw_refresh",
		},
		{
			name:        "secure defaults",
			baseURL:     "https://repo-watch.example.com",
			secure:      true,
			accessName:  "__Host-rw_access",
			refreshName: "__Host-rw_refresh",
		},
		{
			name:        "custom secure names",
			baseURL:     "https://repo-watch.example.com",
			access:      "custom_access",
			refresh:     "custom_refresh",
			secure:      true,
			accessName:  "__Host-custom_access",
			refreshName: "__Host-custom_refresh",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewCookieManager(CookieConfig{
				AccessName:  tt.access,
				RefreshName: tt.refresh,
				BaseURL:     tt.baseURL,
			})

			require.Equal(t, tt.accessName, manager.AccessName())
			require.Equal(t, tt.refreshName, manager.RefreshName())
			require.Equal(t, tt.secure, secureCookies(tt.baseURL))

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			manager.SetSession(c, TokenPair{
				AccessToken:      "access",
				RefreshToken:     "refresh",
				AccessExpiresIn:  10 * time.Minute,
				RefreshExpiresIn: 30 * 24 * time.Hour,
			})

			cookies := w.Result().Cookies()
			require.Len(t, cookies, 2)

			access := findCookie(t, cookies, tt.accessName)
			require.Equal(t, "access", access.Value)
			require.True(t, access.HttpOnly)
			require.Equal(t, tt.secure, access.Secure)
			require.Equal(t, "/", access.Path)
			require.Equal(t, http.SameSiteLaxMode, access.SameSite)
			require.Equal(t, 600, access.MaxAge)

			refresh := findCookie(t, cookies, tt.refreshName)
			require.Equal(t, "refresh", refresh.Value)
			require.True(t, refresh.HttpOnly)
			require.Equal(t, 2592000, refresh.MaxAge)
		})
	}
}

func TestCookieManagerReadsAndClearsSession(t *testing.T) {
	tests := []struct {
		name   string
		secure bool
	}{
		{name: "development", secure: false},
		{name: "secure", secure: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewCookieManager(CookieConfig{Secure: tt.secure})

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			c.Request.AddCookie(&http.Cookie{Name: manager.AccessName(), Value: "access"})
			c.Request.AddCookie(&http.Cookie{Name: manager.RefreshName(), Value: "refresh"})

			access, ok := manager.AccessToken(c)
			require.True(t, ok)
			require.Equal(t, "access", access)

			refresh, ok := manager.RefreshToken(c)
			require.True(t, ok)
			require.Equal(t, "refresh", refresh)

			manager.ClearSession(c)
			cookies := w.Result().Cookies()
			require.Len(t, cookies, 2)
			for _, cookie := range cookies {
				require.Less(t, cookie.MaxAge, 0)
				require.Empty(t, cookie.Value)
			}
		})
	}
}

func findCookie(t *testing.T, cookies []*http.Cookie, name string) *http.Cookie {
	t.Helper()

	for _, cookie := range cookies {
		if cookie.Name == name {
			return cookie
		}
	}

	t.Fatalf("cookie %q not found", name)
	return nil
}
