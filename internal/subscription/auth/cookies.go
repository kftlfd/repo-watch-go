package auth

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	defaultAccessCookieName  = "rw_access"
	defaultRefreshCookieName = "rw_refresh"
)

// CookieConfig controls the names and transport flags of web session cookies.
// BaseURL enables secure cookies and __Host- names when it is HTTPS.
type CookieConfig struct {
	AccessName  string
	RefreshName string
	BaseURL     string
	Secure      bool
}

// CookieManager reads and writes the web access and refresh cookies.
type CookieManager struct {
	accessName  string
	refreshName string
	secure      bool
}

// NewCookieManager returns a cookie manager. Missing names default to
// rw_access and rw_refresh. Secure managers prefix final names with __Host-.
func NewCookieManager(cfg CookieConfig) *CookieManager {
	if cfg.AccessName == "" {
		cfg.AccessName = defaultAccessCookieName
	}
	if cfg.RefreshName == "" {
		cfg.RefreshName = defaultRefreshCookieName
	}
	if secureCookies(cfg.BaseURL) {
		cfg.Secure = true
	}
	if cfg.Secure {
		cfg.AccessName = secureCookieName(cfg.AccessName)
		cfg.RefreshName = secureCookieName(cfg.RefreshName)
	}

	return &CookieManager{
		accessName:  cfg.AccessName,
		refreshName: cfg.RefreshName,
		secure:      cfg.Secure,
	}
}

// AccessName returns the final access cookie name.
func (m *CookieManager) AccessName() string {
	return m.accessName
}

// RefreshName returns the final refresh cookie name.
func (m *CookieManager) RefreshName() string {
	return m.refreshName
}

// SetSession writes the access and refresh cookies for pair.
func (m *CookieManager) SetSession(c *gin.Context, pair TokenPair) {
	setCookie(c, m.accessName, pair.AccessToken, maxAgeSeconds(pair.AccessExpiresIn), m.secure, true)
	setCookie(c, m.refreshName, pair.RefreshToken, maxAgeSeconds(pair.RefreshExpiresIn), m.secure, true)
}

// ClearSession expires the access and refresh cookies.
func (m *CookieManager) ClearSession(c *gin.Context) {
	clearCookie(c, m.accessName, m.secure, true)
	clearCookie(c, m.refreshName, m.secure, true)
}

// AccessToken returns the access cookie value.
func (m *CookieManager) AccessToken(c *gin.Context) (string, bool) {
	token, err := c.Cookie(m.accessName)
	return token, err == nil
}

// RefreshToken returns the refresh cookie value.
func (m *CookieManager) RefreshToken(c *gin.Context) (string, bool) {
	token, err := c.Cookie(m.refreshName)
	return token, err == nil
}

// secureCookies reports whether cookies should use the Secure flag and
// __Host- names for baseURL.
func secureCookies(baseURL string) bool {
	u, err := url.Parse(baseURL)
	return err == nil && strings.EqualFold(u.Scheme, "https")
}

func secureCookieName(name string) string {
	if strings.HasPrefix(name, "__Host-") {
		return name
	}
	return "__Host-" + name
}

func setCookie(c *gin.Context, name, value string, maxAge int, secure, httpOnly bool) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		Secure:   secure,
		HttpOnly: httpOnly,
		SameSite: http.SameSiteLaxMode,
	})
}

func clearCookie(c *gin.Context, name string, secure, httpOnly bool) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Secure:   secure,
		HttpOnly: httpOnly,
		SameSite: http.SameSiteLaxMode,
	})
}

func maxAgeSeconds(d time.Duration) int {
	seconds := int(d.Seconds())
	if seconds < 0 {
		return 0
	}
	return seconds
}
