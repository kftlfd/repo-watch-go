package httpserver

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	// CSRFFormField is the hidden form field used for CSRF tokens.
	CSRFFormField = "csrf_token"
	// CSRFHeader is the header alternative to the hidden form field.
	CSRFHeader = "X-CSRF-Token"

	defaultCSRFCookieName = "rw_csrf"

	csrfContextKey    = "httpserver.csrf_token"
	csrfTokenBytes    = 32
	csrfInternalError = "internal server error"
)

var (
	ErrWeakCSRFSecret = errors.New("csrf secret must be at least 32 bytes")
	errInvalidCSRF    = errors.New("invalid csrf token")
)

// CSRFConfig controls signed double-submit CSRF protection. BaseURL derives
// the Secure flag, cookie name, and app origin when they are not set.
type CSRFConfig struct {
	Secret     []byte
	CookieName string
	BaseURL    string
	Secure     bool
	AppOrigin  string
}

// CSRF issues and validates signed double-submit tokens.
type CSRF struct {
	secret     []byte
	cookieName string
	secure     bool
	appOrigin  string
}

// NewCSRF returns a CSRF token manager.
func NewCSRF(cfg CSRFConfig) (*CSRF, error) {
	if len(cfg.Secret) < 32 {
		return nil, ErrWeakCSRFSecret
	}
	if cfg.BaseURL != "" {
		u, err := url.Parse(cfg.BaseURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("invalid csrf base URL")
		}
		cfg.Secure = cfg.Secure || strings.EqualFold(u.Scheme, "https")
		if cfg.AppOrigin == "" {
			cfg.AppOrigin = strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host)
		}
	}
	if cfg.AppOrigin == "" {
		return nil, fmt.Errorf("csrf app origin is required")
	}
	if cfg.CookieName == "" {
		cfg.CookieName = defaultCSRFCookieName
		if cfg.Secure {
			cfg.CookieName = "__Host-" + defaultCSRFCookieName
		}
	}

	return &CSRF{
		secret:     slices.Clone(cfg.Secret),
		cookieName: cfg.CookieName,
		secure:     cfg.Secure,
		appOrigin:  strings.TrimRight(cfg.AppOrigin, "/"),
	}, nil
}

// Middleware validates CSRF tokens on unsafe requests. Safe requests receive a
// token cookie and context token for rendering forms.
func (c *CSRF) Middleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if isSafeMethod(ctx.Request.Method) {
			token, err := ctx.Cookie(c.cookieName)
			if err != nil || !c.Validate(token) {
				_, err = c.Rotate(ctx)
				if err != nil {
					RecordError(ctx, "generate csrf token", err)
					ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": csrfInternalError})
					return
				}
			} else {
				ctx.Set(csrfContextKey, token)
			}
			ctx.Next()
			return
		}

		if !c.validRequestOrigin(ctx.Request) {
			ctx.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": errInvalidCSRF.Error()})
			return
		}

		cookieToken, cookieErr := ctx.Cookie(c.cookieName)
		formToken := ctx.PostForm(CSRFFormField)
		if formToken == "" {
			formToken = ctx.GetHeader(CSRFHeader)
		}

		if cookieErr != nil ||
			!c.Validate(cookieToken) ||
			!c.Validate(formToken) ||
			subtle.ConstantTimeCompare([]byte(cookieToken), []byte(formToken)) != 1 {
			ctx.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": errInvalidCSRF.Error()})
			return
		}

		ctx.Set(csrfContextKey, cookieToken)
		ctx.Next()
	}
}

// GetCSRFToken returns the request's current CSRF token.
func GetCSRFToken(c *gin.Context) string {
	token, _ := c.Get(csrfContextKey)
	if value, ok := token.(string); ok {
		return value
	}
	return ""
}

// Rotate generates a new CSRF token, stores it in the cookie, and returns it.
func (c *CSRF) Rotate(ctx *gin.Context) (string, error) {
	token, err := c.NewToken()
	if err != nil {
		return "", err
	}

	http.SetCookie(ctx.Writer, &http.Cookie{
		Name:     c.cookieName,
		Value:    token,
		Path:     "/",
		Secure:   c.secure,
		HttpOnly: false,
		SameSite: http.SameSiteLaxMode,
	})
	ctx.Set(csrfContextKey, token)
	return token, nil
}

// NewToken returns a new signed token in nonce.signature form.
func (c *CSRF) NewToken() (string, error) {
	nonce := make([]byte, csrfTokenBytes)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("generate csrf nonce: %w", err)
	}

	encodedNonce := base64.RawURLEncoding.EncodeToString(nonce)
	signature := c.sign(encodedNonce)
	return encodedNonce + "." + signature, nil
}

// Validate reports whether token has a valid signature.
func (c *CSRF) Validate(token string) bool {
	nonce, signature, ok := strings.Cut(token, ".")
	if !ok || nonce == "" || signature == "" {
		return false
	}

	expected := c.sign(nonce)
	return hmac.Equal([]byte(signature), []byte(expected))
}

// CookieName returns the CSRF cookie name.
func (c *CSRF) CookieName() string {
	return c.cookieName
}

func (c *CSRF) sign(nonce string) string {
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write([]byte(nonce))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (c *CSRF) validRequestOrigin(r *http.Request) bool {
	if origin := r.Header.Get("Origin"); origin != "" {
		if !strings.EqualFold(strings.TrimRight(origin, "/"), c.appOrigin) {
			return false
		}
	}

	switch r.Header.Get("Sec-Fetch-Site") {
	case "", "same-origin", "none":
		return true
	default:
		return false
	}
}

func isSafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}
