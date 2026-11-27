package httpserver

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"mime"
	"net"
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
	csrfInvalidForm   = "invalid form body"
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

	// FormMemory is the maximum memory used to parse multipart forms. Non-positive
	// values default to defaultMaxBodyBytes.
	FormMemory int64
}

// CSRF issues and validates signed double-submit tokens.
type CSRF struct {
	secret     []byte
	cookieName string
	secure     bool
	appOrigin  string
	formMemory int64
}

// NewCSRF returns a CSRF token manager.
func NewCSRF(cfg CSRFConfig) (*CSRF, error) {
	if len(cfg.Secret) < 32 {
		return nil, ErrWeakCSRFSecret
	}
	if cfg.FormMemory <= 0 {
		cfg.FormMemory = defaultMaxBodyBytes
	}
	if cfg.BaseURL != "" {
		u, err := url.Parse(cfg.BaseURL)
		if err != nil || u.Host == "" || u.User != nil || !validHTTPScheme(u.Scheme) {
			return nil, fmt.Errorf("invalid csrf base URL")
		}
		cfg.Secure = cfg.Secure || strings.EqualFold(u.Scheme, "https")
		if cfg.AppOrigin == "" {
			cfg.AppOrigin = strings.ToLower(u.Scheme) + "://" + u.Host
		}
	}
	if cfg.AppOrigin == "" {
		return nil, fmt.Errorf("csrf app origin is required")
	}
	appOrigin, err := canonicalOrigin(cfg.AppOrigin)
	if err != nil {
		return nil, fmt.Errorf("invalid csrf app origin: %w", err)
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
		appOrigin:  appOrigin,
		formMemory: cfg.FormMemory,
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

		// Gin's PostForm parses on demand but discards parsing errors. Parse
		// explicitly so malformed/limited bodies return 400 instead of a
		// misleading CSRF 403.
		mediaType, _, _ := mime.ParseMediaType(ctx.GetHeader("Content-Type"))
		var formErr error
		if mediaType == "multipart/form-data" {
			formErr = ctx.Request.ParseMultipartForm(c.formMemory)
		} else {
			formErr = ctx.Request.ParseForm()
		}
		if formErr != nil {
			ctx.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": csrfInvalidForm})
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

func canonicalOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || !validHTTPScheme(u.Scheme) ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", fmt.Errorf("invalid origin")
	}

	hostname := strings.ToLower(u.Hostname())
	if hostname == "" || strings.HasSuffix(u.Host, ":") {
		return "", fmt.Errorf("invalid origin host")
	}
	port := u.Port()
	if port == "" {
		if strings.EqualFold(u.Scheme, "https") {
			port = "443"
		} else {
			port = "80"
		}
	}

	return strings.ToLower(u.Scheme) + "://" + net.JoinHostPort(hostname, port), nil
}

func validHTTPScheme(scheme string) bool {
	return strings.EqualFold(scheme, "http") || strings.EqualFold(scheme, "https")
}

func (c *CSRF) validRequestOrigin(r *http.Request) bool {
	if origin := r.Header.Get("Origin"); origin != "" {
		canonical, err := canonicalOrigin(origin)
		if err != nil || canonical != c.appOrigin {
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
