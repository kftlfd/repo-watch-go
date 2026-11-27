package httpserver

import "github.com/gin-gonic/gin"

const (
	defaultContentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'"
	defaultPermissionsPolicy     = "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()"
)

// SecurityHeaders sets baseline security headers for every response. Handlers
// that need a different policy, such as the API reference page, may override
// Content-Security-Policy before writing the response.
func SecurityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.Writer.Header()
		header.Set("Content-Security-Policy", defaultContentSecurityPolicy)
		header.Set("Cross-Origin-Opener-Policy", "same-origin")
		header.Set("Cross-Origin-Resource-Policy", "same-origin")
		header.Set("Permissions-Policy", defaultPermissionsPolicy)
		header.Set("Referrer-Policy", "no-referrer")
		header.Set("X-Content-Type-Options", "nosniff")
		header.Set("X-Frame-Options", "DENY")
		c.Next()
	}
}
