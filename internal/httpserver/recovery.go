package httpserver

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Recovery records panics in the request context so ObservabilityMiddleware can
// log them, then returns a generic 500 response. Gin's built-in recovery writer
// is discarded because its request dump can include cookies, query strings, and
// other sensitive request data in debug mode.
func Recovery() gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, recovered any) {
		RecordError(c, "panic", fmt.Errorf("%v", recovered))
		c.AbortWithStatus(http.StatusInternalServerError)
	})
}
