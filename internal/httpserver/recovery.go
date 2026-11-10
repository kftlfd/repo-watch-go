package httpserver

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Recovery records panics in the request context so ObservabilityMiddleware can
// log them, then returns a generic 500 response.
func Recovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		RecordError(c, "panic", fmt.Errorf("%v", recovered))
		c.AbortWithStatus(http.StatusInternalServerError)
	})
}
