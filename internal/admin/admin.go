package admin

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Deps struct {
	Metrics prometheus.Gatherer
	// Ready must return nil when the service can serve traffic. A nil Ready
	// check keeps the endpoint unconditionally ready, which is useful for
	// services without external dependencies.
	Ready func(context.Context) error
}

func NewHandler(d Deps) http.Handler {
	g := gin.New()
	g.Use(gin.Recovery())

	g.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	g.GET("/ready", func(c *gin.Context) {
		if d.Ready != nil {
			if err := d.Ready(c.Request.Context()); err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{
					"status": "not ready",
					"error":  err.Error(),
				})
				return
			}
		}

		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	g.GET("/metrics", gin.WrapH(
		promhttp.HandlerFor(d.Metrics, promhttp.HandlerOpts{})),
	)

	return g
}
