package admin

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewHandler(metricsRegistry prometheus.Gatherer) http.Handler {
	g := gin.New()
	g.Use(gin.Recovery())

	g.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	g.GET("/ready", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	g.GET("/metrics", gin.WrapH(
		promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{})),
	)

	return g
}
