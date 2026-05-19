package observability

import (
	"fmt"
	"log"
	"time"

	appobs "feedsystem_video_go/internal/observability"

	"github.com/gin-gonic/gin"
)

func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		latency := time.Since(start)
		log.Printf(
			`event=http_access method=%s path=%s route=%s status=%d latency_ms=%d client_ip=%s size=%d`,
			c.Request.Method,
			c.Request.URL.Path,
			routePattern(c),
			c.Writer.Status(),
			latency.Milliseconds(),
			c.ClientIP(),
			c.Writer.Size(),
		)
	}
}

func Metrics(metrics *appobs.Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		if metrics == nil {
			c.Next()
			return
		}

		start := time.Now()
		metrics.HTTPInFlight.Inc()
		defer metrics.HTTPInFlight.Dec()

		c.Next()

		status := fmt.Sprintf("%d", c.Writer.Status())
		route := routePattern(c)
		metrics.HTTPRequestsTotal.WithLabelValues(c.Request.Method, route, status).Inc()
		metrics.HTTPRequestLatency.WithLabelValues(c.Request.Method, route, status).Observe(time.Since(start).Seconds())
	}
}

func routePattern(c *gin.Context) string {
	if c.FullPath() != "" {
		return c.FullPath()
	}
	return c.Request.URL.Path
}
