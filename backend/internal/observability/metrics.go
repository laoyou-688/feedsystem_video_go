package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry          *prometheus.Registry
	HTTPRequestsTotal *prometheus.CounterVec
	HTTPRequestLatency *prometheus.HistogramVec
	HTTPInFlight      prometheus.Gauge
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	factory := promauto.With(registry)
	return &Metrics{
		registry: registry,
		HTTPRequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "feedsystem_http_requests_total",
				Help: "Total number of HTTP requests handled by the API.",
			},
			[]string{"method", "route", "status"},
		),
		HTTPRequestLatency: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "feedsystem_http_request_duration_seconds",
				Help:    "Latency distribution of HTTP requests handled by the API.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "route", "status"},
		),
		HTTPInFlight: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "feedsystem_http_in_flight_requests",
				Help: "Current number of in-flight HTTP requests.",
			},
		),
	}
}

func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return promhttp.Handler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
