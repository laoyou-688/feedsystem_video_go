package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsHandler(t *testing.T) {
	metrics := NewMetrics()
	metrics.HTTPRequestsTotal.WithLabelValues("GET", "/metrics", "200").Inc()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	metrics.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "feedsystem_http_requests_total") {
		t.Fatalf("expected metrics output to include feedsystem_http_requests_total, got: %s", body)
	}
}
