package obs_test

import (
	"context"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/cloud4-final/jammy-5th/libs/go/admin"
	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/cloud4-final/jammy-5th/libs/go/obs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func TestMetricsDurationBuckets(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	meter, shutdown, err := obs.Setup("ingest", config.Common{SiteID: "site-a", InstanceID: "ingest-a1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	histogram, err := meter.Float64Histogram("http.server.request.duration", metric.WithUnit("s"))
	if err != nil {
		t.Fatal(err)
	}
	sampled := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	}))
	histogram.Record(sampled, 0.004, metric.WithAttributes(
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/v1/instructions/{id}"),
		attribute.Int("http.response.status_code", 202),
		attribute.String("batch_id", "forbidden"),
	))
	duration, err := meter.Float64Histogram("jammy_x_duration", metric.WithUnit("s"))
	if err != nil {
		t.Fatal(err)
	}
	duration.Record(sampled, 0.004, metric.WithAttributes(
		attribute.String("result", "stored"),
		attribute.String("http.route", "/must-not-appear"),
	))
	counter, err := meter.Int64Counter("jammy_ingest_events_total")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(sampled, 1, metric.WithAttributes(
		attribute.String("result", "accepted"),
		attribute.String("http.route", "/must-not-appear"),
	))
	response := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	request.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	admin.New(9090, nil).ServeHTTP(response, request)
	if strings.Contains(response.Body.String(), "batch_id") || strings.Contains(response.Body.String(), "/must-not-appear") {
		t.Fatalf("high-cardinality label exported: %s", response.Body.String())
	}
	for _, fragment := range []string{
		`service="ingest"`, `instance="ingest-a1"`, `site="site-a"`, `http_request_method="POST"`, `http_route="/v1/instructions/{id}"`,
		`http_response_status_code="202"`, `result="accepted"`, `result="stored"`,
	} {
		if response.Code != 200 || !strings.Contains(response.Body.String(), fragment) {
			t.Fatalf("missing %s from status %d output: %s", fragment, response.Code, response.Body.String())
		}
	}
	plain := httptest.NewRecorder()
	admin.New(9090, nil).ServeHTTP(plain, httptest.NewRequest("GET", "/metrics", nil))
	for _, scrape := range []struct {
		format   string
		response *httptest.ResponseRecorder
		bounds   string
	}{
		{"OpenMetrics", response, "0.001 0.0025 0.005 0.01 0.025 0.05 0.1 0.25 0.5 1.0 2.5 5.0 10.0 +Inf"},
		{"Prometheus text", plain, "0.001 0.0025 0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10 +Inf"},
	} {
		if scrape.response.Code != 200 {
			t.Fatalf("%s scrape returned %d: %s", scrape.format, scrape.response.Code, scrape.response.Body.String())
		}
		want := strings.Fields(scrape.bounds)
		for _, name := range []string{"http_server_request_duration_seconds", "jammy_x_duration_seconds"} {
			pattern := regexp.MustCompile(`(?m)^` + name + `_bucket\{[^\n]*\ble="([^"]+)"`)
			var buckets []string
			for _, match := range pattern.FindAllStringSubmatch(scrape.response.Body.String(), -1) {
				buckets = append(buckets, match[1])
			}
			if !slices.Equal(buckets, want) {
				t.Errorf("%s %s buckets = %v, want %v", scrape.format, name, buckets, want)
			}
		}
	}
}
