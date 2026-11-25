package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/cloud4-final/jammy-5th/libs/go/obs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestLoadRejectsEmptyInstanceID(t *testing.T) {
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("INSTANCE_ID", "")
	if _, err := load(); err == nil || !strings.Contains(err.Error(), "INSTANCE_ID") {
		t.Fatalf("expected empty instance ID error, got %v", err)
	}
}

func TestLoadUsesFileSettings(t *testing.T) {
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("INSTANCE_ID", "plantsim-1")
	t.Setenv("PLANTSIM_DELAY_MS", "invalid")
	file := filepath.Join(t.TempDir(), "delay")
	if err := os.WriteFile(file, []byte("120\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLANTSIM_DELAY_MS_FILE", file)
	cfg, err := load()
	if err != nil || cfg.delay != 120*time.Millisecond {
		t.Fatalf("delay = %s, error = %v", cfg.delay, err)
	}
}

func TestStaleEpochRejected409(t *testing.T) {
	sim := testSimulator(t, 0, 0)
	sendCommand(t, sim, "asset-1", 2, 200, "acked")
	sendCommand(t, sim, "asset-1", 1, 409, "stale_epoch")
}

func TestAckRecordsMaxEpoch(t *testing.T) {
	sim := testSimulator(t, 0, 0)
	sendCommand(t, sim, "asset-1", 0, 200, "acked")
	sendCommand(t, sim, "asset-1", 5, 200, "acked")
	sendCommand(t, sim, "asset-1", 5, 200, "acked")
	sendCommand(t, sim, "asset-1", 4, 409, "stale_epoch")
	sendCommand(t, sim, "asset-2", 0, 200, "acked")
}

func TestRepeatedCommandIDSameResult(t *testing.T) {
	sim := testSimulator(t, 0, 0)
	sendCommand(t, sim, "asset-1", 1, 200, "acked")
	sendCommand(t, sim, "asset-1", 3, 200, "acked")
	sendCommand(t, sim, "asset-1", 1, 200, "acked")
	sendCommand(t, sim, "asset-1", 2, 409, "stale_epoch")

	sim.failRatio = 1
	sendCommand(t, sim, "asset-2", 5, 503, "error")
	sim.failRatio = 0
	sendCommand(t, sim, "asset-2", 5, 503, "error")
	sendCommand(t, sim, "asset-2", 1, 200, "acked")
}

func TestFailedCommandDoesNotAdvanceEpoch(t *testing.T) {
	sim := testSimulator(t, 0, 1)
	sendCommand(t, sim, "asset-1", 5, 503, "error")
	sim.failRatio = 0
	sendCommand(t, sim, "asset-1", 2, 200, "acked")
}

func TestCanceledCommandDoesNotAdvanceEpoch(t *testing.T) {
	sim := testSimulator(t, time.Second, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := httptest.NewRecorder()
	sim.command(res, commandRequest("asset-1", 5).WithContext(ctx))
	sim.delay = 0
	sendCommand(t, sim, "asset-1", 2, 200, "acked")
}

func TestMetricsLabels(t *testing.T) {
	sim := testSimulator(t, 0, 0)
	sendCommand(t, sim, "asset-1", 5, 200, "acked")
	sendCommand(t, sim, "asset-1", 2, 409, "stale_epoch")
	res := httptest.NewRecorder()
	obs.MetricsHandler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := res.Body.String()
	for _, expected := range []string{
		`jammy_plantsim_commands_total`, `service="plantsim"`, `site="site-a"`, `instance="plantsim-1"`,
		`result="acked"`, `result="stale_epoch"`, `result="error"`,
		`http_request_method="POST"`, `http_route="/v1/commands"`,
		`http_response_status_code="200"`, `http_response_status_code="409"`,
		`http_server_request_duration_seconds_bucket`, `le="0.001"`, `le="10"`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("/metrics missing %s", expected)
		}
	}
	if strings.Contains(body, "asset-1") {
		t.Fatal("/metrics exposed an asset ID")
	}
}

func TestCommandContinuesDispatcherTrace(t *testing.T) {
	sim := testSimulator(t, 0, 1)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})

	req := commandRequest("asset-1", 5)
	req.Header.Set("traceparent", "00-00000000000000000000000000000042-0000000000000023-01")
	resp := httptest.NewRecorder()
	sim.command(resp, req)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP status = %d, want 503", resp.Code)
	}
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	span := spans[0]
	if span.Name() != "POST /v1/commands" || span.SpanKind() != trace.SpanKindServer {
		t.Fatalf("unexpected name %q or kind %v", span.Name(), span.SpanKind())
	}
	if span.Parent().SpanID().String() != "0000000000000023" ||
		span.SpanContext().TraceID().String() != "00000000000000000000000000000042" {
		t.Fatalf("unexpected parent %v or span context %v", span.Parent(), span.SpanContext())
	}
	attrs := make(map[string]any)
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsInterface()
	}
	if attrs["http.request.method"] != "POST" || attrs["http.route"] != "/v1/commands" ||
		attrs["http.response.status_code"] != int64(503) || span.Status().Code != codes.Error {
		t.Fatalf("unexpected attributes %v or status %v", attrs, span.Status())
	}
}

func testSimulator(t *testing.T, delay time.Duration, failRatio float64) *simulator {
	t.Helper()
	meter, shutdown, err := obs.Setup(service, config.Common{SiteID: "site-a", InstanceID: "plantsim-1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	sim, err := newSimulator(delay, failRatio, meter)
	if err != nil {
		t.Fatal(err)
	}
	return sim
}

func commandRequest(asset string, epoch int64) *http.Request {
	body := fmt.Sprintf(`{"command_id":%q,"instruction_id":"i1","asset_id":%q,
		"reduction_kw":"12.500","generation":1,"epoch":%d,"valid_until":"2026-11-23T03:00:00Z"}`, fmt.Sprintf("%s-%d", asset, epoch), asset, epoch)
	return httptest.NewRequest(http.MethodPost, "/v1/commands", strings.NewReader(body))
}

func sendCommand(t *testing.T, sim *simulator, asset string, epoch int64, wantCode int, wantStatus string) {
	t.Helper()
	res := httptest.NewRecorder()
	sim.command(res, commandRequest(asset, epoch))
	var body struct{ Status string }
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if res.Code != wantCode || body.Status != wantStatus {
		t.Fatalf("command %s at epoch %d returned %d %q, want %d %q", asset, epoch, res.Code, body.Status, wantCode, wantStatus)
	}
}
