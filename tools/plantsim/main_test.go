package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStaleEpochRejected409(t *testing.T) {
	sim := newSimulator(0, 0, "site-a", "plantsim-1")
	sendCommand(t, sim, "asset-1", 2, 200, "acked")
	sendCommand(t, sim, "asset-1", 1, 409, "stale_epoch")
}

func TestAckRecordsMaxEpoch(t *testing.T) {
	sim := newSimulator(0, 0, "site-a", "plantsim-1")
	sendCommand(t, sim, "asset-1", 0, 200, "acked")
	sendCommand(t, sim, "asset-1", 5, 200, "acked")
	sendCommand(t, sim, "asset-1", 5, 200, "acked")
	sendCommand(t, sim, "asset-1", 4, 409, "stale_epoch")
	sendCommand(t, sim, "asset-2", 0, 200, "acked")
}

func TestRepeatedCommandIDSameResult(t *testing.T) {
	sim := newSimulator(0, 0, "site-a", "plantsim-1")
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
	sim := newSimulator(0, 1, "site-a", "plantsim-1")
	sendCommand(t, sim, "asset-1", 5, 503, "error")
	sim.failRatio = 0
	sendCommand(t, sim, "asset-1", 2, 200, "acked")
}

func TestCanceledCommandDoesNotAdvanceEpoch(t *testing.T) {
	sim := newSimulator(time.Second, 0, "site-a", "plantsim-1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := httptest.NewRecorder()
	sim.command(res, commandRequest("asset-1", 5).WithContext(ctx))
	sim.delay = 0
	sendCommand(t, sim, "asset-1", 2, 200, "acked")
}

func TestMetricsLabels(t *testing.T) {
	sim := newSimulator(0, 0, "site-a", "plantsim-1")
	sendCommand(t, sim, "asset-1", 5, 200, "acked")
	sendCommand(t, sim, "asset-1", 2, 409, "stale_epoch")
	res := httptest.NewRecorder()
	sim.metrics.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := res.Body.String()
	for _, expected := range []string{
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
