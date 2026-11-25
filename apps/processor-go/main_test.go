package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/cloud4-final/jammy-5th/libs/go/logx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestRejectInvalidParityEnv(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "direct-db")
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	for name, value := range map[string]string{
		"HTTP_PORT": "0", "ADMIN_PORT": "0", "MAX_ACK_PENDING": "0",
		"PROCESSOR_MAX_DELIVER": "0", "ANOMALY_DROP_RATIO": "0", "CONSUMER_NAME": "",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			if _, err := load(); err == nil {
				t.Fatalf("accepted %s=%q", name, value)
			}
		})
	}
}

func TestLoadNamesInvalidNATSStreams(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "jetstream")
	t.Setenv("NATS_STREAMS", "stream,")
	_, err := load()
	if err == nil || !strings.HasPrefix(err.Error(), "invalid NATS_STREAMS: ") {
		t.Fatalf("load error = %v, want invalid NATS_STREAMS", err)
	}
}

func TestRejectDuplicateNATSStreams(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "jetstream")
	t.Setenv("NATS_STREAMS", "raw-a1,raw-a1")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	_, err := load()
	if err == nil || !strings.HasPrefix(err.Error(), "invalid NATS_STREAMS: ") {
		t.Fatalf("load error = %v, want duplicate NATS_STREAMS error", err)
	}
}

func TestLoadValidatesDatabaseAndNATSURL(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "direct-db")
	t.Setenv("DATABASE_URL", "")
	if _, err := load(); err == nil || err.Error() != "DATABASE_URL is required" {
		t.Fatalf("missing DATABASE_URL: got %v", err)
	}
	t.Setenv("DATABASE_URL", "://bad")
	if _, err := load(); err == nil || err.Error() != "invalid DATABASE_URL" {
		t.Fatalf("invalid DATABASE_URL: got %v", err)
	}
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	if _, err := load(); err != nil {
		t.Fatalf("valid direct-db config failed: %v", err)
	}

	t.Setenv("PIPELINE_MODE", "jetstream")
	t.Setenv("NATS_STREAMS", "raw-a1,raw-a2")
	t.Setenv("NATS_URL", "")
	if _, err := load(); err == nil || err.Error() != "NATS_URL is required" {
		t.Fatalf("missing NATS_URL: got %v", err)
	}
	t.Setenv("NATS_URL", "://bad")
	if _, err := load(); err == nil || err.Error() != "invalid NATS_URL" {
		t.Fatalf("invalid NATS_URL: got %v", err)
	}
	t.Setenv("NATS_URL", "nats://localhost:4222")
	if _, err := load(); err != nil {
		t.Fatalf("valid jetstream config failed: %v", err)
	}
}

func TestProcessorScheduleAndFilter(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "direct-db")
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	loaded, err := load()
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	if loaded.maxDeliver != 10 {
		t.Fatalf("default maxDeliver = %d, want 10", loaded.maxDeliver)
	}
	cfg := settings{stateFlush: 19 * time.Second, consumer: "processor", maxPending: 17, maxDeliver: 10, ackWait: 5 * time.Second}
	c := cfg.consumerConfig("raw-test")
	if c.FilterSubject != "jammy.raw.raw-test" || c.MaxAckPending != 17 || c.MaxDeliver != 10 {
		t.Fatalf("consumer configuration: %+v", c)
	}
	if fetchWait != time.Second || idleDelay != 200*time.Millisecond || retryDelay != time.Second || assetRefreshInterval != time.Minute {
		t.Fatal("processor poll, retry or refresh timing differs from parity rules")
	}
	now := time.Now()
	if receivedLag(now, time.Time{}) != 0 || time.Duration(receivedLag(now, now.Add(-3*time.Second))) != 3*time.Second {
		t.Fatal("processor lag did not track the last processed row")
	}
}

type headerMsg struct {
	jetstream.Msg
	headers nats.Header
}

func (m headerMsg) Headers() nats.Header { return m.headers }

func TestStartBatchSpanExtractsTraceparentAndContextLogs(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })
	tracer := provider.Tracer("jammy.processor")

	const traceID = "00000000000000000000000000000042"
	const parentSpanID = "0000000000000023"
	for _, key := range []string{"traceparent", "Traceparent"} {
		msg := headerMsg{headers: nats.Header{
			key:           {"00-" + traceID + "-" + parentSpanID + "-01"},
			"Nats-Msg-Id": {"batch-1"},
		}}
		work, span := startBatchSpan(t.Context(), tracer, "raw-a1", []jetstream.Msg{msg})
		var buf bytes.Buffer
		logger, err := logx.New(&buf, "processor", config.Common{SiteID: "test", InstanceID: "i1", LogLevel: "info"})
		if err != nil {
			t.Fatal(err)
		}
		logger.WarnContext(work, "batch failure")
		span.End()

		var entry map[string]any
		if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
			t.Fatalf("log output %q: %v", buf.String(), err)
		}
		if entry["trace_id"] != traceID || entry["span_id"] == "" {
			t.Fatalf("expected trace context in log entry, got %v", entry)
		}
	}

	ended := recorder.Ended()
	if len(ended) != 2 {
		t.Fatalf("got %d spans, want 2", len(ended))
	}
	for _, s := range ended {
		if s.SpanKind() != trace.SpanKindConsumer || s.Parent().TraceID().String() != traceID || s.Parent().SpanID().String() != parentSpanID {
			t.Fatalf("unexpected span parent/kind: kind=%v parent=%v", s.SpanKind(), s.Parent())
		}
	}

	multi := []jetstream.Msg{
		headerMsg{headers: nats.Header{"traceparent": {"00-00000000000000000000000000000011-0000000000000012-01"}}},
		headerMsg{headers: nats.Header{"Traceparent": {"00-00000000000000000000000000000021-0000000000000022-01"}}},
	}
	_, batchSpan := startBatchSpan(t.Context(), tracer, "raw-a1", multi)
	batchSpan.End()
	ended = recorder.Ended()
	links := ended[len(ended)-1].Links()
	if len(links) != 2 || links[0].SpanContext.TraceID().String() != "00000000000000000000000000000011" || links[1].SpanContext.TraceID().String() != "00000000000000000000000000000021" {
		t.Fatalf("unexpected links: %+v", links)
	}
}

func TestObservationResetsLagWhenPendingZero(t *testing.T) {
	var src observation
	src.lag.Store(int64(5 * time.Second))
	updateSourceObservation(&src, 3)
	if src.pending.Load() != 3 || src.lag.Load() != int64(5*time.Second) {
		t.Fatalf("pending=%d lag=%d, want 3 and 5s", src.pending.Load(), src.lag.Load())
	}
	updateSourceObservation(&src, 0)
	if src.pending.Load() != 0 || src.lag.Load() != 0 {
		t.Fatalf("pending=%d lag=%d, want 0 and 0", src.pending.Load(), src.lag.Load())
	}
}

func TestCheckDependenciesLiveChecks(t *testing.T) {
	sources := make([]observation, 1)
	sources[0].ready.Store(true)
	okPing := func(context.Context) error { return nil }
	failPing := func(context.Context) error { return errors.New("pg down") }
	okStream := func(context.Context, string) error { return nil }
	failStream := func(context.Context, string) error { return errors.New("stream missing") }

	if !checkDependencies(t.Context(), true, true, sources, []string{"raw-a1"}, true, okStream, okPing) {
		t.Fatal("expected ready when all live checks pass")
	}
	if checkDependencies(t.Context(), true, true, sources, []string{"raw-a1"}, true, okStream, failPing) {
		t.Fatal("expected unready when live DB ping fails")
	}
	if checkDependencies(t.Context(), true, true, sources, []string{"raw-a1"}, true, failStream, okPing) {
		t.Fatal("expected unready when live stream check fails")
	}
	if checkDependencies(t.Context(), true, true, sources, []string{"raw-a1"}, false, okStream, okPing) {
		t.Fatal("expected unready when NATS is disconnected")
	}
}
