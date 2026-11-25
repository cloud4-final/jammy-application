package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/ingest-go/internal/server"
	"github.com/cloud4-final/jammy-5th/apps/ingest-go/internal/sinkdb"
	"github.com/cloud4-final/jammy-5th/apps/ingest-go/internal/sinkjs"
	"github.com/cloud4-final/jammy-5th/libs/go/admin"
	"github.com/cloud4-final/jammy-5th/libs/go/codec"
	"github.com/cloud4-final/jammy-5th/libs/go/gate"
	"github.com/cloud4-final/jammy-5th/libs/go/leasefence"
	"github.com/cloud4-final/jammy-5th/libs/go/natsx"
	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	protobuf        = "application/x-protobuf"
	testRawMaxBytes = 4 << 20
)

func must[T any](t *testing.T, value T, err error) T {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func sample() *pb.EventBatch {
	return &pb.EventBatch{
		BatchId: "batch-1", ProducerId: "test",
		Events: []*pb.Event{{EventId: "event-1", RoutingKey: "asset-1", Profile: "synth-small", SchemaVersion: 1, EventTimeUnixMs: time.Now().UnixMilli()}},
	}
}

func marshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(msg)
	return must(t, b, err)
}

func compress(t *testing.T, body []byte) []byte {
	t.Helper()
	encoder, err := zstd.NewWriter(nil)
	enc := must(t, encoder, err)
	defer func() { _ = enc.Close() }()
	return enc.EncodeAll(body, nil)
}

func intake(t *testing.T, sink server.Sink, inflight int) *server.Intake {
	t.Helper()
	handler, err := server.New(server.Config{
		Instance: "ingress-1", MaxBody: 16 << 20, MaxDecoded: 64 << 20, Inflight: inflight,
		Gate: gate.Config{MaxPast: 24 * time.Hour, MaxFuture: 5 * time.Minute},
	}, sink, noop.NewMeterProvider().Meter("test"))
	return must(t, handler, err)
}

func post(h http.Handler, body []byte, media, encoding string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	req.Header.Set("Content-Type", media)
	req.Header.Set("Content-Encoding", encoding)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	return resp
}

func receipt(t *testing.T, result *httptest.ResponseRecorder) server.Receipt {
	t.Helper()
	if result.Code != 202 {
		t.Fatalf("got HTTP %d: %s", result.Code, result.Body)
	}
	var response server.Receipt
	mustOK(t, json.Unmarshal(result.Body.Bytes(), &response))
	return response
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("docker", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("docker %v: %v: %s%s", args, err, stderr.String(), stdout.String())
	}
	return strings.TrimSpace(stdout.String())
}

func container(t *testing.T, port string, args ...string) (string, string) {
	t.Helper()
	id := docker(t, append([]string{"run", "-d", "-p", "127.0.0.1::" + port}, args...)...)
	t.Cleanup(func() { docker(t, "rm", "-f", id) })
	return id, docker(t, "port", id, port+"/tcp")
}

func await(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("dependency did not become ready")
}

func stream(t *testing.T, max int64) (*server.Intake, jetstream.JetStream) {
	t.Helper()
	_, addr := container(t, "4222", "nats:2.15.3", "-js")
	t.Setenv("NATS_URL", "nats://"+addr)
	conn, err := natsx.Connect()
	nc := must(t, conn, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	mustOK(t, err)
	sink := sinkjs.New(js, nc.IsConnected, "raw-a1", 45*time.Minute, max, 168*time.Hour, 1<<30)
	await(t, func() bool { return nc.IsConnected() })
	mustOK(t, sink.Prepare(t.Context()))
	handler := intake(t, sink, 2)
	handler.SetReady(true)
	return handler, js
}

func stored(t *testing.T, js jetstream.JetStream, name string) *jetstream.RawStreamMsg {
	t.Helper()
	stream, err := js.Stream(t.Context(), name)
	mustOK(t, err)
	msg, err := stream.GetMsg(t.Context(), 1)
	return must(t, msg, err)
}

func TestValidBatch202AndInStream(t *testing.T) {
	handler, js := stream(t, testRawMaxBytes)
	batch := sample()
	before := time.Now().UnixMilli()
	r := receipt(t, post(handler, marshal(t, batch), protobuf, ""))
	msg := stored(t, js, "raw-a1")
	received, err := strconv.ParseInt(msg.Header.Get("Jammy-Received-At-Ms"), 10, 64)
	if r.Accepted != 1 || r.Duplicate || msg.Subject != "jammy.raw.raw-a1" ||
		msg.Header.Get("Nats-Msg-Id") != batch.BatchId || msg.Header.Get("Jammy-Ingest-Id") != "ingress-1" ||
		msg.Header.Get("Content-Type") != protobuf || msg.Header.Get("Content-Encoding") != "" || err != nil || received < before {
		t.Fatalf("unexpected receipt %+v or stream headers %v", r, msg.Header)
	}
}

func TestFullyValidBatchPassesBytesThrough(t *testing.T) {
	handler, js := stream(t, testRawMaxBytes)
	body := compress(t, marshal(t, sample()))
	receipt(t, post(handler, body, protobuf, "zstd"))
	msg := stored(t, js, "raw-a1")
	if !bytes.Equal(body, msg.Data) || msg.Header.Get("Content-Encoding") != "zstd" {
		t.Fatal("compressed protobuf changed in transit")
	}
}

func TestPartialBatchReencodesValidOnly(t *testing.T) {
	handler, js := stream(t, testRawMaxBytes)
	batch := sample()
	batch.Events = append(batch.Events, &pb.Event{EventId: "event-2"})
	input, err := protojson.Marshal(batch)
	r := receipt(t, post(handler, must(t, input, err), "application/json", ""))
	msg := stored(t, js, "raw-a1")
	encoded, _, err := codec.DecodeBatch(msg.Data, msg.Header.Get("Content-Type"), msg.Header.Get("Content-Encoding"), 1<<20, 1<<20)
	if err != nil || msg.Header.Get("Content-Encoding") != "zstd" || r.Accepted != 1 || r.Rejected != 1 ||
		!proto.Equal(encoded, &pb.EventBatch{BatchId: batch.BatchId, ProducerId: batch.ProducerId, Events: batch.Events[:1]}) {
		t.Fatalf("unexpected raw message: receipt %+v, decoded %v, error %v", r, encoded, err)
	}
}

func TestRejectedGoToQuarantineWithReason(t *testing.T) {
	handler, js := stream(t, testRawMaxBytes)
	batch := sample()
	bad := &pb.Event{EventId: "invalid id"}
	batch.Events = append(batch.Events, bad)
	r := receipt(t, post(handler, marshal(t, batch), protobuf, ""))
	msg := stored(t, js, "quarantine-raw-a1")
	raw := stored(t, js, "raw-a1")
	var event pb.Event
	mustOK(t, proto.Unmarshal(msg.Data, &event))
	if r.Reasons["bad_event_id"] != 1 || !proto.Equal(&event, bad) ||
		msg.Subject != "jammy.quarantine.raw-a1" || msg.Header.Get("Jammy-Reason") != "bad_event_id" ||
		msg.Header.Get("Nats-Msg-Id") != batch.BatchId+":q:1" || msg.Header.Get("Jammy-Batch-Id") != batch.BatchId ||
		msg.Header.Get("Jammy-Ingest-Id") != "ingress-1" || msg.Header.Get("Jammy-Received-At-Ms") != raw.Header.Get("Jammy-Received-At-Ms") ||
		msg.Time.After(raw.Time) {
		t.Fatalf("unexpected quarantine event %+v, headers %v, receipt %+v", event.String(), msg.Header, r)
	}
}

func TestDuplicateBatchIDSingleMessage(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(fmt.Sprint("all_rejected_", rejected), func(t *testing.T) {
			handler, js := stream(t, testRawMaxBytes)
			batch := sample()
			streamName := "raw-a1"
			if rejected {
				batch.Events[0].EventId = ""
				streamName = "quarantine-raw-a1"
			}
			body := marshal(t, batch)
			first := receipt(t, post(handler, body, protobuf, ""))
			second := receipt(t, post(handler, body, protobuf, ""))
			s, err := js.Stream(t.Context(), streamName)
			mustOK(t, err)
			info, err := s.Info(t.Context())
			mustOK(t, err)
			if first.Duplicate || !second.Duplicate || second.Accepted != 0 || second.Rejected != 0 || len(second.Reasons) != 0 || info.State.Msgs != 1 {
				t.Fatalf("unexpected replay %+v, first %+v, messages %d", second, first, info.State.Msgs)
			}
		})
	}
}

func TestStreamFullReturns503(t *testing.T) {
	handler, js := stream(t, 1<<20)
	for i := 0; i < 3; i++ {
		body := make([]byte, 400<<10)
		_, err := js.Publish(t.Context(), "jammy.raw.raw-a1", body)
		if i < 2 {
			mustOK(t, err)
		} else if err == nil {
			t.Fatal("stream not full")
		}
	}
	batch := sample()
	payload := marshal(t, &pb.SynthLarge{Blob: make([]byte, 65536)})
	for i := 0; i < 5; i++ {
		event := proto.CloneOf(batch.Events[0])
		event.EventId = fmt.Sprintf("event-%d", i+2)
		event.Profile = "synth-large"
		event.Payload = payload
		batch.Events = append(batch.Events, event)
	}
	r := post(handler, marshal(t, batch), protobuf, "")
	if r.Code != 503 || r.Header().Get("Retry-After") != "2" {
		t.Fatalf("got %d %v", r.Code, r.Header())
	}
}

type blockedSink struct{ entered, release chan struct{} }

func (s blockedSink) Store(ctx context.Context, _ server.Batch) (bool, error) {
	close(s.entered)
	select {
	case <-s.release:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func TestInflightLimit429(t *testing.T) {
	sink := blockedSink{make(chan struct{}), make(chan struct{})}
	h := intake(t, sink, 1)
	h.SetReady(true)
	body := marshal(t, sample())
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- post(h, body, protobuf, "") }()
	<-sink.entered
	second := post(h, body, protobuf, "")
	close(sink.release)
	receipt(t, <-done)
	if second.Code != 429 || second.Header().Get("Retry-After") != "1" {
		t.Fatalf("got %d %v", second.Code, second.Header())
	}
}

func TestDBModeCopiesRows(t *testing.T) {
	_, addr := container(t, "5432", "-e", "POSTGRES_PASSWORD=test", "--tmpfs", "/var/lib/postgresql/data", "postgres:17.12-trixie")
	p, err := pgxpool.New(t.Context(), "postgres://postgres:test@"+addr+"/postgres?sslmode=disable")
	mustOK(t, err)
	t.Cleanup(p.Close)
	await(t, func() bool {
		c, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		return p.Ping(c) == nil
	})
	schema, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "db", "migrations", "0001_init.up.sql"))
	mustOK(t, err)
	_, err = p.Exec(t.Context(), string(schema))
	mustOK(t, err)
	sink := sinkdb.New(p, 24*time.Hour, nil)
	mustOK(t, sink.Prepare(t.Context()))
	h := intake(t, sink, 1)
	h.SetReady(true)
	batch := sample()
	batch.Events = append(batch.Events, &pb.Event{})
	body := marshal(t, batch)
	first := receipt(t, post(h, body, protobuf, ""))
	second := receipt(t, post(h, body, protobuf, ""))
	var raw, quarantine, claims int
	mustOK(t, p.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM jammy.raw_events),
		(SELECT count(*) FROM jammy.quarantine), (SELECT count(*) FROM jammy.processed_batches WHERE consumer='ingest')`).Scan(&raw, &quarantine, &claims))
	if raw != 1 || quarantine != 1 || claims != 1 || first.Accepted != 1 || !second.Duplicate || second.Accepted != 0 || second.Rejected != 0 {
		t.Fatalf("unexpected raw=%d quarantine=%d claims=%d receipts=%+v/%+v", raw, quarantine, claims, first, second)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "db", "migrations", "0003_site_lease_state.up.sql"))
	mustOK(t, err)
	_, err = p.Exec(t.Context(), string(migration))
	mustOK(t, err)
	_, err = p.Exec(t.Context(), "INSERT INTO jammy.site_lease_state VALUES ('site-a', true, 1, true, now())")
	mustOK(t, err)
	guarded := intake(t, sinkdb.New(p, 24*time.Hour, leasefence.New("site-a", 10*time.Second)), 1)
	guarded.SetReady(true)
	if response := post(guarded, body, protobuf, ""); response.Code != 503 || response.Header().Get("Retry-After") != "2" {
		t.Fatalf("fenced ingest returned %d %v", response.Code, response.Header())
	}
}

func TestNATSDownReadyzThenRecovers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	mustOK(t, err)
	addr := ln.Addr().String()
	mustOK(t, ln.Close())
	t.Setenv("NATS_URL", "nats://"+addr)
	nc, err := natsx.Connect()
	mustOK(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	mustOK(t, err)
	sink := sinkjs.New(js, nc.IsConnected, "raw-a1", 45*time.Minute, testRawMaxBytes, 168*time.Hour, 1<<30)
	h := intake(t, sink, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go h.Maintain(ctx, sink)
	probe := admin.New(0, map[string]func(context.Context) bool{"store": h.Ready})
	status := func() int {
		response := httptest.NewRecorder()
		probe.ServeHTTP(response, httptest.NewRequest("GET", "/readyz", nil))
		return response.Code
	}
	if status() != 503 {
		t.Fatal("ready before NATS is available")
	}
	id := docker(t, "run", "-d", "-p", addr+":4222", "nats:2.15.3", "-js")
	t.Cleanup(func() { docker(t, "rm", "-f", id) })
	await(t, func() bool { return status() == 200 })
}

func TestSigtermDrainsInflight(t *testing.T) {
	_, js := stream(t, testRawMaxBytes)
	root := filepath.Join("..", "..", "..", "..")
	binary := filepath.Join(t.TempDir(), "ingest")
	build := exec.Command("go", "build", "-o", binary, "./apps/ingest-go")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build ingest: %v: %s", err, output)
	}
	port := func() int {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		mustOK(t, err)
		defer func() { _ = listener.Close() }()
		return listener.Addr().(*net.TCPAddr).Port
	}
	httpPort, adminPort := port(), port()
	if httpPort == adminPort {
		adminPort = port()
	}
	process := exec.CommandContext(t.Context(), binary)
	process.Env = append(os.Environ(),
		"SITE_ID=test", "INSTANCE_ID=ingress-1", "INGEST_STREAM=raw-a1", "PIPELINE_MODE=jetstream",
		fmt.Sprintf("HTTP_PORT=%d", httpPort), fmt.Sprintf("ADMIN_PORT=%d", adminPort),
		"OTEL_EXPORTER_OTLP_ENDPOINT=", "INGEST_MAX_INFLIGHT=1", "JS_MAX_BYTES=4MiB", "QUARANTINE_MAX_AGE=6h", "QUARANTINE_MAX_BYTES=2MiB",
	)
	var logs bytes.Buffer
	process.Stdout, process.Stderr = &logs, &logs
	mustOK(t, process.Start())
	finished := make(chan error, 1)
	go func() { finished <- process.Wait(); close(finished) }()
	t.Cleanup(func() {
		_ = process.Process.Kill()
		<-finished
		if t.Failed() {
			t.Log(logs.String())
		}
	})

	client := &http.Client{Timeout: 15 * time.Second}
	ready := fmt.Sprintf("http://127.0.0.1:%d/readyz", adminPort)
	checkReady := func(code int) bool {
		resp, err := client.Get(ready)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == code
	}
	await(t, func() bool { return checkReady(200) })
	quarantine, err := js.Stream(t.Context(), "quarantine-raw-a1")
	mustOK(t, err)
	info, err := quarantine.Info(t.Context())
	mustOK(t, err)
	if info.Config.MaxAge != 6*time.Hour || info.Config.MaxBytes != 2<<20 {
		t.Fatalf("unexpected quarantine limits: age=%s bytes=%d", info.Config.MaxAge, info.Config.MaxBytes)
	}

	body := marshal(t, sample())
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	uri := fmt.Sprintf("http://127.0.0.1:%d/v1/batches", httpPort)
	type result struct {
		status int
		err    error
	}
	outcome := make(chan result, 1)
	go func() {
		resp, err := client.Post(uri, protobuf, reader)
		if err != nil {
			outcome <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, err = io.Copy(io.Discard, resp.Body)
		outcome <- result{status: resp.StatusCode, err: err}
	}()
	// The handler reads the request body before trying to store the batch.
	_, err = writer.Write(body[:1])
	mustOK(t, err)
	await(t, func() bool {
		resp, err := client.Post(uri, protobuf, nil)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusTooManyRequests
	})
	mustOK(t, process.Process.Signal(syscall.SIGTERM))
	await(t, func() bool { return checkReady(503) })
	_, err = writer.Write(body[1:])
	mustOK(t, err)
	mustOK(t, writer.Close())
	got := <-outcome
	mustOK(t, got.err)
	if got.status != 202 {
		t.Fatalf("got HTTP %d", got.status)
	}
	mustOK(t, <-finished)
	if msg := stored(t, js, "raw-a1"); !bytes.Equal(msg.Data, body) {
		t.Fatal("accepted batch was not persisted")
	}
}

type unreadableBody struct{ t *testing.T }

func (b unreadableBody) Read([]byte) (int, error) {
	b.t.Fatal("unready intake read the request body")
	return 0, io.EOF
}

func TestUnreadyRejectsBeforeReadingBody(t *testing.T) {
	h := intake(t, nil, 1)
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", unreadableBody{t})
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("got HTTP %d, want 503", resp.Code)
	}
}

func TestZstdBombRejected413(t *testing.T) {
	h := intake(t, nil, 1)
	h.SetReady(true)
	bomb := compress(t, make([]byte, (64<<20)+1))
	if r := post(h, bomb, protobuf, "zstd"); r.Code != 413 {
		t.Fatalf("got HTTP %d", r.Code)
	}
}

func TestInflightGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	sink := blockedSink{make(chan struct{}), make(chan struct{})}
	h, err := server.New(server.Config{
		Instance: "ingress-1", MaxBody: 16 << 20, MaxDecoded: 64 << 20, Inflight: 3,
		Gate: gate.Config{MaxPast: 24 * time.Hour, MaxFuture: 5 * time.Minute},
	}, sink, provider.Meter("ingest"))
	mustOK(t, err)
	h.SetReady(true)

	readGauge := func(name string) int64 {
		t.Helper()
		var rm metricdata.ResourceMetrics
		mustOK(t, reader.Collect(t.Context(), &rm))
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == name {
					gauge := m.Data.(metricdata.Gauge[int64])
					return gauge.DataPoints[0].Value
				}
			}
		}
		t.Fatalf("metric %s not found", name)
		return 0
	}

	if got := readGauge("jammy_ingest_inflight_requests"); got != 0 {
		t.Fatalf("initial inflight = %d, want 0", got)
	}
	if got := readGauge("jammy_ingest_inflight_limit"); got != 3 {
		t.Fatalf("inflight limit = %d, want 3", got)
	}

	done := make(chan *httptest.ResponseRecorder, 1)
	body := marshal(t, sample())
	go func() { done <- post(h, body, protobuf, "") }()
	<-sink.entered
	if got := readGauge("jammy_ingest_inflight_requests"); got != 1 {
		close(sink.release)
		t.Fatalf("active inflight = %d, want 1", got)
	}
	close(sink.release)
	receipt(t, <-done)
	if got := readGauge("jammy_ingest_inflight_requests"); got != 0 {
		t.Fatalf("drained inflight = %d, want 0", got)
	}
}

func TestRequestOpensServerSpanAndPropagatesContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	h := intake(t, nil, 1)
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", nil)
	req.Header.Set("traceparent", "00-00000000000000000000000000000042-0000000000000023-01")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("got HTTP %d, want 503", resp.Code)
	}

	h.SetReady(true)
	req400 := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader([]byte("bad")))
	req400.Header.Set("Content-Type", protobuf)
	resp400 := httptest.NewRecorder()
	h.ServeHTTP(resp400, req400)
	if resp400.Code != http.StatusBadRequest {
		t.Fatalf("got HTTP %d, want 400", resp400.Code)
	}

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	s := spans[0]
	if s.Name() != "POST /v1/batches" || s.SpanKind() != trace.SpanKindServer {
		t.Fatalf("unexpected span name=%q kind=%v", s.Name(), s.SpanKind())
	}
	if s.Parent().SpanID().String() != "0000000000000023" {
		t.Fatalf("parent span id = %s, want 0000000000000023", s.Parent().SpanID())
	}
	if s.SpanContext().TraceID().String() != "00000000000000000000000000000042" {
		t.Fatalf("trace id = %s, want ...0042", s.SpanContext().TraceID())
	}
	if s.Status().Code != codes.Error {
		t.Fatalf("503 span status = %v, want Error", s.Status().Code)
	}
	attrs := make(map[string]any, len(s.Attributes()))
	for _, kv := range s.Attributes() {
		attrs[string(kv.Key)] = kv.Value.AsInterface()
	}
	if attrs["http.request.method"] != "POST" || attrs["http.route"] != "/v1/batches" || attrs["http.response.status_code"] != int64(503) {
		t.Fatalf("unexpected 503 span attributes: %v", attrs)
	}
	if spans[1].Status().Code == codes.Error {
		t.Fatalf("400 span unexpectedly marked Error: %v", spans[1].Status())
	}
}
