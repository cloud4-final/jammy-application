package main

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"github.com/cloud4-final/jammy-5th/tools/payloadgen/gen"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

func testConfig() config {
	return config{
		duration: 10 * time.Second, grace: time.Second, maxInFlight: 8, ringBytes: 2 << 20,
		payload: gen.Options{Seed: 42, EventsPerBatch: 10, Assets: 16, Anchor: time.Now().UTC()},
	}
}

type failingTransport struct{ err error }

func (transport failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, transport.err
}

func TestPostRecordsTimeoutSeparatelyFromOtherTransportErrors(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(err.Error(), func(t *testing.T) {
			client := &http.Client{Transport: failingTransport{err}}
			record := post(t.Context(), client, "http://ingest", batch{BatchID: "fixture"}, time.Now())
			raw, marshalErr := json.Marshal(record)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			var receipt map[string]any
			if err := json.Unmarshal(raw, &receipt); err != nil {
				t.Fatal(err)
			}
			if record.Status != 0 || receipt["batch_id"] != "fixture" ||
				(receipt["timed_out"] == true) != (err == context.DeadlineExceeded) {
				t.Errorf("unexpected failure receipt: %s", raw)
			}
		})
	}
}

// Choose an integral number of profile cycles over the test window. At
// production batch sizes one synth-large request can account for more than
// 1% of a short measurement.
func testRate(t *testing.T, cfg config, cycles int) float64 {
	t.Helper()
	var size int
	for _, profile := range profiles {
		opts := cfg.payload
		opts.Profile = profile
		encoder, err := gen.NewBatchEncoder(opts, "000000000000000000000000-0")
		if err != nil {
			t.Fatal(err)
		}
		entry, _, err := encoder.Next(nil)
		defer func() { _ = encoder.Close() }()
		if err != nil {
			t.Fatal(err)
		}
		size += entry.WireBytes
	}
	return float64(cycles*size) / cfg.duration.Seconds() / 1e6
}

func TestRateScheduleAndUniqueBatchIDs(t *testing.T) {
	cfg := testConfig()
	const cycles = 40
	cfg.rate = testRate(t, cfg, cycles)
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	var ids sync.Map
	var bytes [2]atomic.Int64
	var count [2]atomic.Int64
	var first, last [2]atomic.Int64
	for i := range bytes {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			at := time.Now().UnixNano()
			first[i].CompareAndSwap(0, at)
			last[i].Store(at)
			if r.Method != http.MethodPost || r.URL.Path != "/v1/batches" ||
				r.Header.Get("Content-Type") != "application/x-protobuf" || r.Header.Get("Content-Encoding") != "zstd" {
				t.Error("unexpected ingest request")
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			raw, err := decoder.DecodeAll(body, nil)
			message := new(pb.EventBatch)
			if err != nil || proto.Unmarshal(raw, message) != nil || message.BatchId == "" {
				t.Error("request body was not a compressed EventBatch")
				return
			}
			if _, exists := ids.LoadOrStore(message.BatchId, true); exists {
				t.Errorf("reused batch_id %q", message.BatchId)
			}
			bytes[i].Add(int64(len(body)))
			count[i].Add(1)
			w.WriteHeader(http.StatusAccepted)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"batch_id": message.BatchId, "accepted": len(message.Events), "rejected": 0, "duplicate": false,
			}); err != nil {
				t.Error(err)
			}
		}))
		defer server.Close()
		cfg.targets = append(cfg.targets, server.URL)
	}
	summary, err := run(t.Context(), cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	for i, target := range cfg.targets {
		if n := count[i].Load(); n != cycles*int64(len(profiles)) {
			t.Errorf("%s: expected %d arrivals, got %d", target, cycles*len(profiles), n)
		}
		if elapsed := time.Duration(last[i].Load() - first[i].Load()); elapsed < 9800*time.Millisecond ||
			elapsed >= 10*time.Second {
			t.Errorf("%s: unexpected arrival span %s", target, elapsed)
		}
		measured := float64(bytes[i].Load()) / cfg.duration.Seconds() / 1e6
		if math.Abs(measured/cfg.rate-1) > 0.01 {
			t.Errorf("%s: stub received %.6f MB/s, want %.6f MB/s (±1%%)", target, measured, cfg.rate)
		}
		actual := summary.PerTarget[target]
		if actual.Achieved != measured || actual.Measure.AcceptedWireBytes != bytes[i].Load() ||
			actual.Statuses[202] != count[i].Load() || actual.P99 == nil {
			t.Errorf("%s: summary differs from stub, got %.6f MB/s and %v", target, actual.Achieved, actual.Statuses)
		}
	}
	if summary.NotSent != 0 || summary.GeneratorStarved != 0 {
		t.Errorf("not_sent=%d generator_starved=%d", summary.NotSent, summary.GeneratorStarved)
	}
}

func TestSlowTargetDropsScheduledArrivals(t *testing.T) {
	cfg := testConfig()
	cfg.duration = 600 * time.Millisecond
	cfg.maxInFlight = 1
	const cycles = 20
	cfg.rate = testRate(t, cfg, cycles)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
			return
		}
		time.Sleep(120 * time.Millisecond)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg.targets = []string{server.URL}
	summary, err := run(t.Context(), cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	sent := summary.Statuses[503]
	if sent < 3 || sent > 6 || summary.NotSent+sent != cycles*int64(len(profiles)) || summary.GeneratorStarved != 0 {
		t.Errorf("sent=%d not_sent=%d generator_starved=%d", sent, summary.NotSent, summary.GeneratorStarved)
	}
}

func TestEmptyRingFlagsStarvation(t *testing.T) {
	cfg := config{duration: 50 * time.Millisecond, maxInFlight: 1}
	target := &target{ring: newBatchRing(1024), interval: 10 * time.Millisecond}
	result := target.send(t.Context(), cfg, nil, time.Now().Add(10*time.Millisecond),
		5*time.Millisecond, func(string, ...any) {}, func() {})
	if result.NotSent != 5 || result.GeneratorStarved != 5 || result.Measure.GeneratorStarved != 5 {
		t.Errorf("not_sent=%d, generator_starved=%d, measure starved=%d",
			result.NotSent, result.GeneratorStarved, result.Measure.GeneratorStarved)
	}
}

func TestRingEnforcesByteBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ring := newBatchRing(100)
	onFull := make(chan struct{}, 1)
	produced := make(chan error, 1)
	wire := make([]byte, 60)
	wire[0] = 42
	if err := ring.push(ctx, gen.IndexEntry{BatchID: "first"}, wire, func() {}); err != nil {
		t.Fatal(err)
	}
	wire[0] = 43
	go func() {
		produced <- ring.push(ctx, gen.IndexEntry{BatchID: "second"}, wire, func() {
			select {
			case onFull <- struct{}{}:
			default:
			}
		})
	}()
	select {
	case <-onFull:
	case err := <-produced:
		t.Fatalf("second batch bypassed byte limit: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if first, ok := ring.pop(); !ok || first.BatchID != "first" || first.wire[0] != 42 {
		t.Fatal("ring changed bytes or reordered the first batch")
	}
	if err := <-produced; err != nil {
		t.Fatal(err)
	}
	if second, ok := ring.pop(); !ok || second.BatchID != "second" || second.wire[0] != 43 {
		t.Fatal("ring lost or reordered the second batch")
	}
	if ring.peak != 60 || ring.bytes.Load() != 0 {
		t.Errorf("peak=%d queued=%d", ring.peak, ring.bytes.Load())
	}
	if err := ring.push(ctx, gen.IndexEntry{}, make([]byte, 101), func() {}); err == nil {
		t.Fatal("accepted a batch larger than the entire ring")
	}
}
