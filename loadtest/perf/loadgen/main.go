// Command loadgen sends a fixed-rate stream of JAMMY EventBatch requests.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloud4-final/jammy-5th/tools/payloadgen/gen"
)

type config struct {
	targets                 []string
	rate                    float64
	duration, warmup, grace time.Duration
	linger                  time.Duration
	maxInFlight             int
	ringBytes               int64
	payload                 gen.Options
}

type requestRecord struct {
	Started        float64 `json:"started"`
	Target         string  `json:"target"`
	Status         int     `json:"status"`
	DurationMS     float64 `json:"duration_ms"`
	WireBytes      int     `json:"wire_bytes"`
	BatchID        string  `json:"batch_id"`
	AcceptedEvents *int    `json:"accepted_events"`
	NewBatch       bool    `json:"new_batch"`
	TimedOut       bool    `json:"timed_out,omitempty"`
}

type totals struct {
	WireBytes         int64         `json:"wire_bytes"`
	AcceptedWireBytes int64         `json:"accepted_wire_bytes"`
	RawBytes          int64         `json:"raw_bytes"`
	Statuses          map[int]int64 `json:"requests_by_status"`
	LatencyMS         [61_001]int64 `json:"-"` // 1 ms buckets; per-request timeout is 60 s.
	P50               *int          `json:"p50_ms"`
	P95               *int          `json:"p95_ms"`
	P99               *int          `json:"p99_ms"`
	NotSent           int64         `json:"not_sent"`
	GeneratorStarved  int64         `json:"generator_starved"`
	Achieved          float64       `json:"achieved_MB_s"`
}

func (t *totals) record(item batch, response requestRecord) {
	t.WireBytes += int64(item.WireBytes)
	t.RawBytes += int64(item.RawBytes)
	if response.Status == http.StatusAccepted {
		t.AcceptedWireBytes += int64(item.WireBytes)
	}
	if t.Statuses == nil {
		t.Statuses = make(map[int]int64)
	}
	t.Statuses[response.Status]++
	t.LatencyMS[min(int(math.Ceil(response.DurationMS)), len(t.LatencyMS)-1)]++
}

func (t *totals) finish(duration time.Duration) {
	t.Achieved = float64(t.WireBytes) / duration.Seconds() / 1e6
	var count int64
	for _, value := range t.Statuses {
		count += value
	}
	if count == 0 {
		return
	}
	percentiles := []**int{&t.P50, &t.P95, &t.P99}
	for i, fraction := range []float64{0.5, 0.95, 0.99} {
		rank := int64(math.Ceil(float64(count) * fraction))
		for bucket, frequency := range t.LatencyMS {
			rank -= frequency
			if rank <= 0 {
				*percentiles[i] = &bucket
				break
			}
		}
	}
}

type targetReport struct {
	totals
	Measure       totals `json:"measure"`
	RingPeakBytes int64  `json:"ring_peak_bytes"`
}

type report struct {
	totals
	Measure   totals                   `json:"measure"`
	PerTarget map[string]*targetReport `json:"per_target"`
	RingBytes int64                    `json:"ring_bytes_limit"`
}

func (t *totals) add(other *totals) {
	t.WireBytes += other.WireBytes
	t.AcceptedWireBytes += other.AcceptedWireBytes
	t.RawBytes += other.RawBytes
	t.NotSent += other.NotSent
	t.GeneratorStarved += other.GeneratorStarved
	if t.Statuses == nil {
		t.Statuses = make(map[int]int64)
	}
	for code, count := range other.Statuses {
		t.Statuses[code] += count
	}
	for bucket, count := range other.LatencyMS {
		t.LatencyMS[bucket] += count
	}
}

func post(ctx context.Context, client *http.Client, target string, item batch, started time.Time) requestRecord {
	record := requestRecord{Started: float64(started.UnixMicro()) / 1000, Target: target,
		WireBytes: item.WireBytes, BatchID: item.BatchID}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/v1/batches", bytes.NewReader(item.wire))
	if err != nil {
		return record
	}
	// Disable body replay. Each scheduled POST is attempted at most once.
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Content-Encoding", "zstd")
	if response, err := client.Do(request); err == nil {
		defer func() { _ = response.Body.Close() }()
		record.Status = response.StatusCode
		if record.Status == http.StatusAccepted {
			var receipt struct {
				BatchID   string `json:"batch_id"`
				Accepted  *int   `json:"accepted"`
				Rejected  *int   `json:"rejected"`
				Duplicate *bool  `json:"duplicate"`
			}
			err = json.NewDecoder(response.Body).Decode(&receipt)
			if err == nil && receipt.BatchID == item.BatchID && receipt.Accepted != nil &&
				*receipt.Accepted >= 0 && *receipt.Accepted <= item.Events {
				record.AcceptedEvents = receipt.Accepted
				record.NewBatch = *receipt.Accepted == item.Events && receipt.Rejected != nil &&
					*receipt.Rejected == 0 && receipt.Duplicate != nil && !*receipt.Duplicate
			}
		}
		_, _ = io.Copy(io.Discard, response.Body)
	} else {
		record.TimedOut = os.IsTimeout(err)
	}
	record.DurationMS = float64(time.Since(started)) / float64(time.Millisecond)
	return record
}

func waitUntil(ctx context.Context, timer *time.Timer, at time.Time) bool {
	timer.Reset(time.Until(at))
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (t *target) send(ctx context.Context, cfg config, client *http.Client, start time.Time,
	offset time.Duration, logf func(string, ...any), stopProducing context.CancelFunc) *targetReport {
	result := new(targetReport)
	var lock sync.Mutex
	var pending sync.WaitGroup
	slots := make(chan struct{}, cfg.maxInFlight)
	measure := start.Add(cfg.warmup)
	end := measure.Add(cfg.duration)
	requests, cancel := context.WithDeadline(ctx, end.Add(cfg.grace))
	defer cancel()
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()

	for at := start.Add(offset); at.Before(end); at = at.Add(t.interval) {
		if !waitUntil(ctx, timer, at) {
			break
		}
		started := time.Now()
		outOfTime := !started.Before(end) || !started.Before(at.Add(t.interval)) ||
			(at.Before(measure) && !started.Before(measure))
		item, ready := t.ring.pop()
		// Consume the scheduled batch even on a drop, preserving sequence
		// across targets with different response times.
		if !ready {
			result.GeneratorStarved++
			if !at.Before(measure) {
				result.Measure.GeneratorStarved++
			}
		}
		if ready && !outOfTime {
			select {
			case slots <- struct{}{}:
				pending.Go(func() {
					defer func() { <-slots }()
					record := post(requests, client, t.url, item, started)
					lock.Lock()
					result.record(item, record)
					if !started.Before(measure) {
						result.Measure.record(item, record)
					}
					lock.Unlock()
					data, _ := json.Marshal(record)
					logf("PERF_REQUEST %s\n", data)
				})
				continue
			default:
			}
		}
		result.NotSent++
		if !at.Before(measure) {
			result.Measure.NotSent++
		}
	}
	stopProducing()
	waitUntil(ctx, timer, end)
	pending.Wait()
	result.finish(cfg.warmup + cfg.duration)
	result.Measure.finish(cfg.duration)
	return result
}

func run(ctx context.Context, cfg config, logs io.Writer) (*report, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	nonce := make([]byte, 12)
	rand.Read(nonce)
	namespace := hex.EncodeToString(nonce)
	if cfg.payload.Anchor.IsZero() {
		cfg.payload.Anchor = time.Now().UTC()
	}
	var producers sync.WaitGroup
	defer producers.Wait()
	var targets []*target
	stops := make([]context.CancelFunc, 0, len(cfg.targets))
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	for i, address := range cfg.targets {
		t := &target{url: address, ring: newBatchRing(cfg.ringBytes / int64(len(cfg.targets))), ready: make(chan struct{})}
		targets = append(targets, t)
		producerCtx, stopProducing := context.WithCancel(ctx)
		stops = append(stops, stopProducing)
		producers.Go(func() {
			err := t.produce(producerCtx, cfg.payload, fmt.Sprintf("%s-%d", namespace, i), cfg.rate*1e6)
			if producerCtx.Err() == nil {
				cancel(fmt.Errorf("%s: %w", address, err))
			}
		})
	}
	for _, t := range targets {
		select {
		case <-t.ready:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.MaxIdleConnsPerHost = cfg.maxInFlight
	httpTransport.MaxIdleConns = 0
	defer httpTransport.CloseIdleConnections()
	client := &http.Client{Transport: httpTransport, Timeout: 60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var logLock sync.Mutex
	logf := func(format string, args ...any) {
		logLock.Lock()
		defer logLock.Unlock()
		if _, err := fmt.Fprintf(logs, format, args...); err != nil {
			cancel(fmt.Errorf("write load generator log: %w", err))
		}
	}
	now := time.Now()
	start := now.Add(-time.Duration(now.Nanosecond() % int(time.Millisecond)))
	logf("PERF_START %d\n", start.UnixMilli())
	results := make([]*targetReport, len(targets))
	var senders sync.WaitGroup
	for i, t := range targets {
		senders.Go(func() {
			// Stagger hosts evenly within one arrival interval.
			offset := time.Duration(float64(t.interval) * (float64(i) + 0.5) / float64(len(targets)))
			results[i] = t.send(ctx, cfg, client, start, offset, logf, stops[i])
		})
	}
	senders.Wait()
	producers.Wait()
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	summary := &report{PerTarget: make(map[string]*targetReport), RingBytes: cfg.ringBytes}
	for i, t := range targets {
		result := results[i]
		result.RingPeakBytes = t.ring.peak
		summary.PerTarget[t.url] = result
		summary.add(&result.totals)
		summary.Measure.add(&result.Measure)
	}
	summary.finish(cfg.warmup + cfg.duration)
	summary.Measure.finish(cfg.duration)
	return summary, nil
}

// Use type parsing for environment defaults as well as command-line values.
type envDefaults struct{ err error }

func (e *envDefaults) get(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func (e *envDefaults) duration(name, fallback string) time.Duration {
	value, err := time.ParseDuration(e.get(name, fallback))
	if err != nil && e.err == nil {
		e.err = fmt.Errorf("%s: %w", name, err)
	}
	return value
}

func (e *envDefaults) integer(name, fallback string) int64 {
	value, err := strconv.ParseInt(e.get(name, fallback), 10, 64)
	if err != nil && e.err == nil {
		e.err = fmt.Errorf("%s: %w", name, err)
	}
	return value
}

func main() {
	var cfg config
	var env envDefaults
	var targets, path, anchor string
	rate, err := strconv.ParseFloat(env.get("TARGET_WIRE_MBPS_PER_SERVER", "30"), 64)
	if err != nil {
		log.Fatal(err)
	}
	flag.StringVar(&targets, "targets", env.get("TARGETS", ""), "comma-separated ingest base URLs")
	flag.Float64Var(&cfg.rate, "wire-mb-s", rate, "decimal compressed MB/s per target")
	flag.DurationVar(&cfg.duration, "duration", env.duration("DURATION", "180s"), "measurement duration")
	flag.DurationVar(&cfg.warmup, "warmup", env.duration("WARMUP", "60s"), "warmup duration")
	flag.DurationVar(&cfg.grace, "grace", env.duration("PERF_GRACE", "30s"), "response drain after the measurement window")
	flag.DurationVar(&cfg.linger, "linger", 0, "keep the container available to the resource sampler after measurement")
	flag.IntVar(&cfg.maxInFlight, "max-in-flight", int(env.integer("MAX_IN_FLIGHT", "64")), "maximum requests in flight per target")
	flag.Int64Var(&cfg.ringBytes, "ring-bytes", 256<<20, "total queued compressed body budget across targets")
	flag.Int64Var(&cfg.payload.Seed, "seed", env.integer("SEED", "42"), "deterministic payload seed")
	flag.IntVar(&cfg.payload.EventsPerBatch, "events-per-batch", 1000, "events per payload")
	flag.IntVar(&cfg.payload.Assets, "assets", 16, "number of assets")
	flag.StringVar(&anchor, "anchor", "", "RFC3339 payload timestamp anchor (default: current UTC time)")
	flag.StringVar(&path, "summary", "summary.json", "summary JSON path (- for stdout)")
	flag.Parse()
	if env.err != nil {
		log.Fatal(env.err)
	}
	if cfg.rate <= 0 || math.IsNaN(cfg.rate) || math.IsInf(cfg.rate, 0) || cfg.duration <= 0 || cfg.warmup < 0 ||
		cfg.grace < 0 || cfg.linger < 0 || cfg.maxInFlight < 1 || cfg.ringBytes < 1 {
		log.Fatal("rate, duration, max-in-flight and ring-bytes must be positive; warmup, grace and linger must be nonnegative")
	}
	if anchor != "" {
		cfg.payload.Anchor, err = time.Parse(time.RFC3339Nano, anchor)
		if err != nil {
			log.Fatal(err)
		}
	}
	seen := make(map[string]bool)
	for address := range strings.SplitSeq(targets, ",") {
		address = strings.TrimRight(strings.TrimSpace(address), "/")
		parsed, err := url.Parse(address)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" ||
			parsed.User != nil || parsed.Path != "" || parsed.ForceQuery || parsed.RawQuery != "" ||
			parsed.Fragment != "" || seen[address] {
			log.Fatalf("expected a distinct HTTP(S) ingest base URL, got %q", address)
		}
		cfg.targets = append(cfg.targets, address)
		seen[address] = true
	}
	summary, err := run(context.Background(), cfg, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
	data, err := json.Marshal(summary)
	if err != nil {
		log.Fatal(err)
	}
	if path == "-" {
		fmt.Printf("PERF_SUMMARY %s\n", data)
	} else if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		log.Fatal(err)
	}
	// The runner derives its exact windows from PERF_START. Keep the stats
	// container alive past the end of active arrivals even after a fast drain.
	time.Sleep(cfg.linger)
}
