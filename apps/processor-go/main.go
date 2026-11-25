package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/sourcedb"
	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/sourcejs"
	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/state"
	"github.com/cloud4-final/jammy-5th/libs/go/admin"
	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/cloud4-final/jammy-5th/libs/go/dbx"
	"github.com/cloud4-final/jammy-5th/libs/go/leasefence"
	"github.com/cloud4-final/jammy-5th/libs/go/lifecycle"
	"github.com/cloud4-final/jammy-5th/libs/go/logx"
	"github.com/cloud4-final/jammy-5th/libs/go/natsx"
	"github.com/cloud4-final/jammy-5th/libs/go/obs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type settings struct {
	config.Common
	streams                       []string
	consumer                      string
	ackWait, stateFlush, lookback time.Duration
	maxPending, maxDeliver, fetch int
	ratio                         float64
	fence                         *leasefence.Fence
}

func load() (settings, error) {
	common, err := config.LoadCommon()
	if err != nil {
		return settings{}, err
	}
	cfg := settings{Common: common}
	cfg.fence, err = leasefence.FromEnv(common.SiteID)
	if err != nil {
		return cfg, err
	}
	for _, item := range []struct {
		name, fallback string
		dest           *time.Duration
	}{
		{"ACK_WAIT_SECONDS", "30", &cfg.ackWait},
		{"STATE_FLUSH_SECONDS", "1", &cfg.stateFlush},
		{"GATE_MAX_PAST", "24h", &cfg.lookback},
	} {
		value, err := config.Lookup(item.name, item.fallback)
		if err != nil {
			return cfg, err
		}
		*item.dest, err = config.ParseDuration(item.name, value)
		if err != nil || (*item.dest == 0 && item.name != "GATE_MAX_PAST") {
			return cfg, fmt.Errorf("invalid %s", item.name)
		}
	}
	for _, item := range []struct {
		name, fallback string
		dest           *int
	}{
		{"MAX_ACK_PENDING", "1000", &cfg.maxPending},
		{"PROCESSOR_MAX_DELIVER", "10", &cfg.maxDeliver},
		{"FETCH_BATCH", "64", &cfg.fetch},
	} {
		value, err := config.Lookup(item.name, item.fallback)
		if err != nil {
			return cfg, err
		}
		n, err := strconv.ParseUint(value, 10, 31)
		if err != nil || n == 0 {
			return cfg, fmt.Errorf("invalid %s", item.name)
		}
		*item.dest = int(n)
	}
	cfg.consumer, err = config.Lookup("CONSUMER_NAME", "processor")
	if err != nil {
		return cfg, err
	}
	if cfg.consumer == "" {
		return cfg, errors.New("CONSUMER_NAME is required")
	}
	value, err := config.Lookup("ANOMALY_DROP_RATIO", "0.5")
	if err != nil {
		return cfg, err
	}
	cfg.ratio, err = strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(cfg.ratio) || cfg.ratio <= 0 || cfg.ratio > 1 {
		return cfg, errors.New("invalid ANOMALY_DROP_RATIO")
	}
	if cfg.PipelineMode == "jetstream" {
		streams, err := config.Lookup("NATS_STREAMS", "")
		if err != nil {
			return cfg, err
		}
		cfg.streams, err = config.ParseList(streams)
		if err != nil {
			return cfg, fmt.Errorf("invalid NATS_STREAMS: %w", err)
		}
		seen := make(map[string]struct{}, len(cfg.streams))
		for _, stream := range cfg.streams {
			if _, dup := seen[stream]; dup {
				return cfg, fmt.Errorf("invalid NATS_STREAMS: duplicate stream %q", stream)
			}
			seen[stream] = struct{}{}
		}
		natsURL, err := config.Lookup("NATS_URL", "")
		if err != nil {
			return cfg, err
		}
		if natsURL == "" {
			return cfg, errors.New("NATS_URL is required")
		}
		if err := validateNATSURL(natsURL); err != nil {
			return cfg, err
		}
	}
	dbURL, err := config.Lookup("DATABASE_URL", "")
	if err != nil {
		return cfg, err
	}
	if dbURL == "" {
		return cfg, errors.New("DATABASE_URL is required")
	}
	if _, err := pgxpool.ParseConfig(dbURL); err != nil {
		return cfg, errors.New("invalid DATABASE_URL")
	}
	return cfg, nil
}

func validateNATSURL(raw string) error {
	for part := range strings.SplitSeq(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return errors.New("invalid NATS_URL")
		}
		if !strings.Contains(part, "://") {
			part = "nats://" + part
		}
		parsed, err := url.Parse(part)
		if err != nil || parsed.Host == "" {
			return errors.New("invalid NATS_URL")
		}
	}
	return nil
}

type observation struct {
	ready        atomic.Bool
	pending, lag atomic.Int64
}

func (cfg settings) run(ctx context.Context) error {
	if err := logx.Setup("processor", cfg.Common); err != nil {
		return err
	}
	meter, stop, err := obs.Setup("processor", cfg.Common)
	if err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = stop(c)
	}()
	pool, err := dbx.Open(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := state.New(pool, cfg.consumer, cfg.lookback, cfg.ratio, cfg.fence)
	numSources := max(1, len(cfg.streams))
	sources := make([]observation, numSources)
	var dbReady atomic.Bool
	var nc *nats.Conn
	var js jetstream.JetStream
	if cfg.PipelineMode == "jetstream" {
		nc, err = natsx.Connect()
		if err != nil {
			return err
		}
		defer nc.Close()
		js, err = jetstream.New(nc)
		if err != nil {
			return err
		}
	}
	var assetsReady atomic.Bool
	var streamCheck func(context.Context, string) error
	if js != nil {
		streamCheck = func(check context.Context, stream string) error {
			_, err := js.Stream(check, stream)
			return err
		}
	}
	ready := func(ctx context.Context) bool {
		return checkDependencies(
			ctx,
			dbReady.Load(),
			assetsReady.Load(),
			sources,
			cfg.streams,
			nc != nil && nc.IsConnected(),
			streamCheck,
			pool.Ping,
		)
	}
	probe := admin.New(cfg.AdminPort, map[string]func(context.Context) bool{"dependencies": ready})
	if err := probe.Start(); err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = probe.Shutdown(c)
	}()
	batches, e1 := meter.Int64Counter("jammy_processor_batches")
	commit, e2 := meter.Float64Histogram("jammy_processor_commit_duration", metric.WithUnit("s"))
	pending, e3 := meter.Int64ObservableGauge("jammy_processor_consumer_pending")
	lag, e4 := meter.Float64ObservableGauge("jammy_processor_lag", metric.WithUnit("s"))
	if err := errors.Join(e1, e2, e3, e4); err != nil {
		return err
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var count, age int64
		for i := range sources {
			count += sources[i].pending.Load()
			age = max(age, sources[i].lag.Load())
		}
		o.ObserveInt64(pending, count)
		o.ObserveFloat64(lag, time.Duration(age).Seconds())
		return nil
	}, pending, lag)
	if err != nil {
		return err
	}
	defer func() { _ = reg.Unregister() }()
	record := func(ctx context.Context, result string, n int) {
		batches.Add(ctx, int64(n), metric.WithAttributes(attribute.String("result", result)))
	}
	tracer := otel.Tracer("jammy.processor")
	var workers sync.WaitGroup
	workers.Go(func() {
		var maintenance time.Time
		for ctx.Err() == nil {
			check, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := pool.Ping(check)
			if err == nil && time.Now().After(maintenance) {
				err = store.Prepare(check)
				if err == nil {
					maintenance = time.Now().Add(10 * time.Minute)
				}
			}
			cancel()
			dbReady.Store(err == nil || errors.Is(err, leasefence.ErrFenced))
			if err != nil && !errors.Is(err, leasefence.ErrFenced) {
				maintenance = time.Time{}
			}
			pause(ctx, retryDelay)
		}
	})
	workers.Go(func() {
		for ctx.Err() == nil {
			check, cancel := context.WithTimeout(ctx, time.Second)
			err := store.RefreshAssets(check)
			cancel()
			if err != nil {
				pause(ctx, retryDelay)
				continue
			}
			assetsReady.Store(true)
			pause(ctx, assetRefreshInterval)
		}
	})
	workers.Go(func() {
		ticker := time.NewTicker(cfg.stateFlush)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				work, span := tracer.Start(context.WithoutCancel(ctx), "processor.flush")
				err := store.FlushLatest(work)
				if err != nil && !errors.Is(err, leasefence.ErrFenced) {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					slog.WarnContext(work, "Latest state flush failed", "error", err)
				}
				span.End()
				if err != nil && !errors.Is(err, leasefence.ErrFenced) {
					pause(ctx, retryDelay)
				}
			}
		}
	})
	for i := range sources {
		workers.Go(func() {
			source := &sources[i]
			var consumer jetstream.Consumer
			for ctx.Err() == nil {
				if !dbReady.Load() || !assetsReady.Load() || (nc != nil && !nc.IsConnected()) {
					source.ready.Store(false)
					pause(ctx, retryDelay)
					continue
				}
				work := context.WithoutCancel(ctx)
				if js == nil {
					work, span := tracer.Start(work, "processor.poll", trace.WithSpanKind(trace.SpanKindConsumer))
					start := time.Now()
					count, lastReceived, err := sourcedb.Poll(work, store, cfg.fetch)
					fenced := errors.Is(err, leasefence.ErrFenced)
					source.ready.Store(err == nil || fenced)
					if err != nil {
						if fenced {
							record(work, "fenced", 1)
						} else {
							span.RecordError(err)
							span.SetStatus(codes.Error, err.Error())
							record(work, "error", 1)
							slog.WarnContext(work, "Direct DB processor poll failed", "error", err)
						}
						span.End()
						pause(ctx, retryDelay)
					} else {
						source.lag.Store(receivedLag(time.Now(), lastReceived))
						if count > 0 {
							span.SetAttributes(attribute.Int("batch.events", count))
							record(work, "ok", 1)
							commit.Record(work, time.Since(start).Seconds())
						}
						span.End()
						if count == 0 {
							pause(ctx, idleDelay)
						}
					}
					continue
				}
				if consumer == nil {
					c, err := js.CreateOrUpdateConsumer(ctx, cfg.streams[i], cfg.consumerConfig(cfg.streams[i]))
					if err != nil {
						source.ready.Store(false)
						pause(ctx, retryDelay)
						continue
					}
					consumer = c
				}
				messages, err := consumer.Fetch(cfg.fetch, jetstream.FetchMaxWait(fetchWait))
				if err != nil {
					consumer = nil
					source.ready.Store(false)
					pause(ctx, retryDelay)
					continue
				}
				source.ready.Store(true)
				var fetched []jetstream.Msg
				for message := range messages.Messages() {
					fetched = append(fetched, message)
				}
				if len(fetched) > 0 {
					work, span := startBatchSpan(work, tracer, cfg.streams[i], fetched)
					start := time.Now()
					duplicates, lastReceived, err := sourcejs.Process(work, store, fetched, func() { record(work, "poison", 1) })
					if len(duplicates) > 0 {
						commit.Record(work, time.Since(start).Seconds())
						for _, duplicate := range duplicates {
							if duplicate {
								record(work, "duplicate", 1)
							} else {
								record(work, "ok", 1)
							}
						}
					}
					if !lastReceived.IsZero() {
						source.lag.Store(receivedLag(time.Now(), lastReceived))
					}
					if err != nil {
						if errors.Is(err, leasefence.ErrFenced) {
							record(work, "fenced", sourcejs.FailedCount(err))
						} else {
							span.RecordError(err)
							span.SetStatus(codes.Error, err.Error())
							record(work, "error", sourcejs.FailedCount(err))
							slog.WarnContext(work, "JetStream processor batch failed", "stream", cfg.streams[i], "error", err)
						}
						span.End()
						pause(ctx, retryDelay)
						continue
					}
					span.End()
				} else {
					source.lag.Store(0)
				}
				if err := messages.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
					consumer = nil
					source.ready.Store(false)
					pause(ctx, retryDelay)
				} else {
					if info, err := consumer.Info(ctx); err != nil {
						if ctx.Err() == nil {
							consumer = nil
							source.ready.Store(false)
							slog.WarnContext(work, "Consumer status unavailable", "stream", cfg.streams[i], "error", err)
							pause(ctx, retryDelay)
						}
						continue
					} else {
						updateSourceObservation(source, int64(info.NumPending)+int64(info.NumAckPending))
					}
					if len(fetched) == 0 {
						pause(ctx, idleDelay)
					}
				}
			}
		})
	}
	<-ctx.Done()
	probe.SetDraining()
	workers.Wait()
	drain, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	err = store.FlushLatest(drain)
	if errors.Is(err, leasefence.ErrFenced) {
		return nil
	}
	return err
}

const (
	fetchWait            = time.Second
	idleDelay            = 200 * time.Millisecond
	retryDelay           = time.Second
	assetRefreshInterval = time.Minute
)

func (cfg settings) consumerConfig(stream string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable: cfg.consumer, AckPolicy: jetstream.AckExplicitPolicy, AckWait: cfg.ackWait,
		MaxAckPending: cfg.maxPending, MaxDeliver: cfg.maxDeliver, FilterSubject: "jammy.raw." + stream,
	}
}

type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string {
	if c == nil {
		return ""
	}
	if v := c[key]; len(v) > 0 {
		return v[0]
	}
	for k, v := range c {
		if strings.EqualFold(k, key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func (c natsHeaderCarrier) Set(key, value string) {
	c[key] = []string{value}
}

func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func tracePropagator() propagation.TextMapPropagator {
	if p := otel.GetTextMapPropagator(); len(p.Fields()) > 0 {
		return p
	}
	return propagation.TraceContext{}
}

func startBatchSpan(ctx context.Context, tracer trace.Tracer, stream string, messages []jetstream.Msg) (context.Context, trace.Span) {
	parent := ctx
	var links []trace.Link
	prop := tracePropagator()
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		extracted := prop.Extract(ctx, natsHeaderCarrier(msg.Headers()))
		if sc := trace.SpanContextFromContext(extracted); sc.IsValid() {
			if len(messages) == 1 {
				parent = extracted
			} else {
				links = append(links, trace.Link{SpanContext: sc})
			}
		}
	}
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.destination.name", stream),
		attribute.Int("messaging.batch.message_count", len(messages)),
	}
	if len(messages) == 1 && messages[0] != nil {
		if id := natsHeaderCarrier(messages[0].Headers()).Get("Nats-Msg-Id"); id != "" {
			attrs = append(attrs, attribute.String("batch_id", id))
		}
	}
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	}
	if len(links) > 0 {
		opts = append(opts, trace.WithLinks(links...))
	}
	return tracer.Start(parent, "processor.commit", opts...)
}

func updateSourceObservation(source *observation, pending int64) {
	source.pending.Store(pending)
	if pending == 0 {
		source.lag.Store(0)
	}
}

func checkDependencies(
	ctx context.Context,
	dbReady, assetsReady bool,
	sources []observation,
	streams []string,
	natsConnected bool,
	streamCheck func(context.Context, string) error,
	dbPing func(context.Context) error,
) bool {
	if !dbReady || !assetsReady {
		return false
	}
	for i := range sources {
		if !sources[i].ready.Load() {
			return false
		}
	}
	check, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if streamCheck != nil {
		if !natsConnected {
			return false
		}
		for _, stream := range streams {
			if err := streamCheck(check, stream); err != nil {
				return false
			}
		}
	}
	return dbPing(check) == nil
}

func receivedLag(now, received time.Time) int64 {
	if received.IsZero() {
		return 0
	}
	return max(0, int64(now.Sub(received)))
}

func pause(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func main() {
	cfg, err := load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	lifecycle.Run(cfg.run, cfg.ShutdownTimeout)
}
