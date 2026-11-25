package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud4-final/jammy-application/apps/archiver-go/internal/exportdb"
	"github.com/cloud4-final/jammy-application/apps/archiver-go/internal/pq"
	"github.com/cloud4-final/jammy-application/apps/archiver-go/internal/sourcejs"
	"github.com/cloud4-final/jammy-application/libs/go/admin"
	"github.com/cloud4-final/jammy-application/libs/go/config"
	"github.com/cloud4-final/jammy-application/libs/go/dbx"
	"github.com/cloud4-final/jammy-application/libs/go/lifecycle"
	"github.com/cloud4-final/jammy-application/libs/go/logx"
	"github.com/cloud4-final/jammy-application/libs/go/natsx"
	"github.com/cloud4-final/jammy-application/libs/go/obs"
	"github.com/cloud4-final/jammy-application/libs/go/s3x"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type settings struct {
	config.Common
	streams                          []string
	consumer, stage, bucket          string
	s3Prefix                         string
	flushInterval, lateness, ackWait time.Duration
	flushBytes, uploadBPS            int64
	zstdLevel                        int
}

func load() (settings, error) {
	common, err := config.LoadCommon()
	if err != nil {
		return settings{}, err
	}
	cfg := settings{Common: common}
	for _, item := range []struct {
		name, fallback string
		dest           *string
	}{
		{"CONSUMER_NAME", "archiver", &cfg.consumer},
		{"STAGE_DIR", "/stage", &cfg.stage},
		{"S3_BUCKET", "", &cfg.bucket},
	} {
		value, err := config.Lookup(item.name, item.fallback)
		if err != nil {
			return cfg, err
		}
		if value == "" {
			return cfg, fmt.Errorf("%s is required", item.name)
		}
		*item.dest = value
	}
	for _, item := range []struct {
		name, fallback string
		dest           *time.Duration
	}{
		{"ARCHIVE_FLUSH_SECONDS", "10", &cfg.flushInterval},
		{"SEAL_LATENESS", "10m", &cfg.lateness},
	} {
		value, err := config.Lookup(item.name, item.fallback)
		if err != nil {
			return cfg, err
		}
		*item.dest, err = config.ParseDuration(item.name, value)
		if err != nil {
			return cfg, err
		}
	}
	if cfg.flushInterval == 0 {
		return cfg, errors.New("ARCHIVE_FLUSH_SECONDS must be positive")
	}
	for _, item := range []struct {
		name, fallback string
		dest           *int64
	}{
		{"ARCHIVE_FLUSH_BYTES", "64MiB", &cfg.flushBytes},
		{"ARCHIVE_MAX_UPLOAD_BPS", "0", &cfg.uploadBPS},
	} {
		value, err := config.Lookup(item.name, item.fallback)
		if err != nil {
			return cfg, err
		}
		*item.dest, err = config.ParseSize(value)
		if err != nil {
			return cfg, fmt.Errorf("invalid %s", item.name)
		}
	}
	if cfg.flushBytes == 0 {
		return cfg, errors.New("ARCHIVE_FLUSH_BYTES must be positive")
	}
	level, err := config.Lookup("ARCHIVE_ZSTD_LEVEL", "1")
	if err != nil {
		return cfg, err
	}
	cfg.zstdLevel, err = strconv.Atoi(level)
	if err != nil || cfg.zstdLevel < 1 || cfg.zstdLevel > 22 {
		return cfg, errors.New("ARCHIVE_ZSTD_LEVEL must be in 1..22")
	}
	cfg.s3Prefix, err = s3x.Prefix()
	if err != nil {
		return cfg, err
	}
	if cfg.PipelineMode == "jetstream" {
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
			if _, duplicate := seen[stream]; duplicate {
				return cfg, fmt.Errorf("invalid NATS_STREAMS: duplicate stream %q", stream)
			}
			seen[stream] = struct{}{}
		}
	} else {
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
		minimum, err := config.Lookup("DB_POOL_MIN", "1")
		if err != nil {
			return cfg, err
		}
		maximum, err := config.Lookup("DB_POOL_MAX", "10")
		if err != nil {
			return cfg, err
		}
		minConns, err := strconv.ParseUint(minimum, 10, 31)
		if err != nil {
			return cfg, errors.New("invalid DB_POOL_MIN")
		}
		maxConns, err := strconv.ParseUint(maximum, 10, 31)
		if err != nil || maxConns == 0 || maxConns < minConns {
			return cfg, errors.New("DB_POOL_MAX must be positive and at least DB_POOL_MIN")
		}
	}
	const ackWaitName = "ACK_WAIT_SECONDS"
	const overhead = 60 * time.Second
	fallback := ""
	if cfg.flushInterval > (time.Duration(math.MaxInt64)-overhead)/3 {
		_, hasValue := os.LookupEnv(ackWaitName)
		_, hasFile := os.LookupEnv(ackWaitName + "_FILE")
		if !hasValue && !hasFile {
			return cfg, errors.New("ARCHIVE_FLUSH_SECONDS is too large to derive ACK_WAIT_SECONDS")
		}
	} else {
		fallback = strconv.FormatInt(int64((3*cfg.flushInterval+overhead)/time.Second), 10)
	}
	ack, err := config.Lookup(ackWaitName, fallback)
	if err != nil {
		return cfg, err
	}
	cfg.ackWait, err = config.ParseDuration(ackWaitName, ack)
	if err != nil || cfg.ackWait == 0 {
		return cfg, errors.New("invalid ACK_WAIT_SECONDS")
	}
	return cfg, nil
}

type streamStatus struct {
	name    string
	ready   atomic.Bool
	unacked atomic.Int64
}

func (cfg settings) run(ctx context.Context) error {
	if err := logx.Setup("archiver", cfg.Common); err != nil {
		return err
	}
	meter, stop, err := obs.Setup("archiver", cfg.Common)
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = stop(shutdown)
	}()
	if err := prepareStage(cfg.stage); err != nil {
		return err
	}
	client, err := s3x.New(ctx)
	if err != nil {
		return err
	}
	opts := client.Options()
	opts.Retryer = aws.NopRetryer{}
	client = s3.New(opts)
	objects := pq.S3{Client: client, Bucket: cfg.bucket, Limit: cfg.uploadBPS}
	if cfg.uploadBPS > 0 {
		objects.UploadSlot = make(chan struct{}, 1)
	}

	var nc *nats.Conn
	var js jetstream.JetStream
	var pool *pgxpool.Pool
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
	} else {
		pool, err = dbx.Open(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()
	}
	streams := make([]streamStatus, len(cfg.streams))
	for i, stream := range cfg.streams {
		streams[i].name = stream
	}
	ready := func(ctx context.Context) bool {
		return isReady(ctx, cfg.bucket, client, pool, nc, js, streams)
	}
	probe := admin.New(cfg.AdminPort, map[string]func(context.Context) bool{"dependencies": ready})
	if err := probe.Start(); err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = probe.Shutdown(shutdown)
	}()
	files, e1 := meter.Int64Counter("jammy_archiver_files")
	bytes, e2 := meter.Int64Counter("jammy_archiver_upload_bytes")
	duration, e3 := meter.Float64Histogram("jammy_archiver_upload_duration", metric.WithUnit("s"))
	unacked, e4 := meter.Int64ObservableGauge("jammy_archiver_unacked_messages")
	if err := errors.Join(e1, e2, e3, e4); err != nil {
		return err
	}
	registration, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var total int64
		for i := range streams {
			total += streams[i].unacked.Load()
		}
		o.ObserveInt64(unacked, total)
		return nil
	}, unacked)
	if err != nil {
		return err
	}
	defer func() { _ = registration.Unregister() }()
	recordFile := func(ctx context.Context, result string) {
		files.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
	}
	archive := pq.Archive{Objects: objects, Dir: cfg.stage, Level: cfg.zstdLevel,
		Uploaded: func(ctx context.Context, size int64, elapsed time.Duration) {
			bytes.Add(ctx, size)
			duration.Record(ctx, elapsed.Seconds())
		},
		Written: func(ctx context.Context) { recordFile(ctx, "ok") },
	}

	var workers sync.WaitGroup
	tracer := otel.Tracer("jammy.archiver")
	if pool != nil {
		exporter := exportdb.Exporter{Pool: pool, Archive: archive, Lateness: cfg.lateness, KeyPrefix: cfg.s3Prefix}
		workers.Go(func() {
			for ctx.Err() == nil {
				work, span := tracer.Start(context.WithoutCancel(ctx), "archive.export")
				exported, err := exporter.ExportNext(work)
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					slog.WarnContext(work, "Direct DB archive export failed", "error", err)
				}
				span.End()
				if err != nil {
					pause(ctx, sourcejs.RetryWait)
				} else if !exported {
					pause(ctx, cfg.flushInterval)
				}
			}
		})
	} else {
		archiver := sourcejs.Archiver{
			Archive: archive, Lateness: cfg.lateness, KeyPrefix: cfg.s3Prefix,
			Poison: func(ctx context.Context) { recordFile(ctx, "poison") },
		}
		for i, stream := range cfg.streams {
			workers.Go(func() {
				status := &streams[i]
				for ctx.Err() == nil {
					if nc.IsConnected() {
						consumer, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
							Durable: cfg.consumer, AckPolicy: jetstream.AckExplicitPolicy, AckWait: cfg.ackWait,
							FilterSubject: "jammy.raw." + stream,
						})
						if err == nil {
							status.ready.Store(true)
							err = sourcejs.Run(ctx, consumer, cfg.flushBytes, cfg.flushInterval, cfg.ackWait, drainBudget(cfg.ShutdownTimeout), func(work context.Context, messages []jetstream.Msg) error {
								parent, links := sourcejs.FlushTraceContext(work, messages)
								work, span := tracer.Start(parent, "archive.flush", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithLinks(links...))
								defer span.End()
								err := archiver.Flush(work, messages)
								if err != nil {
									span.RecordError(err)
									span.SetStatus(codes.Error, err.Error())
									slog.WarnContext(work, "JetStream archive flush failed", "error", err)
								}
								return err
							}, func(n int) { status.unacked.Store(int64(n)) })
						}
						if err != nil && ctx.Err() == nil {
							slog.WarnContext(ctx, "JetStream archive consumer unavailable")
						}
					}
					status.ready.Store(false)
					pause(ctx, sourcejs.RetryWait)
				}
			})
		}
	}
	<-ctx.Done()
	probe.SetDraining()
	workers.Wait()
	return nil
}

func isReady(ctx context.Context, bucket string, client *s3.Client, pool *pgxpool.Pool, nc *nats.Conn, js jetstream.JetStream, streams []streamStatus) bool {
	check, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if pool != nil {
		if err := pool.Ping(check); err != nil {
			return false
		}
	} else {
		if nc == nil || !nc.IsConnected() || nc.FlushWithContext(check) != nil {
			return false
		}
		for i := range streams {
			if !streams[i].ready.Load() {
				return false
			}
			if _, err := js.Stream(check, streams[i].name); err != nil {
				return false
			}
		}
	}
	return s3Ready(check, client, bucket)
}

func s3Ready(ctx context.Context, client *s3.Client, bucket string) bool {
	check, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err := client.HeadBucket(check, &s3.HeadBucketInput{Bucket: &bucket})
	return err == nil
}

func validateNATSURL(raw string) error {
	invalid := errors.New("invalid NATS_URL")
	var hasTCP, hasWebSocket bool
	for part := range strings.SplitSeq(raw, ",") {
		parsed, err := url.Parse(strings.TrimSpace(part))
		if err != nil || parsed.Hostname() == "" || strings.ContainsFunc(parsed.Hostname(), unicode.IsSpace) {
			return invalid
		}
		if port := parsed.Port(); port != "" {
			if _, err := strconv.ParseUint(port, 10, 16); err != nil {
				return invalid
			}
		}
		switch parsed.Scheme {
		case "nats", "tls":
			hasTCP = true
		case "ws", "wss":
			hasWebSocket = true
		default:
			return invalid
		}
	}
	if hasTCP && hasWebSocket {
		return invalid
	}
	return nil
}

func prepareStage(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || !strings.HasPrefix(name, "jammy-") || !strings.HasSuffix(name, ".parquet") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func drainBudget(shutdownTimeout time.Duration) time.Duration {
	if shutdownTimeout <= 0 {
		return 0
	}
	return shutdownTimeout - min(2*time.Second, shutdownTimeout/4)
}

func pause(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
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
