package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/exportdb"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/pq"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/sourcejs"
	"github.com/cloud4-final/jammy-5th/libs/go/admin"
	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/cloud4-final/jammy-5th/libs/go/dbx"
	"github.com/cloud4-final/jammy-5th/libs/go/lifecycle"
	"github.com/cloud4-final/jammy-5th/libs/go/logx"
	"github.com/cloud4-final/jammy-5th/libs/go/natsx"
	"github.com/cloud4-final/jammy-5th/libs/go/obs"
	"github.com/cloud4-final/jammy-5th/libs/go/s3x"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type settings struct {
	config.Common
	streams                          []string
	consumer, stage, bucket          string
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
	if cfg.PipelineMode == "jetstream" {
		streams, err := config.Lookup("NATS_STREAMS", "")
		if err != nil {
			return cfg, err
		}
		cfg.streams, err = config.ParseList(streams)
		if err != nil {
			return cfg, fmt.Errorf("invalid NATS_STREAMS: %w", err)
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
	if err := os.MkdirAll(cfg.stage, 0755); err != nil {
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
	ready := func(ctx context.Context) bool {
		check, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if pool != nil {
			if err := pool.Ping(check); err != nil {
				return false
			}
		} else {
			if !nc.IsConnected() || nc.FlushWithContext(check) != nil {
				return false
			}
			for i := range streams {
				if !streams[i].ready.Load() {
					return false
				}
			}
		}
		_, err := client.HeadBucket(check, &s3.HeadBucketInput{Bucket: &cfg.bucket})
		return err == nil
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
		exporter := exportdb.Exporter{Pool: pool, Archive: archive, Lateness: cfg.lateness}
		workers.Go(func() {
			for ctx.Err() == nil {
				work, span := tracer.Start(context.WithoutCancel(ctx), "archive.export")
				exported, err := exporter.ExportNext(work)
				if err != nil {
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
			Archive: archive, Lateness: cfg.lateness,
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
							err = sourcejs.Run(ctx, consumer, cfg.flushBytes, cfg.flushInterval, cfg.ackWait, func(work context.Context, messages []jetstream.Msg) error {
								work, span := tracer.Start(work, "archive.flush")
								defer span.End()
								err := archiver.Flush(work, messages)
								if err != nil {
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
