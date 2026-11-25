package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cloud4-final/jammy-application/apps/ingest-go/internal/server"
	"github.com/cloud4-final/jammy-application/apps/ingest-go/internal/sinkdb"
	"github.com/cloud4-final/jammy-application/apps/ingest-go/internal/sinkjs"
	"github.com/cloud4-final/jammy-application/libs/go/admin"
	"github.com/cloud4-final/jammy-application/libs/go/config"
	"github.com/cloud4-final/jammy-application/libs/go/dbx"
	"github.com/cloud4-final/jammy-application/libs/go/leasefence"
	"github.com/cloud4-final/jammy-application/libs/go/lifecycle"
	"github.com/cloud4-final/jammy-application/libs/go/logx"
	"github.com/cloud4-final/jammy-application/libs/go/natsx"
	"github.com/cloud4-final/jammy-application/libs/go/obs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
)

type settings struct {
	config.Common
	intake          server.Config
	stream          string
	maxAge          time.Duration
	maxBytes        int64
	quarantineAge   time.Duration
	quarantineBytes int64
	fence           *leasefence.Fence
}

func load() (settings, error) {
	common, err := config.LoadCommon()
	if err != nil {
		return settings{}, err
	}
	cfg := settings{Common: common, intake: server.Config{Instance: common.InstanceID}}
	for _, setting := range []struct {
		name, fallback string
		dest           *int64
	}{
		{"INGEST_MAX_BODY_BYTES", "16MiB", &cfg.intake.MaxBody},
		{"INGEST_MAX_DECOMPRESSED_BYTES", "64MiB", &cfg.intake.MaxDecoded},
		{"JS_MAX_BYTES", "60GiB", &cfg.maxBytes},
		{"QUARANTINE_MAX_BYTES", "1GiB", &cfg.quarantineBytes},
	} {
		value, err := config.Lookup(setting.name, setting.fallback)
		if err != nil {
			return settings{}, err
		}
		*setting.dest, err = config.ParseSize(value)
		if err != nil || *setting.dest <= 0 {
			return settings{}, fmt.Errorf("invalid %s", setting.name)
		}
	}
	for _, setting := range []struct {
		name, fallback string
		dest           *time.Duration
	}{
		{"GATE_MAX_PAST", "24h", &cfg.intake.Gate.MaxPast},
		{"GATE_MAX_FUTURE", "5m", &cfg.intake.Gate.MaxFuture},
		{"JS_MAX_AGE", "45m", &cfg.maxAge},
		{"QUARANTINE_MAX_AGE", "168h", &cfg.quarantineAge},
	} {
		value, err := config.Lookup(setting.name, setting.fallback)
		if err != nil {
			return settings{}, err
		}
		*setting.dest, err = config.ParseDuration(setting.name, value)
		if err != nil {
			return settings{}, err
		}
	}
	if cfg.quarantineAge <= 0 {
		return settings{}, errors.New("QUARANTINE_MAX_AGE must be positive")
	}
	value, err := config.Lookup("INGEST_MAX_INFLIGHT", "64")
	if err != nil {
		return settings{}, err
	}
	inflight, err := strconv.ParseUint(value, 10, 31)
	if err != nil || inflight == 0 {
		return settings{}, errors.New("invalid INGEST_MAX_INFLIGHT")
	}
	cfg.intake.Inflight = int(inflight)
	if common.PipelineMode == "jetstream" {
		natsURL, err := config.Lookup("NATS_URL", "")
		if err != nil {
			return settings{}, err
		}
		if natsURL == "" {
			return settings{}, errors.New("NATS_URL is required")
		}
		cfg.stream, err = config.Lookup("INGEST_STREAM", "")
		if err != nil {
			return settings{}, err
		}
		if cfg.stream == "" {
			return settings{}, errors.New("INGEST_STREAM is required")
		}
		if strings.ContainsAny(cfg.stream, ">*. /\\\t\r\n") {
			return settings{}, errors.New("invalid INGEST_STREAM")
		}
	} else {
		dbURL, err := config.Lookup("DATABASE_URL", "")
		if err != nil {
			return settings{}, err
		}
		if dbURL == "" {
			return settings{}, errors.New("DATABASE_URL is required")
		}
		if _, err := pgxpool.ParseConfig(dbURL); err != nil {
			return settings{}, errors.New("invalid DATABASE_URL")
		}
		minimum, err := config.Lookup("DB_POOL_MIN", "1")
		if err != nil {
			return settings{}, err
		}
		maximum, err := config.Lookup("DB_POOL_MAX", "10")
		if err != nil {
			return settings{}, err
		}
		minConns, err := strconv.ParseUint(minimum, 10, 31)
		if err != nil {
			return settings{}, errors.New("invalid DB_POOL_MIN")
		}
		maxConns, err := strconv.ParseUint(maximum, 10, 31)
		if err != nil || maxConns == 0 || maxConns < minConns {
			return settings{}, errors.New("DB_POOL_MAX must be positive and at least DB_POOL_MIN")
		}
		cfg.fence, err = leasefence.FromEnv(common.SiteID)
		if err != nil {
			return settings{}, err
		}
	}
	return cfg, nil
}

func (cfg settings) backend(ctx context.Context) (server.Backend, func(), error) {
	if cfg.PipelineMode == "direct-db" {
		pool, err := dbx.Open(ctx)
		if err != nil {
			return nil, nil, err
		}
		return sinkdb.New(pool, cfg.intake.Gate.MaxPast, cfg.fence), pool.Close, nil
	}
	nc, err := natsx.Connect()
	if err != nil {
		return nil, nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return sinkjs.New(js, nc.IsConnected, cfg.stream, cfg.maxAge, cfg.maxBytes, cfg.quarantineAge, cfg.quarantineBytes), nc.Close, nil
}

func (cfg settings) run(ctx context.Context) error {
	meter, stopTelemetry, err := obs.Setup("ingest", cfg.Common)
	if err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = stopTelemetry(stop)
	}()
	if err := logx.Setup("ingest", cfg.Common); err != nil {
		return err
	}
	sink, closeSink, err := cfg.backend(ctx)
	if err != nil {
		return err
	}
	defer closeSink()
	handler, err := server.New(cfg.intake, sink, meter)
	if err != nil {
		return err
	}
	probe := admin.New(cfg.AdminPort, map[string]func(context.Context) bool{"store": handler.Ready})
	if err := probe.Start(); err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = probe.Shutdown(stop)
	}()

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.HTTPPort))
	if err != nil {
		return err
	}
	httpServer := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	finished := make(chan error, 1)
	go func() { finished <- httpServer.Serve(listener) }()
	maintenance, cancelMaintenance := context.WithCancel(ctx)
	maintained := make(chan struct{})
	go func() {
		defer close(maintained)
		handler.Maintain(maintenance, sink)
	}()
	defer func() {
		cancelMaintenance()
		<-maintained
	}()

	select {
	case err := <-finished:
		return err
	case <-ctx.Done():
		handler.Drain()
		probe.SetDraining()
		drain, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(drain); err != nil {
			return errors.Join(err, httpServer.Close())
		}
		return nil
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
