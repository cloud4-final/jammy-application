// Plantsim handles the dispatcher command contract for local experiments.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cloud4-final/jammy-5th/libs/go/admin"
	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/cloud4-final/jammy-5th/libs/go/lifecycle"
	"github.com/cloud4-final/jammy-5th/libs/go/logx"
	"github.com/cloud4-final/jammy-5th/libs/go/obs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type command struct {
	CommandID     string    `json:"command_id"`
	InstructionID string    `json:"instruction_id"`
	AssetID       string    `json:"asset_id"`
	ReductionKW   string    `json:"reduction_kw"`
	Generation    *int64    `json:"generation"`
	Epoch         *int64    `json:"epoch"`
	ValidUntil    time.Time `json:"valid_until"`
}

type outcome struct {
	code   int
	status string
}

type simulator struct {
	mu        sync.Mutex
	maxEpoch  map[string]int64
	outcomes  map[string]outcome
	delay     time.Duration
	failRatio float64
	commands  metric.Int64Counter
	duration  metric.Float64Histogram
}

const service = "plantsim"

func newSimulator(delay time.Duration, failRatio float64, meter metric.Meter) (*simulator, error) {
	commands, err := meter.Int64Counter("jammy_plantsim_commands", metric.WithDescription("Command outcomes."))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("http.server.request.duration", metric.WithUnit("s"),
		metric.WithDescription("HTTP request duration in seconds."))
	if err != nil {
		return nil, err
	}
	for _, status := range []string{"acked", "stale_epoch", "error"} {
		commands.Add(context.Background(), 0, metric.WithAttributes(attribute.String("result", status)))
	}
	return &simulator{
		maxEpoch: make(map[string]int64), outcomes: make(map[string]outcome),
		delay: delay, failRatio: failRatio, commands: commands, duration: duration,
	}, nil
}

func (s *simulator) command(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	respond := func(code int, status string) {
		writeStatus(w, code, status)
		s.duration.Record(r.Context(), time.Since(start).Seconds(), metric.WithAttributes(
			attribute.String("http.request.method", http.MethodPost),
			attribute.String("http.route", "/v1/commands"),
			attribute.Int("http.response.status_code", code),
		))
	}
	var c command
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	if err := decoder.Decode(&c); err != nil || decoder.Decode(new(any)) != io.EOF ||
		c.CommandID == "" || c.InstructionID == "" || c.AssetID == "" || c.ReductionKW == "" ||
		c.Generation == nil || c.Epoch == nil || *c.Epoch < 0 || c.ValidUntil.IsZero() {
		respond(http.StatusBadRequest, "invalid_body")
		return
	}
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}

	s.mu.Lock()
	if r.Context().Err() != nil {
		s.mu.Unlock()
		return
	}
	result, found := s.outcomes[c.CommandID]
	if !found {
		switch {
		case *c.Epoch < s.maxEpoch[c.AssetID]:
			result = outcome{http.StatusConflict, "stale_epoch"}
		case s.failRatio > 0 && rand.Float64() < s.failRatio:
			result = outcome{http.StatusServiceUnavailable, "error"}
		default:
			result = outcome{http.StatusOK, "acked"}
			s.maxEpoch[c.AssetID] = *c.Epoch
		}
		s.outcomes[c.CommandID] = result
	}
	s.mu.Unlock()
	s.commands.Add(r.Context(), 1, metric.WithAttributes(attribute.String("result", result.status)))
	respond(result.code, result.status)
}

func writeStatus(w http.ResponseWriter, code int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}

type settings struct {
	config.Common
	delay     time.Duration
	failRatio float64
}

func load() (settings, error) {
	common, err := config.LoadCommon()
	if err != nil {
		return settings{}, err
	}
	rawDelay, err := config.Lookup("PLANTSIM_DELAY_MS", "0")
	if err != nil {
		return settings{}, err
	}
	delay, err := strconv.ParseUint(rawDelay, 10, 64)
	if err != nil || delay > math.MaxInt64/uint64(time.Millisecond) {
		return settings{}, errors.New("PLANTSIM_DELAY_MS must be a nonnegative millisecond duration")
	}
	rawRatio, err := config.Lookup("PLANTSIM_FAIL_RATIO", "0")
	if err != nil {
		return settings{}, err
	}
	failRatio, err := strconv.ParseFloat(rawRatio, 64)
	if err != nil || math.IsNaN(failRatio) || failRatio < 0 || failRatio > 1 {
		return settings{}, errors.New("PLANTSIM_FAIL_RATIO must be a number in [0, 1]")
	}
	return settings{Common: common, delay: time.Duration(delay) * time.Millisecond, failRatio: failRatio}, nil
}

func main() {
	cfg, err := load()
	if err != nil {
		logger, _ := logx.New(os.Stdout, service, config.Common{LogLevel: "info"})
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	if err := logx.Setup(service, cfg.Common); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	lifecycle.Run(cfg.run, cfg.ShutdownTimeout)
}

func (cfg settings) run(ctx context.Context) error {
	meter, stopTelemetry, err := obs.Setup(service, cfg.Common)
	if err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = stopTelemetry(stop)
	}()

	sim, err := newSimulator(cfg.delay, cfg.failRatio, meter)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/commands", sim.command)
	public := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.HTTPPort))
	if err != nil {
		return err
	}
	probe := admin.New(cfg.AdminPort, nil)
	if err := probe.Start(); err != nil {
		return errors.Join(err, listener.Close())
	}

	finished := make(chan error, 1)
	go func() { finished <- public.Serve(listener) }()
	slog.Info("plantsim listening", "http_port", cfg.HTTPPort, "admin_port", cfg.AdminPort)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-finished:
	}
	probe.SetDraining()
	drain, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return errors.Join(serveErr, public.Shutdown(drain), probe.Shutdown(drain))
}
