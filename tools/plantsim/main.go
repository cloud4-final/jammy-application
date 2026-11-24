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
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	commands  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	metrics   http.Handler
}

func newSimulator(delay time.Duration, failRatio float64, site, instance string) *simulator {
	labels := prometheus.Labels{"service": "plantsim", "site": site, "instance": instance}
	commands := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "jammy_plantsim_commands_total", Help: "Command outcomes.", ConstLabels: labels,
	}, []string{"result"})
	for _, status := range []string{"acked", "stale_epoch", "error"} {
		commands.WithLabelValues(status)
	}
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_server_request_duration_seconds", Help: "HTTP request duration in seconds.", ConstLabels: labels,
		Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"http_request_method", "http_route", "http_response_status_code"})
	registry := prometheus.NewRegistry()
	registry.MustRegister(commands, duration)
	return &simulator{
		maxEpoch: make(map[string]int64), outcomes: make(map[string]outcome),
		delay: delay, failRatio: failRatio, commands: commands, duration: duration,
		metrics: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}
}

func (s *simulator) command(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	respond := func(code int, status string) {
		writeStatus(w, code, status)
		s.duration.WithLabelValues(http.MethodPost, "/v1/commands", strconv.Itoa(code)).Observe(time.Since(start).Seconds())
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
	s.commands.WithLabelValues(result.status).Inc()
	respond(result.code, result.status)
}

func writeStatus(w http.ResponseWriter, code int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}

func main() {
	site := os.Getenv("SITE_ID")
	host, _ := os.Hostname()
	instance := env("INSTANCE_ID", host)
	var level slog.Level
	levelErr := level.UnmarshalText([]byte(env("LOG_LEVEL", "info")))
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			switch attr.Key {
			case slog.TimeKey:
				attr.Key = "ts"
			case slog.LevelKey:
				attr.Value = slog.StringValue(strings.ToLower(attr.Value.String()))
			}
			return attr
		},
	})).With("service", "plantsim", "site", site, "instance", instance, "trace_id", "", "span_id", "")
	if levelErr != nil {
		logger.Error("invalid LOG_LEVEL")
		os.Exit(1)
	}
	if err := run(site, instance, logger); err != nil {
		logger.Error("plantsim stopped", "error", err)
		os.Exit(1)
	}
}

func run(site, instance string, logger *slog.Logger) error {
	if site == "" {
		return errors.New("SITE_ID is required")
	}
	httpPort, err := integerEnv("HTTP_PORT", "8080", 1, 65535)
	if err != nil {
		return err
	}
	adminPort, err := integerEnv("ADMIN_PORT", "9090", 1, 65535)
	if err != nil {
		return err
	}
	shutdownSeconds, err := integerEnv("SHUTDOWN_TIMEOUT_SECONDS", "25", 0, math.MaxInt64/int64(time.Second))
	if err != nil {
		return err
	}
	delayMS, err := integerEnv("PLANTSIM_DELAY_MS", "0", 0, math.MaxInt64/int64(time.Millisecond))
	if err != nil {
		return err
	}
	failRatio, err := strconv.ParseFloat(env("PLANTSIM_FAIL_RATIO", "0"), 64)
	if err != nil || math.IsNaN(failRatio) || failRatio < 0 || failRatio > 1 {
		return errors.New("PLANTSIM_FAIL_RATIO must be a number in [0, 1]")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	sim := newSimulator(time.Duration(delayMS)*time.Millisecond, failRatio, site, instance)
	publicMux := http.NewServeMux()
	publicMux.HandleFunc("POST /v1/commands", sim.command)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	adminMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ctx.Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	adminMux.Handle("GET /metrics", sim.metrics)

	public := &http.Server{Handler: publicMux, ReadHeaderTimeout: 5 * time.Second}
	admin := &http.Server{Handler: adminMux, ReadHeaderTimeout: 5 * time.Second}
	publicListener, err := net.Listen("tcp", fmt.Sprintf(":%d", httpPort))
	if err != nil {
		return err
	}
	defer func() {
		if err := publicListener.Close(); err != nil {
			logger.Debug("closing public listener", "error", err)
		}
	}()
	adminListener, err := net.Listen("tcp", fmt.Sprintf(":%d", adminPort))
	if err != nil {
		return err
	}
	defer func() {
		if err := adminListener.Close(); err != nil {
			logger.Debug("closing admin listener", "error", err)
		}
	}()

	serverErrors := make(chan error, 2)
	go func() { serverErrors <- public.Serve(publicListener) }()
	go func() { serverErrors <- admin.Serve(adminListener) }()
	logger.Info("plantsim listening", "http_port", httpPort, "admin_port", adminPort)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serverErrors:
		stop()
	}
	drain, cancel := context.WithTimeout(context.Background(), time.Duration(shutdownSeconds)*time.Second)
	defer cancel()
	return errors.Join(serveErr, public.Shutdown(drain), admin.Shutdown(drain))
}

func integerEnv(name, fallback string, low, high int64) (int64, error) {
	raw := env(name, fallback)
	if strings.IndexFunc(raw, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, fmt.Errorf("%s must contain decimal digits", name)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < low || value > high {
		return 0, fmt.Errorf("%s must be in [%d, %d]", name, low, high)
	}
	return value, nil
}

func env(name, fallback string) string {
	if v, found := os.LookupEnv(name); found {
		return v
	}
	return fallback
}
