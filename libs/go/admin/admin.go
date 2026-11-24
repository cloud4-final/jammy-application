package admin

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cloud4-final/jammy-5th/libs/go/obs"
)

type Server struct {
	http     *http.Server
	checks   map[string]func(context.Context) bool
	draining atomic.Bool
}

func New(port int, checks map[string]func(context.Context) bool) *Server {
	server := &Server{checks: maps.Clone(checks)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", server.readyz)
	mux.Handle("GET /metrics", obs.MetricsHandler())
	server.http = &http.Server{Addr: ":" + strconv.Itoa(port), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.http.Handler.ServeHTTP(w, r)
}

// Start binds the admin port before returning and serves in the background.
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return err
	}
	go func() {
		if err := s.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("admin server stopped", "error", err)
		}
	}()
	return nil
}

func (s *Server) SetDraining() {
	s.draining.Store(true)
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.SetDraining()
	return s.http.Shutdown(ctx)
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	for _, check := range s.checks {
		if !check(r.Context()) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
	if s.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}
