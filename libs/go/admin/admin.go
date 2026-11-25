package admin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cloud4-final/jammy-application/libs/go/obs"
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
	checks := make(map[string]string, len(s.checks))
	if !s.draining.Load() {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		healthy := make(chan string, len(s.checks))
		done := make(chan struct{}, len(s.checks))
		for name, check := range s.checks {
			checks[name] = "fail"
			go func() {
				if check(ctx) && ctx.Err() == nil {
					healthy <- name
				}
				done <- struct{}{}
			}()
		}
	wait:
		for range s.checks {
			select {
			case <-done:
			case <-ctx.Done():
				break wait
			}
		}
		for range len(healthy) {
			checks[<-healthy] = "ok"
		}
	}

	status, code := "ok", http.StatusOK
	for _, result := range checks {
		if result == "fail" {
			status, code = "fail", http.StatusServiceUnavailable
			break
		}
	}
	if s.draining.Load() {
		status, code = "fail", http.StatusServiceUnavailable
		clear(checks)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}{status, checks})
}
