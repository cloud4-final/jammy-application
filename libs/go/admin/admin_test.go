package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReadyzTracksDependency(t *testing.T) {
	ready := false
	server := New(9090, map[string]func(context.Context) bool{
		"db": func(context.Context) bool { return ready },
		"s3": func(context.Context) bool { return true },
	})
	checkReady(t, server, 503, `{"status":"fail","checks":{"db":"fail","s3":"ok"}}`)
	ready = true
	checkReady(t, server, 200, `{"status":"ok","checks":{"db":"ok","s3":"ok"}}`)
	server.SetDraining()
	checkReady(t, server, 503, `{"status":"fail","checks":{}}`)
}

func TestReadyzTimesOut(t *testing.T) {
	server := New(9090, map[string]func(context.Context) bool{
		"slow": func(ctx context.Context) bool {
			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Second):
			}
			return true
		},
		"fast": func(context.Context) bool { return true },
	})
	start := time.Now()
	checkReady(t, server, 503, `{"status":"fail","checks":{"fast":"ok","slow":"fail"}}`)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("readiness took %v", elapsed)
	}
}

func checkReady(t *testing.T, server http.Handler, code int, body string) {
	t.Helper()
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest("GET", "/readyz", nil))
	if response.Code != code || response.Header().Get("Content-Type") != "application/json" ||
		response.Body.String() != body+"\n" {
		t.Fatalf("readyz = %d %q %s", response.Code, response.Header().Get("Content-Type"), response.Body)
	}
}

func TestLivezAlways200(t *testing.T) {
	server := New(9090, map[string]func(context.Context) bool{"db": func(context.Context) bool { return false }})
	checkStatus(t, server, "/livez", 200)
	server.SetDraining()
	checkStatus(t, server, "/livez", 200)
}

func checkStatus(t *testing.T, server http.Handler, path string, want int) {
	t.Helper()
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
	if response.Code != want {
		t.Fatalf("%s status = %d, want %d", path, response.Code, want)
	}
}
