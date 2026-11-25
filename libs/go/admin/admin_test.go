package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadyzTracksDependency(t *testing.T) {
	ready := false
	server := New(9090, map[string]func(context.Context) bool{"db": func(context.Context) bool { return ready }})
	checkStatus(t, server, "/readyz", 503)
	ready = true
	checkStatus(t, server, "/readyz", 200)
	server.SetDraining()
	checkStatus(t, server, "/readyz", 503)
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
