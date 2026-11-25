package natsx

import (
	"testing"
	"time"
)

func TestConnectReturnsWhileServerUnavailable(t *testing.T) {
	t.Setenv("NATS_URL", "nats://127.0.0.1:1")
	t.Setenv("NATS_URL_FILE", "")
	t.Setenv("NATS_CREDS_FILE", "")
	conn, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.IsConnected() || !conn.IsReconnecting() {
		t.Fatalf("initial NATS status = %s", conn.Status())
	}
	if conn.Opts.ReconnectWait != time.Second {
		t.Fatalf("ReconnectWait = %v, want 1s", conn.Opts.ReconnectWait)
	}
	if conn.Opts.ReconnectJitter != 0 || conn.Opts.ReconnectJitterTLS != 0 {
		t.Fatalf("reconnect jitter = %v (plain), %v (TLS), want zero", conn.Opts.ReconnectJitter, conn.Opts.ReconnectJitterTLS)
	}
}

func TestConnectRejectsInvalidEndpointAtStartup(t *testing.T) {
	t.Setenv("NATS_URL_FILE", "")
	t.Setenv("NATS_CREDS_FILE", "")
	for _, tc := range []struct{ name, endpoint string }{
		{"wrong scheme", "http://example.test:4222"},
		{"missing host", "nats://"},
		{"zero port", "nats://example.test:0"},
		{"port overflow", "nats://example.test:65536"},
		{"empty list element", "nats://example.test,"},
		{"mixed transports", "nats://example.test,ws://example.test"},
		{"invalid port with credentials", "nats://user:password@localhost:invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NATS_URL", tc.endpoint)
			conn, err := Connect()
			if conn != nil {
				conn.Close()
			}
			if err == nil || err.Error() != "invalid NATS_URL" {
				t.Fatalf("Connect() error = %v, want invalid NATS_URL", err)
			}
		})
	}
}
