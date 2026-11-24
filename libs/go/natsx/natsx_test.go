package natsx

import "testing"

func TestConnectReturnsWhileServerUnavailable(t *testing.T) {
	t.Setenv("NATS_URL", "nats://127.0.0.1:1")
	t.Setenv("NATS_CREDS_FILE", "")
	conn, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.IsConnected() || !conn.IsReconnecting() {
		t.Fatalf("initial NATS status = %s", conn.Status())
	}
}
