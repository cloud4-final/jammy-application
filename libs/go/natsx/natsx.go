package natsx

import (
	"errors"
	"net/url"
	"os"

	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/nats-io/nats.go"
)

// Connect enables indefinite background reconnect, including after initial
// connection failure. Call IsConnected in a readiness check.
func Connect() (*nats.Conn, error) {
	endpoint, err := config.Lookup("NATS_URL", "")
	if err != nil {
		return nil, err
	}
	if endpoint == "" {
		return nil, errors.New("NATS_URL is required")
	}
	opts := []nats.Option{nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1)}
	if path := os.Getenv("NATS_CREDS_FILE"); path != "" {
		opts = append(opts, nats.UserCredentials(path))
	}
	conn, err := nats.Connect(endpoint, opts...)
	if _, ok := errors.AsType[*url.Error](err); ok || errors.Is(err, nats.ErrMixingWebsocketSchemes) {
		return nil, errors.New("invalid NATS_URL")
	}
	return conn, err
}
