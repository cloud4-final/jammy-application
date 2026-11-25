package natsx

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cloud4-final/jammy-application/libs/go/config"
	"github.com/nats-io/nats.go"
)

// Connect enables indefinite background reconnect, including after initial
// connection failure. Readiness checks should verify the required streams.
func Connect() (*nats.Conn, error) {
	endpoint, err := config.Lookup("NATS_URL", "")
	if err != nil {
		return nil, err
	}
	if endpoint == "" {
		return nil, errors.New("NATS_URL is required")
	}
	if !validEndpoints(endpoint) {
		return nil, errors.New("invalid NATS_URL")
	}
	opts := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.ReconnectJitter(0, 0),
	}
	if path := os.Getenv("NATS_CREDS_FILE"); path != "" {
		opts = append(opts, nats.UserCredentials(path))
	}
	conn, err := nats.Connect(endpoint, opts...)
	if _, ok := errors.AsType[*url.Error](err); ok || errors.Is(err, nats.ErrMixingWebsocketSchemes) {
		return nil, errors.New("invalid NATS_URL")
	}
	return conn, err
}

func validEndpoints(endpoint string) bool {
	var tcp, websocket bool
	for raw := range strings.SplitSeq(endpoint, ",") {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || parsed.Hostname() == "" {
			return false
		}
		switch parsed.Scheme {
		case "nats", "tls":
			tcp = true
		case "ws", "wss":
			websocket = true
		default:
			return false
		}
		if port := parsed.Port(); port != "" {
			value, err := strconv.ParseUint(port, 10, 16)
			if err != nil || value == 0 {
				return false
			}
		}
	}
	return !tcp || !websocket
}
