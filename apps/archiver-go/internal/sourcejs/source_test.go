package sourcejs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type fixtureMessage struct {
	jetstream.Msg
	body []byte
}

func (m fixtureMessage) Data() []byte { return m.body }

type fixtureConsumer struct {
	jetstream.Consumer
	message  jetstream.Msg
	requests int
}

func (f *fixtureConsumer) Fetch(_ int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	f.requests++
	messages := make(chan jetstream.Msg, 1)
	if f.requests == 1 {
		messages <- f.message
	}
	close(messages)
	return fixtureBatch{messages}, nil
}

type fixtureBatch struct{ messages chan jetstream.Msg }

func (f fixtureBatch) Messages() <-chan jetstream.Msg { return f.messages }
func (f fixtureBatch) Error() error                   { return nil }

func TestRunFlushesAtByteOrTimeThreshold(t *testing.T) {
	for _, test := range []struct {
		name     string
		limit    int64
		interval time.Duration
		minimum  time.Duration
	}{
		{"bytes", 3, time.Hour, 0},
		{"time", 64, 20 * time.Millisecond, 20 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			consumer := &fixtureConsumer{message: fixtureMessage{body: []byte("abc")}}
			started := time.Now()
			flushed := false
			err := Run(ctx, consumer, test.limit, test.interval, func(_ context.Context, messages []jetstream.Msg) error {
				if len(messages) != 1 || time.Since(started) < test.minimum {
					t.Fatalf("unexpected flush of %d messages", len(messages))
				}
				flushed = true
				cancel()
				return nil
			}, func(int) {})
			if err != nil || !flushed {
				t.Fatalf("flush=%v error=%v", flushed, err)
			}
			if test.name == "bytes" && consumer.requests != 1 {
				t.Fatal("byte flush waited for another pull")
			}
		})
	}
}

func TestRunRetriesFailedFlushBeforeFetching(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	consumer := &fixtureConsumer{message: fixtureMessage{body: []byte("abc")}}
	var failures int
	var first time.Time
	err := Run(ctx, consumer, 3, time.Hour, func(_ context.Context, messages []jetstream.Msg) error {
		if len(messages) != 1 || consumer.requests != 1 {
			t.Fatalf("retry lost the original batch or fetched more")
		}
		if failures == 0 {
			failures++
			first = time.Now()
			return errors.New("failed upload")
		}
		if time.Since(first) < time.Second {
			t.Fatal("retry did not back off for one second")
		}
		cancel()
		return nil
	}, func(int) {})
	if err != nil || ctx.Err() != context.Canceled {
		t.Fatalf("unexpected retry result: %v, %v", err, ctx.Err())
	}
}
