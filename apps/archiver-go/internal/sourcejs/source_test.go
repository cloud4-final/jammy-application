package sourcejs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fixtureMessage struct {
	jetstream.Msg
	body []byte
}

func (m fixtureMessage) Data() []byte { return m.body }
func (m fixtureMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 1}}, nil
}
func (m fixtureMessage) InProgress() error { return nil }

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
			err := Run(ctx, consumer, test.limit, test.interval, time.Second, func(_ context.Context, messages []jetstream.Msg) error {
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
	err := Run(ctx, consumer, 3, time.Hour, time.Second, func(_ context.Context, messages []jetstream.Msg) error {
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

type poisonMessage struct {
	fixtureMessage
	terminated bool
}

func (m *poisonMessage) Headers() nats.Header {
	headers := nats.Header{}
	headers.Set("Content-Type", "application/x-protobuf")
	headers.Set("Jammy-Ingest-Id", "test")
	headers.Set("Jammy-Received-At-Ms", "0")
	return headers
}

func (m *poisonMessage) Term() error {
	if m.terminated {
		return jetstream.ErrMsgAlreadyAckd
	}
	m.terminated = true
	return nil
}

func TestFlushTerminatesUndecodableBatch(t *testing.T) {
	message := &poisonMessage{fixtureMessage: fixtureMessage{body: []byte{0xff}}}
	poison := 0
	archiver := Archiver{Poison: func(context.Context) { poison++ }}
	for range 2 {
		if err := archiver.Flush(t.Context(), []jetstream.Msg{message}); err != nil {
			t.Fatal(err)
		}
	}
	if !message.terminated || poison != 1 {
		t.Fatalf("terminated=%v poison=%d", message.terminated, poison)
	}
}
