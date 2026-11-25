package sourcejs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
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

type headerMessage struct {
	fixtureMessage
	headers nats.Header
}

func (m headerMessage) Headers() nats.Header { return m.headers }

func TestFlushTraceContextExtractsTraceparentAndLinks(t *testing.T) {
	first := headerMessage{headers: nats.Header{
		"Traceparent": {"00-00000000000000000000000000000042-0000000000000023-01"},
	}}
	second := headerMessage{headers: nats.Header{
		"traceparent": {"00-00000000000000000000000000000043-0000000000000024-01"},
	}}
	missing := headerMessage{}

	parent, links := FlushTraceContext(t.Context(), []jetstream.Msg{missing, first, first, second})
	sc := trace.SpanContextFromContext(parent)
	wantTraceID, err := trace.TraceIDFromHex("00000000000000000000000000000042")
	if err != nil {
		t.Fatal(err)
	}
	wantSpanID, err := trace.SpanIDFromHex("0000000000000023")
	if err != nil {
		t.Fatal(err)
	}
	if !sc.IsValid() || sc.TraceID() != wantTraceID || sc.SpanID() != wantSpanID {
		t.Fatalf("got parent span context %+v", sc)
	}
	if len(links) != 2 {
		t.Fatalf("got %d links, want 2 deduplicated links", len(links))
	}

	emptyParent, emptyLinks := FlushTraceContext(t.Context(), []jetstream.Msg{missing})
	if trace.SpanContextFromContext(emptyParent).IsValid() || len(emptyLinks) != 0 {
		t.Fatalf("expected invalid span context and no links when traceparent is missing")
	}
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
