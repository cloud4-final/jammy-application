package sourcejs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/aggregate"
	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/state"
	"github.com/cloud4-final/jammy-5th/libs/go/codec"
	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

type Committer interface {
	Commit(context.Context, []state.Batch) ([]bool, error)
}

type processError struct {
	failed int
	cause  error
}

func (e processError) Error() string { return e.cause.Error() }
func (e processError) Unwrap() error { return e.cause }

func FailedCount(err error) int {
	var failed processError
	if errors.As(err, &failed) {
		return failed.failed
	}
	return 0
}

func Process(ctx context.Context, store Committer, messages []jetstream.Msg) ([]bool, time.Time, error) {
	batches := make([]state.Batch, 0, len(messages))
	valid := make([]jetstream.Msg, 0, len(messages))
	var receivedAt time.Time
	var rejected error
	for _, message := range messages {
		batch, received, err := decode(message)
		if err != nil {
			rejected = err
			continue
		}
		batches = append(batches, batch)
		valid = append(valid, message)
		receivedAt = received
	}
	if len(valid) == 0 {
		if rejected == nil {
			return nil, time.Time{}, nil
		}
		return nil, time.Time{}, processError{len(messages), rejected}
	}
	duplicates, err := store.Commit(ctx, batches)
	if err != nil {
		return nil, time.Time{}, processError{len(messages), err}
	}
	for _, message := range valid {
		// A failed ack is safe to retry because the DB claim is durable.
		_ = message.Ack()
	}
	if rejected != nil {
		return duplicates, receivedAt, processError{len(messages) - len(valid), rejected}
	}
	return duplicates, receivedAt, nil
}

func decode(message jetstream.Msg) (state.Batch, time.Time, error) {
	headers := message.Headers()
	millis, err := strconv.ParseInt(headers.Get("Jammy-Received-At-Ms"), 10, 64)
	if err != nil {
		return state.Batch{}, time.Time{}, fmt.Errorf("raw message is missing its receive time")
	}
	// Ingest can re-encode JSON and partially valid batches, so the
	// consumer needs headroom above the HTTP wire limit.
	const limit = 64 << 20
	batch, _, err := codec.DecodeBatch(message.Data(), headers.Get("Content-Type"), headers.Get("Content-Encoding"), limit, limit)
	if err != nil || batch.BatchId != headers.Get("Nats-Msg-Id") || batch.BatchId == "" {
		return state.Batch{}, time.Time{}, fmt.Errorf("invalid raw batch")
	}
	events := make([]aggregate.Event, len(batch.Events))
	for i, e := range batch.Events {
		value, err := primaryValue(e)
		if err != nil {
			return state.Batch{}, time.Time{}, err
		}
		events[i] = aggregate.Event{Profile: e.Profile, RoutingKey: e.RoutingKey, Time: time.UnixMilli(e.EventTimeUnixMs).UTC(), Value: value}
	}
	return state.Batch{ID: batch.BatchId, Events: events}, time.UnixMilli(millis).UTC(), nil
}

// primaryValue projects the typed payload already accepted by ingest.
func primaryValue(event *pb.Event) (float64, error) {
	switch event.GetProfile() {
	case "solar":
		var reading pb.SolarReading
		err := proto.Unmarshal(event.Payload, &reading)
		return reading.PowerKw, err
	case "synth-small":
		var reading pb.SynthSmall
		err := proto.Unmarshal(event.Payload, &reading)
		return reading.Value, err
	case "synth-large":
		var reading pb.SynthLarge
		err := proto.Unmarshal(event.Payload, &reading)
		return reading.Value, err
	default:
		return 0, fmt.Errorf("cannot decode payload for unknown profile")
	}
}
