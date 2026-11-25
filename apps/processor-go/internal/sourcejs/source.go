package sourcejs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cloud4-final/jammy-application/apps/processor-go/internal/aggregate"
	"github.com/cloud4-final/jammy-application/apps/processor-go/internal/state"
	"github.com/cloud4-final/jammy-application/libs/go/codec"
	"github.com/cloud4-final/jammy-application/libs/go/gate"
	"github.com/nats-io/nats.go/jetstream"
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

const nakDelay = time.Second

var errBatchID = errors.New("Nats-Msg-Id does not match batch_id")

func Process(ctx context.Context, store Committer, messages []jetstream.Msg, recordPoison func()) ([]bool, time.Time, error) {
	batches := make([]state.Batch, 0, len(messages))
	valid := make([]jetstream.Msg, 0, len(messages))
	var receivedAt time.Time
	var rejected error
	var rejectedCount int
	for _, message := range messages {
		batch, received, err := decode(message)
		if err != nil {
			termErr := message.Term()
			switch {
			case termErr == nil:
				recordPoison()
				continue
			case errors.Is(termErr, jetstream.ErrMsgAlreadyAckd):
				continue
			default:
				err = errors.Join(err, termErr)
			}
			rejected = err
			rejectedCount++
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
		return nil, time.Time{}, processError{rejectedCount, rejected}
	}
	duplicates, err := store.Commit(ctx, batches)
	if err != nil {
		for _, message := range valid {
			_ = message.NakWithDelay(nakDelay)
		}
		return nil, time.Time{}, processError{len(valid) + rejectedCount, err}
	}
	for _, message := range valid {
		// A failed ack is safe to retry because the DB claim is durable.
		_ = message.Ack()
	}
	if rejected != nil {
		return duplicates, receivedAt, processError{rejectedCount, rejected}
	}
	return duplicates, receivedAt, nil
}

func decode(message jetstream.Msg) (state.Batch, time.Time, error) {
	headers := message.Headers()
	// Ingest can re-encode JSON and partially valid batches, so the
	// consumer needs headroom above the HTTP wire limit.
	const limit = 64 << 20
	batch, _, err := codec.DecodeBatch(message.Data(), headers.Get("Content-Type"), headers.Get("Content-Encoding"), limit, limit)
	if err != nil {
		return state.Batch{}, time.Time{}, err
	}
	if batch.BatchId != headers.Get("Nats-Msg-Id") {
		return state.Batch{}, time.Time{}, errBatchID
	}
	millis, err := strconv.ParseInt(headers.Get("Jammy-Received-At-Ms"), 10, 64)
	if err != nil {
		return state.Batch{}, time.Time{}, fmt.Errorf("raw message is missing its receive time")
	}
	events := make([]aggregate.Event, len(batch.Events))
	for i, e := range batch.Events {
		value, err := gate.ProjectPrimaryValue(e)
		if err != nil {
			return state.Batch{}, time.Time{}, err
		}
		events[i] = aggregate.Event{Profile: e.Profile, RoutingKey: e.RoutingKey, Time: time.UnixMilli(e.EventTimeUnixMs).UTC(), Value: value}
	}
	return state.Batch{ID: batch.BatchId, Events: events}, time.UnixMilli(millis).UTC(), nil
}
