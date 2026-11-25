package sourcejs

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	fetchWait = time.Second
	idleWait  = 200 * time.Millisecond
	RetryWait = time.Second
)

// Run owns a stream's pull and flush cycle. A failed flush keeps its original
// batch so a retry uses the same sequence bounds and object keys.
func Run(ctx context.Context, consumer jetstream.Consumer, flushBytes int64, interval, ackWait time.Duration,
	flush func(context.Context, []jetstream.Msg) error, unacked func(int)) error {
	var pending []jetstream.Msg
	positions := make(map[uint64]int)
	var size int64
	var deadline time.Time
	drain := context.WithoutCancel(ctx)
	write := func() error {
		if len(pending) == 0 {
			return nil
		}
		progressCtx, finish := context.WithCancel(drain)
		defer finish()
		go keepAckDeadline(progressCtx, pending, ackWait)
		for {
			err := flush(drain, pending)
			if err == nil {
				pending = nil
				clear(positions)
				size = 0
				deadline = time.Time{}
				unacked(0)
				return nil
			}
			if !pause(ctx, RetryWait) {
				return err
			}
		}
	}
	for ctx.Err() == nil {
		wait := fetchWait
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				if err := write(); err != nil {
					return err
				}
				continue
			}
			wait = min(wait, remaining)
		}
		batch, err := consumer.Fetch(64, jetstream.FetchMaxWait(wait))
		if err != nil {
			return errors.Join(err, write())
		}
		count := 0
		for message := range batch.Messages() {
			count++
			if meta, err := message.Metadata(); err == nil {
				sequence := meta.Sequence.Stream
				if position, exists := positions[sequence]; exists {
					pending[position] = message
					continue
				}
				positions[sequence] = len(pending)
			}
			if len(pending) == 0 {
				deadline = time.Now().Add(interval)
			}
			size += int64(len(message.Data()))
			pending = append(pending, message)
			unacked(len(pending))
			if size >= flushBytes || !time.Now().Before(deadline) {
				if err := write(); err != nil {
					return err
				}
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
			return errors.Join(err, write())
		}
		if count == 0 {
			wait := idleWait
			if !deadline.IsZero() {
				wait = min(wait, time.Until(deadline))
			}
			pause(ctx, wait)
		}
	}
	return write()
}

func keepAckDeadline(ctx context.Context, messages []jetstream.Msg, ackWait time.Duration) {
	ticker := time.NewTicker(max(250*time.Millisecond, ackWait/2))
	defer ticker.Stop()
	for {
		for _, message := range messages {
			_ = message.InProgress()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
