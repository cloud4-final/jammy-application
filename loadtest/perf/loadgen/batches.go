package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cloud4-final/jammy-5th/tools/payloadgen/gen"
)

var profiles = [...]string{"solar", "synth-small", "synth-large"}

type batch struct {
	gen.IndexEntry
	wire []byte
}

// The producer owns insertion and the scheduler owns removal. The limit
// covers queued compressed bodies. Each encoder also owns its scratch space,
// and requests own their bodies until their responses have been read.
type batchRing struct {
	queue    chan batch
	released chan struct{}
	bytes    atomic.Int64
	limit    int64
	peak     int64 // written only by the producer
}

func newBatchRing(limit int64) *batchRing {
	return &batchRing{queue: make(chan batch, 256), released: make(chan struct{}, 1), limit: limit}
}

func (r *batchRing) push(ctx context.Context, entry gen.IndexEntry, wire []byte, onFull func()) error {
	size := int64(len(wire))
	if size > r.limit {
		return fmt.Errorf("batch %s is %d bytes, exceeds the target's %d-byte ring", entry.BatchID, size, r.limit)
	}
	for r.bytes.Load()+size > r.limit || len(r.queue) == cap(r.queue) {
		onFull()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.released:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	body := make([]byte, len(wire))
	copy(body, wire)
	r.peak = max(r.peak, r.bytes.Add(size))
	r.queue <- batch{IndexEntry: entry, wire: body}
	return nil
}

func (r *batchRing) pop() (batch, bool) {
	select {
	case item := <-r.queue:
		r.bytes.Add(-int64(len(item.wire)))
		select {
		case r.released <- struct{}{}:
		default:
		}
		return item, true
	default:
		return batch{}, false
	}
}

type target struct {
	url      string
	ring     *batchRing
	interval time.Duration
	ready    chan struct{}
}

func (t *target) produce(ctx context.Context, opts gen.Options, namespace string, bytesPerSecond float64) error {
	var encoders []*gen.BatchEncoder
	var scratch []byte
	var cycleBytes int
	ready := func() {
		select {
		case <-t.ready:
		default:
			close(t.ready)
		}
	}
	prefill := min(float64(t.ring.limit)/2, bytesPerSecond)
	for _, profile := range profiles {
		opts.Profile = profile
		encoder, err := gen.NewBatchEncoder(opts, namespace)
		if err != nil {
			return err
		}
		defer func() { _ = encoder.Close() }()
		encoders = append(encoders, encoder)
		entry, wire, err := encoder.Next(scratch[:0])
		if err != nil {
			return err
		}
		scratch = wire
		cycleBytes += entry.WireBytes
		if int64(cycleBytes) > t.ring.limit {
			return fmt.Errorf("the target's %d-byte ring cannot hold a complete profile cycle", t.ring.limit)
		}
		if len(encoders) == len(profiles) {
			interval := float64(time.Second) * float64(cycleBytes) / float64(len(profiles)) / bytesPerSecond
			if interval < 1 || interval >= float64(time.Duration(1<<63-1)) {
				return fmt.Errorf("wire rate is outside the supported scheduling range")
			}
			t.interval = time.Duration(interval)
		}
		if err = t.ring.push(ctx, entry, wire, func() {}); err != nil {
			return err
		}
	}
	if float64(t.ring.bytes.Load()) >= prefill {
		ready()
	}
	for i := 0; ctx.Err() == nil; i = (i + 1) % len(encoders) {
		entry, wire, err := encoders[i].Next(scratch[:0])
		if err != nil {
			return err
		}
		scratch = wire
		if err = t.ring.push(ctx, entry, wire, ready); err != nil {
			return err
		}
		if float64(t.ring.bytes.Load()) >= prefill {
			ready()
		}
	}
	return ctx.Err()
}
