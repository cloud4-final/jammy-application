package sourcejs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/pq"
	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/seal"
	"github.com/cloud4-final/jammy-5th/libs/go/codec"
	"github.com/cloud4-final/jammy-5th/libs/go/gate"
	"github.com/nats-io/nats.go/jetstream"
)

type Archiver struct {
	Archive  pq.Archive
	index    *seal.Index
	mu       sync.Mutex
	Lateness time.Duration
	Poison   func(context.Context)
}

var errBatchID = errors.New("Nats-Msg-Id does not match batch_id")

type delivery struct {
	msg            jetstream.Msg
	stream, ingest string
	seq            uint64
	rows           []pq.Row
}

type group struct {
	prefix, ingest, stream string
	version                int32
}

type item struct {
	sequence uint64
	row      pq.Row
}

func (a *Archiver) Flush(ctx context.Context, messages []jetstream.Msg) error {
	seen := make(map[string]map[uint64]struct{})
	deliveries := make([]delivery, 0, len(messages))
	var invalid error
	for _, msg := range messages {
		d, err := decode(msg)
		if errors.Is(err, errBatchID) {
			invalid = err
			continue
		}
		if err != nil {
			termErr := msg.Term()
			switch {
			case termErr == nil:
				if a.Poison != nil {
					a.Poison(ctx)
				}
			case errors.Is(termErr, jetstream.ErrMsgAlreadyAckd):
				// A flush may retry after terminating this delivery.
			default:
				invalid = errors.Join(err, termErr)
			}
			continue
		}
		if seen[d.stream] == nil {
			seen[d.stream] = make(map[uint64]struct{})
		}
		if _, duplicate := seen[d.stream][d.seq]; duplicate {
			continue
		}
		seen[d.stream][d.seq] = struct{}{}
		deliveries = append(deliveries, d)
	}
	if invalid != nil || len(deliveries) == 0 {
		return invalid
	}
	slices.SortStableFunc(deliveries, func(a, b delivery) int {
		if order := strings.Compare(a.stream, b.stream); order != 0 {
			return order
		}
		return cmp.Compare(a.seq, b.seq)
	})
	if err := a.persist(ctx, deliveries); err != nil {
		return err
	}
	for _, d := range deliveries {
		_ = d.msg.Ack()
	}
	return nil
}

func (a *Archiver) persist(ctx context.Context, deliveries []delivery) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.index == nil {
		index, err := seal.Load(ctx, a.Archive.Objects, func(manifest pq.Manifest) bool { return manifest.Source == "jetstream" })
		if err != nil {
			return err
		}
		a.index = index
	}
	groups := make(map[group][]item)
	var order []group
	ingesters := make(map[string]struct{})
	for _, d := range deliveries {
		ingesters[d.ingest] = struct{}{}
		for _, row := range d.rows {
			key := group{seal.Prefix(row.Profile, time.UnixMilli(row.EventTime)), d.ingest, d.stream, row.SchemaVersion}
			if _, ok := groups[key]; !ok {
				order = append(order, key)
			}
			groups[key] = append(groups[key], item{d.seq, row})
		}
	}
	for _, key := range order {
		items := groups[key]
		uncovered := items[:0]
		for start := 0; start < len(items); {
			seq := items[start].sequence
			end := start + 1
			for end < len(items) && items[end].sequence == seq {
				end++
			}
			covered, err := a.covered(ctx, key, seq)
			if err != nil {
				return err
			}
			if !covered {
				uncovered = append(uncovered, items[start:end]...)
			}
			start = end
		}
		if len(uncovered) > 0 {
			prefix := a.index.TargetPrefix(key.prefix, key.ingest)
			if err := a.write(ctx, prefix, key, uncovered); err != nil {
				return err
			}
		}
	}
	for _, ingest := range slices.Sorted(maps.Keys(ingesters)) {
		if err := a.index.Flush(ctx, ingest, time.Time{}, a.Lateness, false); err != nil {
			return err
		}
	}
	return nil
}

func decode(msg jetstream.Msg) (delivery, error) {
	meta, err := msg.Metadata()
	if err != nil {
		return delivery{}, err
	}
	headers := msg.Headers()
	ts, err := strconv.ParseInt(headers.Get("Jammy-Received-At-Ms"), 10, 64)
	ingest := headers.Get("Jammy-Ingest-Id")
	if err != nil || ingest == "" {
		return delivery{}, fmt.Errorf("missing archive routing headers")
	}
	const limit = 64 << 20
	batch, _, err := codec.DecodeBatch(msg.Data(), headers.Get("Content-Type"), headers.Get("Content-Encoding"), limit, limit)
	if err != nil {
		return delivery{}, err
	}
	if batch.BatchId != headers.Get("Nats-Msg-Id") {
		return delivery{}, errBatchID
	}
	d := delivery{msg: msg, stream: meta.Stream, ingest: ingest, seq: meta.Sequence.Stream, rows: make([]pq.Row, 0, len(batch.Events))}
	for _, e := range batch.Events {
		value, err := gate.ProjectPrimaryValue(e)
		if err != nil {
			return delivery{}, err
		}
		d.rows = append(d.rows, pq.Row{EventID: e.EventId, RoutingKey: e.RoutingKey, Profile: e.Profile,
			SchemaVersion: int32(e.SchemaVersion), EventTime: e.EventTimeUnixMs, ReceivedAt: ts, IngestID: ingest,
			PrimaryValue: value, Payload: e.Payload})
	}
	return d, nil
}
