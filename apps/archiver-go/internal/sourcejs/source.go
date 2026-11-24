package sourcejs

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
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
}

func (a *Archiver) Flush(ctx context.Context, messages []jetstream.Msg) error {
	if err := a.initIndex(ctx); err != nil {
		return err
	}
	ingesters := make(map[string]struct{})
	for _, msg := range messages {
		ingest, err := a.write(ctx, msg)
		if err != nil {
			return err
		}
		ingesters[ingest] = struct{}{}
	}
	for ingest := range ingesters {
		a.mu.Lock()
		err := a.index.Flush(ctx, ingest, time.Time{}, a.Lateness, false)
		a.mu.Unlock()
		if err != nil {
			return err
		}
	}
	for _, msg := range messages {
		_ = msg.Ack()
	}
	return nil
}

func (a *Archiver) initIndex(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.index != nil {
		return nil
	}
	index, err := seal.Load(ctx, a.Archive.Objects, func(manifest pq.Manifest) bool { return manifest.Source == "jetstream" })
	if err != nil {
		return err
	}
	a.index = index
	return nil
}

func (a *Archiver) write(ctx context.Context, msg jetstream.Msg) (string, error) {
	meta, err := msg.Metadata()
	if err != nil {
		return "", err
	}
	headers := msg.Headers()
	ts, err := strconv.ParseInt(headers.Get("Jammy-Received-At-Ms"), 10, 64)
	ingest := headers.Get("Jammy-Ingest-Id")
	if err != nil || ingest == "" {
		return "", fmt.Errorf("missing archive routing headers")
	}
	const limit = 64 << 20
	batch, _, err := codec.DecodeBatch(msg.Data(), headers.Get("Content-Type"), headers.Get("Content-Encoding"), limit, limit)
	if err != nil || batch.BatchId != headers.Get("Nats-Msg-Id") {
		return "", fmt.Errorf("invalid raw archive batch")
	}
	received := time.UnixMilli(ts)
	result := gate.Validate(batch, received, gate.Config{MaxPast: time.Duration(1<<63 - 1), MaxFuture: time.Duration(1<<63 - 1)})
	if len(result.Rejected) != 0 {
		return "", fmt.Errorf("invalid raw events")
	}
	groups := make(map[string][]pq.Row)
	seq := meta.Sequence.Stream
	// A message keeps the same sequence range after any partial flush and
	// after an ACK timeout. Replays therefore resolve to the original key.
	for i, e := range result.Accepted {
		key := seal.Prefix(e.Profile, time.UnixMilli(e.EventTimeUnixMs)) + fmt.Sprintf("%s-%d-%d.parquet", ingest, seq, seq)
		groups[key] = append(groups[key], pq.Row{EventID: e.EventId, RoutingKey: e.RoutingKey, Profile: e.Profile,
			SchemaVersion: int32(e.SchemaVersion), EventTime: e.EventTimeUnixMs, ReceivedAt: ts, IngestID: ingest,
			PrimaryValue: result.PrimaryValues[i], Payload: e.Payload})
	}
	for _, rawKey := range slices.Sorted(maps.Keys(groups)) {
		a.mu.Lock()
		key := a.index.Target(rawKey, ingest)
		a.mu.Unlock()
		ref, manifest, err := a.Archive.Write(ctx, key, pq.Rows(groups[rawKey]), pq.Manifest{Source: "jetstream", Stream: &meta.Stream, FirstSeq: &seq, LastSeq: &seq}, false)
		if err != nil {
			return "", err
		}
		a.mu.Lock()
		err = a.index.Record(ref, manifest, ingest)
		a.mu.Unlock()
		if err != nil {
			return "", err
		}
	}
	return ingest, nil
}
