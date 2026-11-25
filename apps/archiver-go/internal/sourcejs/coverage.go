package sourcejs

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"

	"github.com/cloud4-final/jammy-5th/apps/archiver-go/internal/pq"
)

func (a *Archiver) covered(ctx context.Context, key group, seq uint64) (bool, error) {
	for _, prefix := range []string{key.prefix, "raw-late/" + strings.TrimPrefix(key.prefix, "raw/")} {
		for object, m := range a.index.Manifests[prefix] {
			if m.Stream != nil && *m.Stream == key.stream && m.SchemaVersion == key.version && contains(m, seq) {
				if _, _, ok := bounds(object, prefix, key.ingest); ok {
					return true, nil
				}
			}
		}
		for object := range a.index.Orphans[prefix] {
			first, last, ok := bounds(object, prefix, key.ingest)
			if !ok || seq < first || seq > last {
				continue
			}
			m, err := a.Archive.Inspect(ctx, object)
			if err != nil {
				return false, err
			}
			if m.Stream == nil || *m.Stream != key.stream || len(m.SequenceRanges) == 0 {
				continue
			}
			m.FirstSeq, m.LastSeq = &first, &last
			if _, err := pq.PutJSON(ctx, a.Archive.Objects, pq.ManifestKey(object), &m, nil); err != nil {
				return false, err
			}
			ref := pq.Ref{Key: object, Rows: m.Rows, SHA256: m.SHA256}
			if err := a.index.Record(ref, m, key.ingest); err != nil {
				return false, err
			}
			if m.SchemaVersion == key.version && contains(m, seq) {
				return true, nil
			}
		}
	}
	return false, nil
}

func contains(m pq.Manifest, sequence uint64) bool {
	if m.FirstSeq == nil || m.LastSeq == nil || sequence < *m.FirstSeq || sequence > *m.LastSeq {
		return false
	}
	if len(m.SequenceRanges) == 0 {
		// Older manifests only prove coverage if both bounds are identical.
		return *m.FirstSeq == *m.LastSeq
	}
	_, found := slices.BinarySearchFunc(m.SequenceRanges, sequence, func(r [2]uint64, seq uint64) int {
		switch {
		case r[1] < seq:
			return -1
		case r[0] > seq:
			return 1
		default:
			return 0
		}
	})
	return found
}

func bounds(object, prefix, ingest string) (uint64, uint64, bool) {
	name, ok := strings.CutPrefix(object, prefix+ingest+"-")
	if !ok {
		return 0, 0, false
	}
	first, last, ok := strings.Cut(strings.TrimSuffix(name, ".parquet"), "-")
	if !ok {
		return 0, 0, false
	}
	low, a := strconv.ParseUint(first, 10, 64)
	high, b := strconv.ParseUint(last, 10, 64)
	return low, high, a == nil && b == nil
}

func (a *Archiver) write(ctx context.Context, prefix string, key group, items []item) error {
	first, last := items[0].sequence, items[len(items)-1].sequence
	object := fmt.Sprintf("%s%s-%d-%d.parquet", prefix, key.ingest, first, last)
	_, known := a.index.Manifests[prefix][object]
	_, orphan := a.index.Orphans[prefix][object]
	if known || orphan {
		// Preserve a legacy file whose bounds do not establish coverage. Split
		// at a sequence boundary to keep every batch intact.
		if first == last {
			return fmt.Errorf("cannot verify coverage for existing archive object %s", object)
		}
		pivot := items[len(items)/2].sequence
		split := 0
		for split < len(items) && (items[split].sequence < pivot || items[split].sequence == first) {
			split++
		}
		if err := a.write(ctx, prefix, key, items[:split]); err != nil {
			return err
		}
		return a.write(ctx, prefix, key, items[split:])
	}
	var ranges [][2]uint64
	for _, entry := range items {
		if len(ranges) == 0 || entry.sequence-ranges[len(ranges)-1][1] > 1 {
			ranges = append(ranges, [2]uint64{entry.sequence, entry.sequence})
		} else {
			ranges[len(ranges)-1][1] = entry.sequence
		}
	}
	rows := iter.Seq2[pq.Row, error](func(yield func(pq.Row, error) bool) {
		for _, entry := range items {
			if !yield(entry.row, nil) {
				return
			}
		}
	})
	ref, manifest, err := a.Archive.Write(ctx, object, rows, pq.Manifest{
		Source: "jetstream", Stream: &key.stream, FirstSeq: &first, LastSeq: &last, SequenceRanges: ranges,
	})
	if err != nil {
		return err
	}
	return a.index.Record(ref, manifest, key.ingest)
}
