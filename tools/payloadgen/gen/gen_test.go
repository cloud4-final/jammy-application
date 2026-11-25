package gen

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloud4-final/jammy-5th/libs/go/codec"
	"github.com/cloud4-final/jammy-5th/libs/go/gate"
	pb "github.com/cloud4-final/jammy-5th/libs/go/pb/jammyv1"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

func TestDeterministicWithSeed(t *testing.T) {
	for _, profile := range []string{"solar", "synth-small", "synth-large"} {
		t.Run(profile, func(t *testing.T) {
			first, second := t.TempDir(), t.TempDir()
			opts := Options{Profile: profile, EventsPerBatch: 6, Batches: 2, Assets: 4, Seed: 42}
			if err := WriteBatches(first, opts); err != nil {
				t.Fatal(err)
			}
			if err := WriteBatches(second, opts); err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{"index.json", "batch-000001.bin", "batch-000002.bin"} {
				if !bytes.Equal(readFile(t, first, file), readFile(t, second, file)) {
					t.Errorf("same seed produced different %s files", file)
				}
			}
		})
	}
}

func TestBatchesPassGate(t *testing.T) {
	for _, profile := range []string{"solar", "synth-small", "synth-large"} {
		t.Run(profile, func(t *testing.T) {
			dir := t.TempDir()
			now := time.Date(2026, time.November, 23, 4, 0, 0, 0, time.UTC)
			opts := Options{Profile: profile, EventsPerBatch: 6, Batches: 2, Assets: 4, Seed: 42, Anchor: now}
			if err := WriteBatches(dir, opts); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"batch-000001.bin", "batch-000002.bin"} {
				wire := readFile(t, dir, name)
				batch, _, err := codec.DecodeBatch(wire, "application/x-protobuf", "zstd", 16<<20, 64<<20)
				if err != nil {
					t.Fatalf("decode %s: %v", name, err)
				}
				result := gate.Validate(batch, now, gate.Config{MaxPast: 24 * time.Hour, MaxFuture: 5 * time.Minute})
				if len(result.Accepted) != opts.EventsPerBatch || len(result.Rejected) != 0 {
					t.Errorf("%s: accepted %d of %d events, rejected %v", name, len(result.Accepted), opts.EventsPerBatch, result.Rejected)
				}
			}
		})
	}
}

func TestIndexBytesMatchFiles(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, time.November, 23, 4, 0, 0, 0, time.UTC)
	opts := Options{Profile: "synth-large", EventsPerBatch: 8, Batches: 3, Assets: 5, Seed: 77, Anchor: start}
	if err := WriteBatches(dir, opts); err != nil {
		t.Fatal(err)
	}
	var index []IndexEntry
	if err := json.Unmarshal(readFile(t, dir, "index.json"), &index); err != nil {
		t.Fatal(err)
	}
	if len(index) != opts.Batches {
		t.Fatalf("index has %d batches, want %d", len(index), opts.Batches)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	for _, entry := range index {
		wire := readFile(t, dir, entry.File)
		assertSingleFrame(t, wire)
		raw, err := decoder.DecodeAll(wire, nil)
		if err != nil {
			t.Fatal(err)
		}
		var batch pb.EventBatch
		if err := proto.Unmarshal(raw, &batch); err != nil {
			t.Fatal(err)
		}
		if len(wire) != entry.WireBytes || len(raw) != entry.RawBytes ||
			batch.GetBatchId() != entry.BatchID || len(batch.GetEvents()) != entry.Events ||
			entry.Events != opts.EventsPerBatch {
			t.Errorf("index entry does not describe %s: %+v", entry.File, entry)
		}
		for _, event := range batch.GetEvents() {
			eventTime := time.UnixMilli(event.GetEventTimeUnixMs())
			if eventTime.After(start) || eventTime.Before(start.Add(-time.Minute)) {
				t.Errorf("event_time %s is outside the minute ending at %s", eventTime, start)
			}
		}
	}
}

// A frame ends at its last block and optional four-byte checksum.
func assertSingleFrame(t *testing.T, wire []byte) {
	t.Helper()
	var header zstd.Header
	if err := header.Decode(wire); err != nil || header.Skippable {
		t.Fatalf("expected a zstd data frame, got %+v, %v", header, err)
	}
	offset := header.HeaderSize
	for {
		if len(wire)-offset < 3 {
			t.Fatal("truncated zstd block header")
		}
		block := uint32(wire[offset]) | uint32(wire[offset+1])<<8 | uint32(wire[offset+2])<<16
		blockSize := int(block >> 3)
		if (block>>1)&3 == 1 {
			blockSize = 1 // RLE stores one byte regardless of decoded size.
		}
		offset += 3 + blockSize
		if block&1 != 0 {
			break
		}
	}
	if header.HasCheckSum {
		offset += 4
	}
	if offset != len(wire) {
		t.Fatalf("frame consumes %d of %d bytes", offset, len(wire))
	}
}

func readFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
