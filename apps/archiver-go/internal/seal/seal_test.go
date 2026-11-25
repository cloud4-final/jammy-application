package seal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cloud4-final/jammy-application/apps/archiver-go/internal/pq"
)

type memoryObjects struct {
	data map[string][]byte
}

func (m *memoryObjects) Put(_ context.Context, key string, body io.ReadSeeker, _ int64, _ string) error {
	raw, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	m.data[key] = raw
	return nil
}

func (m *memoryObjects) Get(_ context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.data[key])), nil
}

func (m *memoryObjects) Exists(_ context.Context, key string) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

func (m *memoryObjects) List(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for key := range m.data {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	return out, nil
}

func TestSealWrittenAfterWatermark(t *testing.T) {
	hour := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	end := hour.Add(time.Hour + 10*time.Minute)
	if Eligible(hour, end, 10*time.Minute, false) || !Eligible(hour, end.Add(time.Millisecond), 10*time.Minute, false) {
		t.Fatal("JetStream seals only after the watermark exceeds hour end plus lateness")
	}
	if Eligible(hour, hour.Add(time.Hour), 10*time.Minute, true) || !Eligible(hour, hour.Add(2*time.Hour), 10*time.Minute, true) {
		t.Fatal("direct DB seals only through the last received hour allowed by the lateness window")
	}
}

func TestLoadTargetAndSealWithS3Prefix(t *testing.T) {
	hour := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	for _, keyPrefix := range []string{"site-a/", "raw/"} {
		store := &memoryObjects{data: make(map[string][]byte)}
		prefix := PrefixWith(keyPrefix, "solar", hour)
		wantPrefix := keyPrefix + "raw/profile=solar/dt=2026-11-23/hour=12/"
		if prefix != wantPrefix {
			t.Fatalf("PrefixWith = %q, want %q", prefix, wantPrefix)
		}
		parquetKey := prefix + "ing-1-1.parquet"
		seq := uint64(1)
		manifest := pq.Manifest{
			Version:       1,
			Source:        "jetstream",
			FirstSeq:      &seq,
			LastSeq:       &seq,
			Rows:          3,
			SchemaVersion: 1,
			SHA256:        "abc",
			MinTime:       hour.Format(pq.Stamp),
			MaxTime:       hour.Add(2 * time.Hour).Format(pq.Stamp),
		}
		rawManifest, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		store.data[parquetKey] = []byte("pq")
		store.data[pq.ManifestKey(parquetKey)] = rawManifest

		index, err := LoadWithPrefix(t.Context(), store, keyPrefix, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := index.Flush(t.Context(), "ing", time.Time{}, 10*time.Minute, false); err != nil {
			t.Fatal(err)
		}
		sealKey := prefix + "_SEALED-ing.json"
		var doc Document
		if err := json.Unmarshal(store.data[sealKey], &doc); err != nil {
			t.Fatalf("missing seal at %s: %v", sealKey, err)
		}
		if len(doc.Files) != 1 || doc.Files[0] != parquetKey {
			t.Fatalf("seal files = %v, want [%s]", doc.Files, parquetKey)
		}
		late := index.TargetPrefix(prefix, "ing")
		wantLate := keyPrefix + "raw-late/profile=solar/dt=2026-11-23/hour=12/"
		if late != wantLate {
			t.Fatalf("TargetPrefix = %q, want %q", late, wantLate)
		}
	}
}
