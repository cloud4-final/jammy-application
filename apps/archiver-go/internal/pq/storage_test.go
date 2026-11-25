package pq

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type spyObjects struct {
	puts int
}

func (s *spyObjects) Put(_ context.Context, _ string, _ io.ReadSeeker, _ int64, _ string) error {
	s.puts++
	return nil
}
func (s *spyObjects) Get(context.Context, string) (io.ReadCloser, error) { return nil, nil }
func (s *spyObjects) Exists(context.Context, string) (bool, error)       { return false, nil }
func (s *spyObjects) List(context.Context, string) ([]string, error)     { return nil, nil }

func TestMultipartPartSizeScalesAbove10000Parts(t *testing.T) {
	const minPart = int64(64 << 20)
	for _, tc := range []struct {
		size int64
		want int64
	}{
		{0, minPart},
		{minPart, minPart},
		{500 << 30, minPart},
		{1000 << 30, 1 + ((1000 << 30) - 1)/10000},
	} {
		if got := multipartPartSize(tc.size); got != tc.want {
			t.Fatalf("multipartPartSize(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}
}

func TestValidateStagedParquetBeforeUpload(t *testing.T) {
	dir := t.TempDir()
	path, info, err := Stage(dir, []Row{{EventID: "e1", RoutingKey: "r", Profile: "solar", SchemaVersion: 1, IngestID: "i1", Payload: []byte{1}}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateFile(path, info.Size, info.Rows); err != nil {
		t.Fatalf("valid staged parquet failed validation: %v", err)
	}

	spy := &spyObjects{}
	archive := Archive{Objects: spy, Dir: dir, Level: 1}

	// Row count mismatch fails before upload.
	if _, _, err := archive.commitStaged(t.Context(), path, File{Rows: info.Rows + 1, Size: info.Size, SHA256: info.SHA256}, "raw/test.parquet", Manifest{Source: "direct-db"}); err == nil {
		t.Fatal("expected row count mismatch error")
	}

	// Corrupt staged bytes fail before upload.
	corrupt := filepath.Join(dir, "corrupt.parquet")
	if err := os.WriteFile(corrupt, []byte("not-a-parquet"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := archive.commitStaged(t.Context(), corrupt, File{Rows: 1, Size: 13}, "raw/corrupt.parquet", Manifest{Source: "direct-db"}); err == nil {
		t.Fatal("expected corrupt parquet validation error")
	}

	// Wrong schema (missing columns / optional / uncompressed) fails before upload.
	type badRow struct {
		EventID string `parquet:"event_id,optional"`
	}
	wrongSchema := filepath.Join(dir, "wrong-schema.parquet")
	if err := parquet.WriteFile(wrongSchema, []badRow{{EventID: "e1"}}); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(wrongSchema)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := archive.commitStaged(t.Context(), wrongSchema, File{Rows: 1, Size: stat.Size()}, "raw/wrong.parquet", Manifest{Source: "direct-db"}); err == nil {
		t.Fatal("expected schema mismatch validation error")
	}

	if spy.puts != 0 {
		t.Fatalf("expected 0 S3 uploads on pre-upload validation failure, got %d", spy.puts)
	}
}
