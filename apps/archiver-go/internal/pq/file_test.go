package pq

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestStageFlushesLargeRowGroups(t *testing.T) {
	payload := bytes.Repeat([]byte{1}, 1<<20)
	rows := make([]Row, 10)
	for i := range rows {
		rows[i] = Row{SchemaVersion: 1, Payload: payload}
	}
	path, info, err := Stage(t.TempDir(), rows, 1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	decoded, err := parquet.OpenFile(f, info.Size)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.RowGroups()) < 2 || decoded.NumRows() != int64(len(rows)) {
		t.Fatalf("got %d groups and %d rows", len(decoded.RowGroups()), decoded.NumRows())
	}
}

func TestParquetSchemaMatchesContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "testdata", "parquet_schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Compression string
		Columns     []struct{ Name, Type string }
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	path, file, err := Stage(t.TempDir(), []Row{{EventID: "event", SchemaVersion: 1, Payload: []byte{1}}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	decoded, err := parquet.OpenFile(f, file.Size)
	if err != nil {
		t.Fatal(err)
	}
	var names, types []string
	for _, field := range decoded.Schema().Fields() {
		if !field.Required() {
			t.Errorf("column %q must be required", field.Name())
		}
		names = append(names, field.Name())
		types = append(types, field.Type().String())
	}
	expectedTypes := map[string]string{
		"string": "STRING", "int32": "INT(32,true)", "timestamp[ms, tz=UTC]": "TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)",
		"double": "DOUBLE", "binary": "BYTE_ARRAY",
	}
	var wantedNames, wantedTypes []string
	for _, item := range fixture.Columns {
		wantedNames = append(wantedNames, item.Name)
		wantedTypes = append(wantedTypes, expectedTypes[item.Type])
	}
	if !slices.Equal(names, wantedNames) || !slices.Equal(types, wantedTypes) {
		t.Fatalf("schema names %v types %v; want %v / %v", names, types, wantedNames, wantedTypes)
	}
	for _, group := range decoded.Metadata().RowGroups {
		for _, column := range group.Columns {
			if column.MetaData.Codec.String() != "ZSTD" || fixture.Compression != "zstd" {
				t.Fatal("column codec differs from contract")
			}
		}
	}
}

func TestManifestRowsAndSHA256(t *testing.T) {
	path, info, err := Stage(t.TempDir(), []Row{{SchemaVersion: 1, EventTime: 200}, {SchemaVersion: 1, EventTime: 100}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	if info.Rows != 2 || info.SHA256 != hex.EncodeToString(hash[:]) || info.MinTime.UnixMilli() != 100 || info.MaxTime.UnixMilli() != 200 {
		t.Fatalf("incorrect Parquet summary: %+v", info)
	}
}
