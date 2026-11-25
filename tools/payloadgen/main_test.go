package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloud4-final/jammy-5th/tools/payloadgen/gen"
)

func TestRunAssetsWritesSeedSQL(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"assets", "--count", "2", "--tenants", "2"}, &output); err != nil {
		t.Fatal(err)
	}
	if sql := output.String(); strings.Count(sql, "INSERT INTO jammy.assets") != 2 || !strings.Contains(sql, "tenant-000002") {
		t.Fatalf("unexpected seed SQL: %q", sql)
	}
}

func TestRunBatchesWritesIndexAndBatch(t *testing.T) {
	output := t.TempDir()
	if err := run([]string{"batches", "--out", output, "--events-per-batch", "2"}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(output, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var index []gen.IndexEntry
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	if len(index) != 1 || index[0].Events != 2 {
		t.Fatalf("unexpected index: %s", data)
	}
	file, err := os.Stat(filepath.Join(output, index[0].File))
	if err != nil {
		t.Fatal(err)
	}
	if file.Size() != int64(index[0].WireBytes) || file.Size() == 0 {
		t.Fatalf("unexpected wire size %d for entry %+v", file.Size(), index[0])
	}
}

func TestRunRejectsInvalidArguments(t *testing.T) {
	for _, test := range []struct {
		args []string
		want string
	}{
		{nil, "usage:"},
		{[]string{"unknown"}, "unknown subcommand"},
		{[]string{"assets", "extra"}, "unexpected argument"},
		{[]string{"batches"}, "requires --out"},
		{[]string{"batches", "--out", t.TempDir(), "--time-mode", "bad"}, "invalid --time-mode"},
	} {
		if err := run(test.args, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("run(%v) = %v, want an error containing %q", test.args, err, test.want)
		}
	}
}
