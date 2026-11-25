package gate_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cloud4-final/jammy-application/libs/go/codec"
	"github.com/cloud4-final/jammy-application/libs/go/gate"
)

func TestGateCases(t *testing.T) {
	dir := "../../../contracts/testdata/gate_cases"
	file, err := os.Open(filepath.Join(dir, "cases.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tc struct {
			Name            string
			InputFile       string         `json:"input_file"`
			ContentType     string         `json:"content_type"`
			ContentEncoding string         `json:"content_encoding"`
			Now             int64          `json:"now_unix_ms"`
			Status          int            `json:"expect_status"`
			Accepted        int            `json:"expect_accepted"`
			Rejected        int            `json:"expect_rejected"`
			Reasons         map[string]int `json:"expect_reasons"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &tc); err != nil {
			t.Fatal(err)
		}
		t.Run(tc.Name, func(t *testing.T) {
			wire, err := os.ReadFile(filepath.Join(dir, tc.InputFile))
			if err != nil {
				t.Fatal(err)
			}
			batch, _, err := codec.DecodeBatch(wire, tc.ContentType, tc.ContentEncoding, 16<<20, 64<<20)
			if tc.Status != 202 {
				var httpErr *codec.HTTPError
				if !errors.As(err, &httpErr) || httpErr.Status != tc.Status {
					t.Fatalf("decode error = %v, want HTTP %d", err, tc.Status)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			result := gate.Validate(batch, time.UnixMilli(tc.Now), gate.Config{
				MaxPast: 24 * time.Hour, MaxFuture: 5 * time.Minute,
			})
			reasons := make(map[string]int)
			for _, rejected := range result.Rejected {
				reasons[rejected.Reason]++
			}
			if len(result.Accepted) != tc.Accepted || len(result.Rejected) != tc.Rejected ||
				len(result.PrimaryValues) != tc.Accepted || !reflect.DeepEqual(reasons, tc.Reasons) {
				t.Fatalf("accepted=%d rejected=%d primary values=%d reasons=%v", len(result.Accepted), len(result.Rejected), len(result.PrimaryValues), reasons)
			}
		})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}
