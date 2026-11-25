package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestEnvFileOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value")
	if err := os.WriteFile(path, []byte("b\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JAMMY_TEST", "a")
	t.Setenv("JAMMY_TEST_FILE", path)
	if got := Env("JAMMY_TEST", "fallback"); got != "b" {
		t.Fatalf("Env = %q, want b", got)
	}
}

func TestLoadCommon(t *testing.T) {
	t.Setenv("SITE_ID", "site-a")
	t.Setenv("INSTANCE_ID", "ingest-a1")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "0")
	cfg, err := LoadCommon()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SiteID != "site-a" || cfg.InstanceID != "ingest-a1" || cfg.HTTPPort != 8080 ||
		cfg.AdminPort != 9090 || cfg.PipelineMode != "jetstream" || cfg.ShutdownTimeout != 0 {
		t.Fatalf("unexpected common settings: %+v", cfg)
	}
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "25s")
	if _, err := LoadCommon(); err == nil {
		t.Fatal("invalid duration accepted")
	}
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "25")
	t.Setenv("SITE_ID", "")
	if _, err := LoadCommon(); err == nil {
		t.Fatal("missing site accepted")
	}
}

func TestEnvCases(t *testing.T) {
	data, err := os.ReadFile("../../../contracts/testdata/env_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Type, Name, Input string
		Expected          any
		ExpectError       bool `json:"expect_error"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Type+"/"+tc.Input, func(t *testing.T) {
			var got any
			var err error
			switch tc.Type {
			case "duration":
				var value time.Duration
				value, err = ParseDuration(tc.Name, tc.Input)
				got = value.Seconds()
			case "size":
				var value int64
				value, err = ParseSize(tc.Input)
				got = float64(value)
			case "list":
				var value []string
				value, err = ParseList(tc.Input)
				items := make([]any, len(value))
				for i, item := range value {
					items[i] = item
				}
				got = items
			case "boolean":
				got, err = ParseBool(tc.Input)
			default:
				t.Fatalf("unknown fixture type %q", tc.Type)
			}
			if (err != nil) != tc.ExpectError || (err == nil && !reflect.DeepEqual(got, tc.Expected)) {
				t.Fatalf("parsed %v, error %v, want %v (error=%v)", got, err, tc.Expected, tc.ExpectError)
			}
		})
	}
}
