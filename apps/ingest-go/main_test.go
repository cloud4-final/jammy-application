package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRejectInvalidParityEnv(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "direct-db")
	t.Setenv("DATABASE_URL", "postgresql://localhost/test")
	for name, value := range map[string]string{
		"HTTP_PORT": "0", "ADMIN_PORT": "0",
		"INGEST_MAX_BODY_BYTES": "0", "INGEST_MAX_DECOMPRESSED_BYTES": "0", "JS_MAX_BYTES": "0",
		"QUARANTINE_MAX_AGE": "0h", "QUARANTINE_MAX_BYTES": "0",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			if _, err := load(); err == nil {
				t.Fatalf("accepted %s=%q", name, value)
			}
		})
	}
}

func TestQuarantineDefaults(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "direct-db")
	t.Setenv("DATABASE_URL", "postgresql://localhost/test")
	cfg, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.quarantineAge != 168*time.Hour || cfg.quarantineBytes != 1<<30 {
		t.Fatalf("unexpected quarantine defaults: age=%s bytes=%d", cfg.quarantineAge, cfg.quarantineBytes)
	}
}

func TestLoadValidatesBackendEnv(t *testing.T) {
	t.Run("jetstream invalid stream name", func(t *testing.T) {
		t.Setenv("SITE_ID", "test")
		t.Setenv("PIPELINE_MODE", "jetstream")
		t.Setenv("NATS_URL", "nats://localhost")
		t.Setenv("INGEST_STREAM", "raw.*")
		if _, err := load(); err == nil || err.Error() != "invalid INGEST_STREAM" {
			t.Fatalf("got err=%v, want invalid INGEST_STREAM", err)
		}
	})

	t.Run("jetstream missing NATS_URL with only SITE_ID", func(t *testing.T) {
		t.Setenv("SITE_ID", "ci")
		for _, name := range []string{"PIPELINE_MODE", "NATS_URL", "NATS_URL_FILE", "INGEST_STREAM", "INGEST_STREAM_FILE"} {
			if prev, ok := os.LookupEnv(name); ok {
				t.Cleanup(func() { _ = os.Setenv(name, prev) })
			} else {
				t.Cleanup(func() { _ = os.Unsetenv(name) })
			}
			_ = os.Unsetenv(name)
		}
		_, err := load()
		if err == nil || err.Error() != "NATS_URL is required" {
			t.Fatalf("got err=%v, want exact %q", err, "NATS_URL is required")
		}
	})

	t.Run("jetstream missing NATS_URL", func(t *testing.T) {
		t.Setenv("SITE_ID", "test")
		t.Setenv("PIPELINE_MODE", "jetstream")
		t.Setenv("NATS_URL", "")
		t.Setenv("INGEST_STREAM", "raw")
		_, err := load()
		if err == nil || !strings.Contains(err.Error(), "NATS_URL") {
			t.Fatalf("got err=%v, want NATS_URL error", err)
		}
	})

	t.Run("jetstream NATS_URL_FILE", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "nats_url")
		if err := os.WriteFile(file, []byte("nats://localhost:4222\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SITE_ID", "test")
		t.Setenv("PIPELINE_MODE", "jetstream")
		t.Setenv("NATS_URL_FILE", file)
		t.Setenv("INGEST_STREAM", "raw")
		if _, err := load(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("direct-db missing DATABASE_URL", func(t *testing.T) {
		t.Setenv("SITE_ID", "test")
		t.Setenv("PIPELINE_MODE", "direct-db")
		t.Setenv("DATABASE_URL", "")
		_, err := load()
		if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
			t.Fatalf("got err=%v, want DATABASE_URL error", err)
		}
	})

	t.Run("direct-db invalid DATABASE_URL", func(t *testing.T) {
		t.Setenv("SITE_ID", "test")
		t.Setenv("PIPELINE_MODE", "direct-db")
		t.Setenv("DATABASE_URL", "://bad-url")
		_, err := load()
		if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
			t.Fatalf("got err=%v, want DATABASE_URL error", err)
		}
	})

	t.Run("direct-db invalid DB_POOL_MAX", func(t *testing.T) {
		t.Setenv("SITE_ID", "test")
		t.Setenv("PIPELINE_MODE", "direct-db")
		t.Setenv("DATABASE_URL", "postgresql://localhost/test")
		t.Setenv("DB_POOL_MIN", "4")
		t.Setenv("DB_POOL_MAX", "2")
		_, err := load()
		if err == nil || !strings.Contains(err.Error(), "DB_POOL_MAX") {
			t.Fatalf("got err=%v, want DB_POOL_MAX error", err)
		}
	})
}
