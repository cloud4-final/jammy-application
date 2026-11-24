package main

import "testing"

func TestRejectInvalidParityEnv(t *testing.T) {
	t.Setenv("SITE_ID", "test")
	t.Setenv("PIPELINE_MODE", "direct-db")
	for name, value := range map[string]string{
		"HTTP_PORT": "0", "ADMIN_PORT": "0",
		"INGEST_MAX_BODY_BYTES": "0", "INGEST_MAX_DECOMPRESSED_BYTES": "0", "JS_MAX_BYTES": "0",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			if _, err := load(); err == nil {
				t.Fatalf("accepted %s=%q", name, value)
			}
		})
	}
}
