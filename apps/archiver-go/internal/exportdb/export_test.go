package exportdb

import (
	"strings"
	"testing"
)

func TestAdvisoryLockMatchesPythonArchiver(t *testing.T) {
	if !strings.Contains(advisoryLockQuery, "hashtext('jammy'), hashtext('archive_exports')") {
		t.Fatalf("unexpected advisory lock query: %q", advisoryLockQuery)
	}
}

func TestExportKeyWithS3Prefix(t *testing.T) {
	if got := (&Exporter{}).exportKey("raw_events_2026112300"); got != "exports/direct-db/raw_events_2026112300.json" {
		t.Fatalf("default exportKey = %q", got)
	}
	if got := (&Exporter{KeyPrefix: "/site-a/"}).exportKey("raw_events_2026112300"); got != "site-a/exports/direct-db/raw_events_2026112300.json" {
		t.Fatalf("prefixed exportKey = %q", got)
	}
}
