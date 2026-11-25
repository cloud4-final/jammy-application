package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrationURLUsesFileOverride(t *testing.T) {
	file := filepath.Join(t.TempDir(), "database-url")
	if err := os.WriteFile(file, []byte("postgres://from-file/db?sslmode=disable&x-migrations-table=other#fragment\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL", "postgres://from-env/db")
	t.Setenv("DATABASE_URL_FILE", file)
	got, err := migrationURL()
	if err != nil {
		t.Fatal(err)
	}
	want := "postgres://from-file/db?x-migrations-table-quoted=true&x-migrations-table=%22public%22.%22schema_migrations%22&sslmode=disable&x-migrations-table=other"
	if got != want {
		t.Fatalf("migration URL = %q, want %q", got, want)
	}
}

func TestMigrationURLRejectsMissingOrUnreadableSettings(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if _, err := migrationURL(); err == nil || err.Error() != "DATABASE_URL is required" {
		t.Fatalf("expected required setting error, got %v", err)
	}
	t.Setenv("DATABASE_URL", "postgres://from-env/db")
	t.Setenv("DATABASE_URL_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := migrationURL(); err == nil || !strings.Contains(err.Error(), "DATABASE_URL_FILE") {
		t.Fatalf("expected unreadable override error, got %v", err)
	}
}
