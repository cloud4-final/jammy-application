package sourcedb

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/state"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestDBModeCursorProgressExact holds an older xid open while a newer
// transaction commits. The cursor must advance past neither event until both
// transactions finish. Two fresh readers then see each event exactly once.
func TestDBModeCursorProgressExact(t *testing.T) {
	docker := func(args ...string) string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		cmd := exec.Command("docker", args...)
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("docker %v: %v\n%s%s", args, err, stderr.String(), stdout.String())
		}
		return strings.TrimSpace(stdout.String())
	}
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx := t.Context()
	id := docker("run", "-d", "-p", "127.0.0.1::5432", "-e", "POSTGRES_PASSWORD=test", "--tmpfs", "/var/lib/postgresql/data", "postgres:17.12-trixie")
	t.Cleanup(func() { docker("rm", "-f", id) })
	addr := docker("port", id, "5432/tcp")
	pool, err := pgxpool.New(ctx, "postgres://postgres:test@"+addr+"/postgres?sslmode=disable")
	check(err)
	t.Cleanup(pool.Close)
	deadline := time.Now().Add(90 * time.Second)
	for {
		c, cancel := context.WithTimeout(ctx, time.Second)
		err = pool.Ping(c)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("postgres did not become ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	migrations := filepath.Join("..", "..", "..", "..", "contracts", "db", "migrations")
	initial, err := os.ReadFile(filepath.Join(migrations, "0001_init.up.sql"))
	check(err)
	_, err = pool.Exec(ctx, string(initial))
	check(err)
	cursor, err := os.ReadFile(filepath.Join(migrations, "0002_direct_db_xid_cursor.up.sql"))
	check(err)
	_, err = pool.Exec(ctx, string(cursor))
	check(err)

	older, err := pool.Begin(ctx)
	check(err)
	defer func() { _ = older.Rollback(ctx) }()
	const insert = `
		INSERT INTO jammy.raw_events
			(event_id, routing_key, profile, schema_version, event_time, ingest_id, primary_value, payload)
		VALUES ($1, 'fixture', 'synth-small', 1, now(), 'fixture', 1, ''::bytea)`
	_, err = older.Exec(ctx, insert, "older")
	check(err)
	_, err = pool.Exec(ctx, insert, "newer")
	check(err)

	poll := func(want int) {
		t.Helper()
		store := state.New(pool, "processor", 24*time.Hour, .5)
		count, received, err := Poll(ctx, store, 1)
		check(err)
		if (count == 0) != received.IsZero() {
			t.Fatalf("count=%d received_at=%s", count, received)
		}
		if count != want {
			t.Fatalf("expected %d rows, got %d", want, count)
		}
	}
	poll(0)
	check(older.Commit(ctx))
	poll(1)
	poll(1)
	poll(0)
	var count int
	check(pool.QueryRow(ctx, "SELECT sum(count) FROM jammy.summary_1m").Scan(&count))
	if count != 2 {
		t.Fatalf("summary counts = %d, expected 2", count)
	}

	hour := time.Now().UTC().Truncate(time.Hour)
	for _, offset := range []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute} {
		_, err := pool.Exec(ctx, `INSERT INTO jammy.raw_events
            (event_id, routing_key, profile, schema_version, event_time, received_at, ingest_id, primary_value, payload)
            VALUES ($1, 'fixture', 'synth-small', 1, now(), $2, 'fixture', 1, ''::bytea)`,
			offset.String(), hour.Add(offset))
		check(err)
	}
	store := state.New(pool, "processor", 24*time.Hour, .5)
	processed, received, err := Poll(ctx, store, 2)
	check(err)
	if processed != 2 || !received.Equal(hour.Add(2*time.Minute)) {
		t.Fatalf("poll should report the last processed row, got count=%d received_at=%s", processed, received)
	}

	t.Run("cursor index in generic plan", func(t *testing.T) {
		check := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		tx, err := pool.Begin(t.Context())
		check(err)
		defer func() { _ = tx.Rollback(t.Context()) }()
		_, err = tx.Exec(t.Context(), `INSERT INTO jammy.raw_events
			(event_id,routing_key,profile,schema_version,event_time,received_at,ingest_id,primary_value,payload,ingest_xid)
			SELECT 'plan-' || i, 'fixture', 'synth-small', 1, $1::timestamptz, $1::timestamptz + (i % 2) * interval '1 hour',
				'fixture', 1, ''::bytea, (100 + i % 500)::text::xid8 FROM generate_series(1, 16384) i`, hour)
		check(err)
		_, err = tx.Exec(t.Context(), `ANALYZE jammy.raw_events; SET LOCAL plan_cache_mode = force_generic_plan`)
		check(err)
		_, err = tx.Exec(t.Context(), "PREPARE cursor_regression AS "+cursorQuery)
		check(err)
		defer func() { _, _ = tx.Exec(t.Context(), "DEALLOCATE cursor_regression") }()
		indexRows, err := tx.Query(t.Context(), `SELECT c.relname FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = 'jammy.raw_events_ingest_cursor_idx'::regclass`)
		check(err)
		var indexes []string
		for indexRows.Next() {
			var name string
			check(indexRows.Scan(&name))
			indexes = append(indexes, name)
		}
		check(indexRows.Err())
		lines, err := tx.Query(t.Context(), "EXPLAIN EXECUTE cursor_regression('0', '', 64)")
		check(err)
		var plan strings.Builder
		for lines.Next() {
			var line string
			check(lines.Scan(&line))
			plan.WriteString(line + "\n")
		}
		check(lines.Err())
		output := plan.String()
		for _, index := range indexes {
			if strings.Contains(output, "Index Scan using "+index+" ") && strings.Contains(output, "Merge Append") {
				return
			}
		}
		t.Fatalf("expected ordered scans on the partition cursor indexes, got:\n%s", output)
	})
}
