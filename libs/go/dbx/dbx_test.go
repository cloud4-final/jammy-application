package dbx

import (
	"context"
	"testing"
	"time"
)

func TestPoolDoesNotWaitForDB(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	t.Setenv("DB_POOL_MIN", "1")
	t.Setenv("DB_POOL_MAX", "10")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pool, err := Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if cfg := pool.Config(); cfg.MinConns != 1 || cfg.MaxConns != 10 {
		t.Errorf("pool bounds = %d..%d", cfg.MinConns, cfg.MaxConns)
	}
	if pool.Ping(ctx) == nil {
		t.Fatal("missing dependency reported healthy")
	}
}
