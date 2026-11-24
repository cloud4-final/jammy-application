package dbx

import (
	"context"
	"errors"
	"strconv"

	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Open creates one pool per process. Connection establishment runs in the
// background and readiness checks can call Ping on the returned pool.
func Open(ctx context.Context) (*pgxpool.Pool, error) {
	url, err := config.Lookup("DATABASE_URL", "")
	if err != nil {
		return nil, err
	}
	if url == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("invalid DATABASE_URL")
	}
	minimum, err := config.Lookup("DB_POOL_MIN", "1")
	if err != nil {
		return nil, err
	}
	maximum, err := config.Lookup("DB_POOL_MAX", "10")
	if err != nil {
		return nil, err
	}
	minConns, err := strconv.ParseUint(minimum, 10, 31)
	if err != nil {
		return nil, errors.New("invalid DB_POOL_MIN")
	}
	maxConns, err := strconv.ParseUint(maximum, 10, 31)
	if err != nil || maxConns == 0 || maxConns < minConns {
		return nil, errors.New("DB_POOL_MAX must be positive and at least DB_POOL_MIN")
	}
	cfg.MinConns = int32(minConns)
	cfg.MaxConns = int32(maxConns)
	return pgxpool.NewWithConfig(ctx, cfg)
}
