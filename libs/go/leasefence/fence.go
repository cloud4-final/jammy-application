// Package leasefence guards database writes using the dispatcher's published lease.
package leasefence

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cloud4-final/jammy-5th/libs/go/config"
	"github.com/jackc/pgx/v5"
)

var ErrFenced = errors.New("database writes are fenced for this site")

type Fence struct {
	siteID string
	maxAge time.Duration
	now    func() time.Time

	mu          sync.Mutex
	cachedUntil time.Time
	allowed     bool
}

func New(siteID string, maxAge time.Duration) *Fence {
	return &Fence{siteID: siteID, maxAge: maxAge, now: time.Now}
}

func FromEnv(siteID string) (*Fence, error) {
	value, err := config.Lookup("WRITE_FENCE", "false")
	if err != nil {
		return nil, err
	}
	enabled, err := config.ParseBool(value)
	if err != nil {
		return nil, fmt.Errorf("invalid WRITE_FENCE: %w", err)
	}
	if !enabled {
		return nil, nil
	}
	maxAge, err := seconds("LEASE_FENCE_SECONDS", "10")
	if err != nil {
		return nil, err
	}
	ttl, err := seconds("LEASE_TTL_SECONDS", "15")
	if err != nil {
		return nil, err
	}
	if maxAge >= ttl {
		return nil, errors.New("LEASE_FENCE_SECONDS must be less than LEASE_TTL_SECONDS")
	}
	return New(siteID, maxAge), nil
}

func seconds(name, fallback string) (time.Duration, error) {
	value, err := config.Lookup(name, fallback)
	if err != nil {
		return 0, err
	}
	if number, err := strconv.ParseUint(value, 10, 63); err != nil || number == 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return config.ParseDuration(name, value)
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (f *Fence) Check(ctx context.Context, db rowQuerier) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	if !now.Before(f.cachedUntil) {
		var holder, fenced bool
		var renewedAt, dbNow time.Time
		err := db.QueryRow(ctx, `SELECT holder, fenced, renewed_at, clock_timestamp()
			FROM jammy.site_lease_state WHERE site_id = $1`, f.siteID).Scan(&holder, &fenced, &renewedAt, &dbNow)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		remaining := f.maxAge - dbNow.Sub(renewedAt)
		f.allowed = err == nil && holder && !fenced && remaining > 0 && f.now().Before(now.Add(remaining))
		f.cachedUntil = now.Add(time.Second)
		if f.allowed {
			f.cachedUntil = now.Add(min(time.Second, remaining))
		}
	}
	if !f.allowed {
		return ErrFenced
	}
	return nil
}

func (f *Fence) Transaction(ctx context.Context, db interface {
	Begin(context.Context) (pgx.Tx, error)
}, write func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if err := f.Check(ctx, tx); err != nil {
			return err
		}
		if err := write(tx); err != nil {
			return err
		}
		return f.Check(ctx, tx)
	})
}
