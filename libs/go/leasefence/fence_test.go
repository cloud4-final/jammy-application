package leasefence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type leaseDB struct {
	holder, fenced bool
	renewed, now   time.Time
	err            error
	calls          int
}

func (db *leaseDB) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	db.calls++
	return db
}

func (db *leaseDB) Scan(dest ...any) error {
	*dest[0].(*bool), *dest[1].(*bool) = db.holder, db.fenced
	*dest[2].(*time.Time), *dest[3].(*time.Time) = db.renewed, db.now
	return db.err
}

func TestCacheRespectsLeaseDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	db := &leaseDB{holder: true, now: now, renewed: now.Add(-9750 * time.Millisecond)}
	fence := New("site-a", 10*time.Second)
	fence.now = func() time.Time { return now }
	check := func(want error, calls int) {
		t.Helper()
		if err := fence.Check(ctx, db); !errors.Is(err, want) || db.calls != calls {
			t.Fatalf("check returned %v after %d reads, want %v after %d reads", err, db.calls, want, calls)
		}
	}
	check(nil, 1)
	now = now.Add(100 * time.Millisecond)
	check(nil, 1)
	now = now.Add(150 * time.Millisecond)
	db.now = db.now.Add(250 * time.Millisecond)
	check(ErrFenced, 2)
	db.renewed = db.now
	now = now.Add(500 * time.Millisecond)
	check(ErrFenced, 2)
	now = now.Add(500 * time.Millisecond)
	check(nil, 3)
	db.err = pgx.ErrNoRows
	now = now.Add(time.Second)
	check(ErrFenced, 4)
}
