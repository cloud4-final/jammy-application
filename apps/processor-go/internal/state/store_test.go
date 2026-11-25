package state

import (
	"testing"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/aggregate"
)

func TestCoalesceLatestEqualTimestampUsesStrictLessThan(t *testing.T) {
	t0 := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)

	store := New(nil, "processor", time.Hour, 0.5, nil)
	store.CoalesceLatest([]aggregate.Event{
		{Profile: "solar", RoutingKey: "a", Time: t0, Value: 10},
		{Profile: "solar", RoutingKey: "a", Time: t0, Value: 20},
	})
	if got := store.latest["a"]; got.Value != 10 {
		t.Fatalf("equal timestamp overwrote prior reading: got %v, want 10", got.Value)
	}

	store.CoalesceLatest([]aggregate.Event{
		{Profile: "solar", RoutingKey: "a", Time: t1, Value: 30},
	})
	if got := store.latest["a"]; got.Value != 30 {
		t.Fatalf("newer timestamp did not replace prior reading: got %v, want 30", got.Value)
	}
}
