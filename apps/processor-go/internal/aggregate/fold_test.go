package aggregate

import (
	"testing"
	"time"
)

func TestFoldMinuteBuckets(t *testing.T) {
	start := time.Unix(60, 0).UTC()
	input := []Event{
		{Profile: "solar", RoutingKey: "panel", Time: start.Add(time.Second), Value: 7},
		{Profile: "solar", RoutingKey: "panel", Time: start.Add(time.Millisecond), Value: 2},
		{Profile: "solar", RoutingKey: "panel", Time: start.Add(time.Minute), Value: 5},
	}
	got := Fold(input)
	if len(got) != 2 || got[Key{"solar", "panel", start}] != (Bucket{2, 9, 2, 7}) {
		t.Fatalf("unexpected buckets: %v", got)
	}
}
