package aggregate

import (
	"math"
	"time"
)

type Event struct {
	Profile, RoutingKey string
	Time                time.Time
	Value               float64
}
type Key struct {
	Profile, RoutingKey string
	Minute              time.Time
}
type Bucket struct {
	Count         int64
	Sum, Min, Max float64
}

func Fold(events []Event) map[Key]Bucket {
	buckets := make(map[Key]Bucket)
	for _, event := range events {
		key := Key{event.Profile, event.RoutingKey, event.Time.UTC().Truncate(time.Minute)}
		bucket, exists := buckets[key]
		if !exists {
			bucket.Min, bucket.Max = event.Value, event.Value
		}
		bucket.Count++
		bucket.Sum += event.Value
		bucket.Min = math.Min(bucket.Min, event.Value)
		bucket.Max = math.Max(bucket.Max, event.Value)
		buckets[key] = bucket
	}
	return buckets
}
