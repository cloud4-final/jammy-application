package state

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/aggregate"
	"github.com/cloud4-final/jammy-5th/apps/processor-go/internal/anomaly"
	"github.com/cloud4-final/jammy-5th/libs/go/leasefence"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	Pool       *pgxpool.Pool
	Consumer   string
	lookback   time.Duration
	ratio      float64
	mu         sync.Mutex
	capacities map[string]float64
	latest     map[string]aggregate.Event
	fence      *leasefence.Fence
}

type Batch struct {
	ID     string
	Events []aggregate.Event
}

func New(pool *pgxpool.Pool, consumer string, lookback time.Duration, ratio float64, fence *leasefence.Fence) *Store {
	return &Store{Pool: pool, Consumer: consumer, lookback: lookback, ratio: ratio, latest: make(map[string]aggregate.Event), fence: fence}
}

func (s *Store) Prepare(ctx context.Context) error {
	lookback := pgtype.Interval{Microseconds: s.lookback.Microseconds(), Valid: true}
	return s.Write(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT jammy.ensure_partitions('3 hours', $1)", lookback); err != nil {
			return err
		}
		return s.prune(ctx, tx)
	})
}

func (s *Store) Prune(ctx context.Context) error {
	return s.Write(ctx, func(tx pgx.Tx) error { return s.prune(ctx, tx) })
}

func (s *Store) prune(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "DELETE FROM jammy.processed_batches WHERE consumer = $1 AND processed_at < now() - interval '1 day'", s.Consumer)
	return err
}

func (s *Store) Write(ctx context.Context, write func(pgx.Tx) error) error {
	return s.fence.Transaction(ctx, s.Pool, write)
}

func (s *Store) RefreshAssets(ctx context.Context) error {
	rows, err := s.Pool.Query(ctx, "SELECT asset_id, capacity_kw::double precision FROM jammy.assets WHERE profile = 'solar'")
	if err != nil {
		return err
	}
	defer rows.Close()
	capacities := make(map[string]float64)
	for rows.Next() {
		var id string
		var capacity float64
		if err := rows.Scan(&id, &capacity); err != nil {
			return err
		}
		capacities[id] = capacity
	}
	if err := rows.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.capacities = capacities
	s.mu.Unlock()
	return nil
}

func (s *Store) Commit(ctx context.Context, batches []Batch) ([]bool, error) {
	// Resolve conflicting batch claims in a common order across workers.
	order := make([]int, len(batches))
	for index := range order {
		order[index] = index
	}
	slices.SortStableFunc(order, func(i, j int) int {
		if batches[i].ID < batches[j].ID {
			return -1
		}
		if batches[i].ID > batches[j].ID {
			return 1
		}
		return 0
	})
	duplicate := make([]bool, len(batches))
	var events []aggregate.Event
	err := s.Write(ctx, func(tx pgx.Tx) error {
		for _, index := range order {
			batch := batches[index]
			tag, err := tx.Exec(ctx, `INSERT INTO jammy.processed_batches (consumer, batch_id)
				VALUES ($1, $2) ON CONFLICT DO NOTHING`, s.Consumer, batch.ID)
			if err != nil {
				return err
			}
			duplicate[index] = tag.RowsAffected() == 0
			if !duplicate[index] {
				events = append(events, batch.Events...)
			}
		}
		return s.Apply(ctx, tx, events)
	})
	if err == nil {
		s.CoalesceLatest(events)
	}
	return duplicate, err
}

func (s *Store) Apply(ctx context.Context, tx pgx.Tx, events []aggregate.Event) error {
	buckets := aggregate.Fold(events)
	keys := slices.Collect(maps.Keys(buckets))
	slices.SortFunc(keys, func(a, b aggregate.Key) int {
		if a.Profile < b.Profile {
			return -1
		}
		if a.Profile > b.Profile {
			return 1
		}
		if a.RoutingKey < b.RoutingKey {
			return -1
		}
		if a.RoutingKey > b.RoutingKey {
			return 1
		}
		return a.Minute.Compare(b.Minute)
	})
	writes := &pgx.Batch{}
	for _, key := range keys {
		bucket := buckets[key]
		writes.Queue(`INSERT INTO jammy.summary_1m (profile, routing_key, minute, count, sum, min, max)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (profile,routing_key,minute) DO UPDATE SET
			count = summary_1m.count + EXCLUDED.count, sum = summary_1m.sum + EXCLUDED.sum,
			min = LEAST(summary_1m.min, EXCLUDED.min), max = GREATEST(summary_1m.max, EXCLUDED.max)`,
			key.Profile, key.RoutingKey, key.Minute, bucket.Count, bucket.Sum, bucket.Min, bucket.Max)
	}
	if writes.Len() == 0 {
		return nil
	}
	if err := tx.SendBatch(ctx, writes).Close(); err != nil {
		return err
	}

	s.mu.Lock()
	capacities := s.capacities
	s.mu.Unlock()
	profiles := make([]string, 0, len(keys))
	routes := make([]string, 0, len(keys))
	minutes := make([]time.Time, 0, len(keys))
	for _, key := range keys {
		profiles = append(profiles, key.Profile)
		routes = append(routes, key.RoutingKey)
		minutes = append(minutes, key.Minute)
	}
	_, err := tx.Exec(ctx, `WITH touched AS (
        SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[]) AS k(profile, routing_key, minute)
    ), means AS (
        SELECT t.profile, t.routing_key, t.minute,
               max(b.sum/b.count) FILTER (WHERE b.minute=t.minute) AS last_mean,
               sum(b.sum)/sum(b.count)::double precision AS window_mean
        FROM touched t JOIN jammy.summary_1m b USING (profile, routing_key)
        WHERE b.minute BETWEEN t.minute - interval '4 minutes' AND t.minute
        GROUP BY t.profile, t.routing_key, t.minute
    )
    INSERT INTO jammy.incidents (routing_key, profile, kind, window_start, window_end, details)
    SELECT routing_key, profile, 'drop', minute - interval '4 minutes', minute + interval '1 minute',
           jsonb_build_object('last_mean', last_mean, 'window_mean', window_mean, 'ratio', $4::double precision)
    FROM means WHERE last_mean <= window_mean * $4
    ORDER BY profile, routing_key, minute
    ON CONFLICT (routing_key,kind,window_start) DO NOTHING`, profiles, routes, minutes, s.ratio)
	if err != nil {
		return err
	}
	for _, key := range keys {
		capacity, known := capacities[key.RoutingKey]
		if known && anomaly.OverCapacity(key.Profile, buckets[key].Max, capacity) {
			err = incident(ctx, tx, key, "over_capacity", key.Minute, map[string]float64{
				"power_kw": buckets[key].Max, "capacity_kw": capacity,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func incident(ctx context.Context, tx pgx.Tx, key aggregate.Key, kind string, start time.Time, details map[string]float64) error {
	_, err := tx.Exec(ctx, `INSERT INTO jammy.incidents (routing_key, profile, kind, window_start, window_end, details)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (routing_key,kind,window_start) DO NOTHING`,
		key.RoutingKey, key.Profile, kind, start, key.Minute.Add(time.Minute), details)
	return err
}

// CoalesceLatest is called only for events whose aggregation committed.
func (s *Store) CoalesceLatest(events []aggregate.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range events {
		prior, found := s.latest[event.RoutingKey]
		if !found || event.Time.After(prior.Time) {
			s.latest[event.RoutingKey] = event
		}
	}
}

// FlushLatest swaps buffers so stream workers can keep committing while the
// upsert runs. On failure, keep pending state for the next flush.
func (s *Store) FlushLatest(ctx context.Context) error {
	s.mu.Lock()
	if len(s.latest) == 0 {
		s.mu.Unlock()
		return nil
	}
	latest := s.latest
	s.latest = make(map[string]aggregate.Event)
	s.mu.Unlock()
	keys := slices.Sorted(maps.Keys(latest))
	profiles := make([]string, 0, len(keys))
	values := make([]float64, 0, len(keys))
	times := make([]time.Time, 0, len(keys))
	for _, key := range keys {
		event := latest[key]
		profiles = append(profiles, event.Profile)
		values = append(values, event.Value)
		times = append(times, event.Time)
	}
	err := s.Write(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO jammy.latest_state (routing_key, profile, primary_value, event_time)
        SELECT * FROM unnest($1::text[], $2::text[], $3::double precision[], $4::timestamptz[])
        ON CONFLICT (routing_key) DO UPDATE SET
        profile=EXCLUDED.profile,primary_value=EXCLUDED.primary_value,event_time=EXCLUDED.event_time,updated_at=now()
        WHERE latest_state.event_time < EXCLUDED.event_time`, keys, profiles, values, times)
		return err
	})
	if err != nil {
		s.CoalesceLatest(slices.Collect(maps.Values(latest)))
	}
	return err
}
