package sinkdb

import (
	"context"
	"time"

	"github.com/cloud4-final/jammy-5th/apps/ingest-go/internal/server"
	"github.com/cloud4-final/jammy-5th/libs/go/leasefence"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

type Sink struct {
	pool    *pgxpool.Pool
	maxPast time.Duration
	fence   *leasefence.Fence
}

func New(pool *pgxpool.Pool, maxPast time.Duration, fence *leasefence.Fence) *Sink {
	return &Sink{pool: pool, maxPast: maxPast, fence: fence}
}

func (s *Sink) Prepare(ctx context.Context) error {
	behind := pgtype.Interval{Microseconds: s.maxPast.Microseconds(), Valid: true}
	return s.fence.Transaction(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT jammy.ensure_partitions('3 hours', $1)", behind); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM jammy.processed_batches
			WHERE consumer = 'ingest' AND processed_at < now() - interval '1 day'`)
		return err
	})
}

func (s *Sink) Check(ctx context.Context) bool { return s.pool.Ping(ctx) == nil }

func (s *Sink) Store(ctx context.Context, req server.Batch) (bool, error) {
	acquire, cancel := context.WithTimeout(ctx, 2*time.Second)
	conn, err := s.pool.Acquire(acquire)
	cancel()
	if err != nil {
		return false, err
	}
	defer conn.Release()

	var duplicate bool
	err = s.fence.Transaction(ctx, conn, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO jammy.processed_batches (consumer, batch_id)
			VALUES ('ingest', $1) ON CONFLICT DO NOTHING`, req.Envelope.BatchId)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			duplicate = true
			return nil
		}

		if len(req.Result.Accepted) > 0 {
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"jammy", "raw_events"},
				[]string{"event_id", "routing_key", "profile", "schema_version", "event_time", "received_at", "ingest_id", "primary_value", "payload"},
				pgx.CopyFromSlice(len(req.Result.Accepted), func(index int) ([]any, error) {
					event := req.Result.Accepted[index]
					payload := event.Payload
					if payload == nil {
						payload = []byte{}
					}
					return []any{
						event.EventId, event.RoutingKey, event.Profile, int32(event.SchemaVersion),
						time.UnixMilli(event.EventTimeUnixMs), req.ReceivedAt, req.IngestID, req.Result.PrimaryValues[index], payload,
					}, nil
				}))
			if err != nil {
				return err
			}
		}

		if len(req.Result.Rejected) > 0 {
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"jammy", "quarantine"},
				[]string{"received_at", "ingest_id", "batch_id", "reason", "raw"},
				pgx.CopyFromSlice(len(req.Result.Rejected), func(index int) ([]any, error) {
					rejected := req.Result.Rejected[index]
					raw, err := proto.Marshal(rejected.Event)
					if err != nil {
						return nil, err
					}
					if raw == nil {
						raw = []byte{}
					}
					return []any{req.ReceivedAt, req.IngestID, req.Envelope.BatchId, rejected.Reason, raw}, nil
				}))
			return err
		}
		return nil
	})
	return duplicate, err
}
