package sourcedb

import (
	"context"
	"time"

	"github.com/cloud4-final/jammy-application/apps/processor-go/internal/aggregate"
	"github.com/cloud4-final/jammy-application/apps/processor-go/internal/state"
	"github.com/jackc/pgx/v5"
)

// Qualify the sort column so PostgreSQL uses the xid8 cursor index and
// orders the native column instead of the text expression in the projection.
const cursorQuery = `
	SELECT ingest_xid::text, event_id, profile, routing_key, event_time, primary_value, received_at
	FROM jammy.raw_events
	WHERE (ingest_xid, event_id) > ($1::text::xid8, $2)
	  AND ingest_xid < pg_snapshot_xmin(pg_current_snapshot())
	ORDER BY raw_events.ingest_xid, event_id LIMIT $3`

func Poll(ctx context.Context, store *state.Store, limit int) (int, time.Time, error) {
	var events []aggregate.Event
	var receivedAt time.Time
	err := store.Write(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO jammy.consumer_cursor (consumer, last_xid, last_event_id)
			VALUES ($1, '0', '') ON CONFLICT DO NOTHING`, store.Consumer)
		if err != nil {
			return err
		}
		var xid, eventID string
		err = tx.QueryRow(ctx, `SELECT last_xid::text, last_event_id
			FROM jammy.consumer_cursor WHERE consumer = $1 FOR UPDATE`, store.Consumer).Scan(&xid, &eventID)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, cursorQuery, xid, eventID, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e aggregate.Event
			if err := rows.Scan(&xid, &eventID, &e.Profile, &e.RoutingKey, &e.Time, &e.Value, &receivedAt); err != nil {
				return err
			}
			events = append(events, e)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(events) > 0 {
			if err := store.Apply(ctx, tx, events); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE jammy.consumer_cursor SET last_xid = $2::text::xid8, last_event_id = $3
				WHERE consumer=$1`, store.Consumer, xid, eventID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, time.Time{}, err
	}
	store.CoalesceLatest(events)
	return len(events), receivedAt, nil
}
