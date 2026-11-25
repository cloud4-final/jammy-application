ALTER TABLE jammy.consumer_cursor
    ADD COLUMN last_received_at timestamptz NOT NULL DEFAULT '-infinity'::timestamptz,
    DROP COLUMN last_xid;

ALTER TABLE jammy.consumer_cursor ALTER COLUMN last_received_at DROP DEFAULT;

DROP INDEX jammy.raw_events_ingest_cursor_idx;
ALTER TABLE jammy.raw_events DROP COLUMN ingest_xid;
