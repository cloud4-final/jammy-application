ALTER TABLE jammy.raw_events
    ADD COLUMN ingest_xid xid8 NOT NULL DEFAULT pg_current_xact_id();

-- A parent index creates indexes on current partitions and on partitions
-- subsequently attached by jammy.ensure_partitions.
CREATE INDEX raw_events_ingest_cursor_idx ON jammy.raw_events (ingest_xid, event_id);

ALTER TABLE jammy.consumer_cursor
    ADD COLUMN last_xid xid8 NOT NULL DEFAULT '0'::xid8,
    DROP COLUMN last_received_at;

ALTER TABLE jammy.consumer_cursor ALTER COLUMN last_xid DROP DEFAULT;
