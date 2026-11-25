\set ON_ERROR_STOP on

BEGIN;
SET LOCAL TIME ZONE 'Pacific/Auckland';

DO $$
DECLARE
    utc_hour timestamp := date_trunc('hour', now() AT TIME ZONE 'UTC');
    initial_partition text := 'raw_events_' || to_char(utc_hour, 'YYYYMMDDHH24');
    claim_batch_id text;
    fixture_batch_ids text[] := ARRAY['fixture-batch-1', 'fixture-batch-1', 'fixture-batch-2'];
    received timestamptz := now();
BEGIN
    PERFORM jammy.ensure_partitions('3 hours', '24 hours');
    IF jammy.ensure_partitions('3 hours', '24 hours') <> 0 THEN
        RAISE EXCEPTION 'ensure_partitions should be idempotent after migration';
    END IF;

    FOREACH claim_batch_id IN ARRAY fixture_batch_ids LOOP
        INSERT INTO jammy.processed_batches (consumer, batch_id)
        VALUES ('ingest', claim_batch_id)
        ON CONFLICT (consumer, batch_id) DO NOTHING;

        IF FOUND THEN
            INSERT INTO jammy.raw_events
                (event_id, routing_key, profile, schema_version, event_time,
                 received_at, ingest_id, primary_value, payload)
            VALUES ('fixture-event', 'fixture-solar', 'solar', 1, now(),
                    received, 'fixture-ingest', 12.5, ''::bytea);

            INSERT INTO jammy.quarantine (ingest_id, batch_id, reason, raw)
            VALUES ('fixture-ingest', claim_batch_id, 'bad_event_id', ''::bytea);
        END IF;

        received := received + interval '1 microsecond';
    END LOOP;

    IF (SELECT count(*) FROM jammy.raw_events WHERE event_id = 'fixture-event') <> 2
       OR (SELECT count(*) FROM jammy.quarantine WHERE ingest_id = 'fixture-ingest') <> 2
       OR (SELECT count(*) FROM jammy.processed_batches WHERE consumer = 'ingest') <> 2
    THEN
        RAISE EXCEPTION 'a repeated batch should add no rows, a distinct batch should add both';
    END IF;

    IF NOT EXISTS (
        SELECT FROM jammy.raw_events
        WHERE received_at = now()
          AND tableoid = to_regclass(format('jammy.%I', initial_partition))
    ) THEN
        RAISE EXCEPTION 'raw_events did not route by the UTC hour';
    END IF;

    INSERT INTO jammy.processed_batches (consumer, batch_id)
    VALUES ('processor', 'fixture-batch-1')
    ON CONFLICT (consumer, batch_id) DO NOTHING;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'processor and ingest claims should be independent';
    END IF;

    BEGIN
        INSERT INTO jammy.raw_events
            (event_id, routing_key, profile, schema_version, event_time,
             received_at, ingest_id, primary_value, payload)
        VALUES ('fixture-outside', 'fixture-solar', 'solar', 1, now(),
                now() - interval '1 day', 'fixture-ingest', 12.5, ''::bytea);

        RAISE EXCEPTION 'raw_events must reject timestamps outside its partitions';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END;
$$;

ROLLBACK;
