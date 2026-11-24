from datetime import UTC, datetime, timedelta

from archiver.seal import Catalog, direct_eligible, hour_prefix, jetstream_eligible

HOUR = datetime(2026, 11, 23, 10, tzinfo=UTC)


def test_seal_written_after_watermark():
    lateness = timedelta(minutes=10)
    boundary = HOUR + timedelta(hours=1, minutes=10)
    assert not jetstream_eligible(HOUR, boundary, lateness)
    assert jetstream_eligible(HOUR, boundary + timedelta(milliseconds=1), lateness)
    assert not direct_eligible(HOUR, HOUR + timedelta(hours=1), lateness)
    assert direct_eligible(HOUR, HOUR + timedelta(hours=2), lateness)


def test_a_stream_flush_only_seals_its_committed_ingest_ids():
    from unittest.mock import Mock

    store = Mock()
    catalog = Catalog(store, timedelta(minutes=10))
    prefix = hour_prefix("synth-small", HOUR)
    for ingest_id in ("ready", "incomplete"):
        catalog.add(
            f"{prefix}{ingest_id}-1-1.parquet",
            {
                "source": "jetstream",
                "rows": 1,
                "min_event_time": HOUR.isoformat(),
                "max_event_time": HOUR.isoformat(),
            },
        )
        catalog.advance(ingest_id, (HOUR + timedelta(hours=2)).isoformat())

    catalog.seal("jetstream", ingest_ids={"ready"})

    store.put_json.assert_called_once()
    assert store.put_json.call_args.args[0] == f"{prefix}_SEALED-ready.json"
