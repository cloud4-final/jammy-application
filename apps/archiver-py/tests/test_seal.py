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


def test_catalog_with_s3_prefix_routes_raw_late_and_seals():
    from archiver.parquet_writer import manifest_key
    from archiver.seal import late_prefix

    for raw_prefix in ("/site-a/", "raw"):
        s3_prefix = f"{raw_prefix.strip('/')}/"
        prefix = hour_prefix("synth-small", HOUR, s3_prefix=s3_prefix)
        parquet_key = f"{prefix}ready-1-1.parquet"
        manifest = {
            "manifest_version": 1,
            "source": "jetstream",
            "stream": "raw-test",
            "first_seq": 1,
            "last_seq": 1,
            "sequence_ranges": [[1, 1]],
            "schema_version": 1,
            "rows": 2,
            "min_event_time": HOUR.isoformat(),
            "max_event_time": (HOUR + timedelta(hours=2)).isoformat(),
        }
        objects = {parquet_key: {}, manifest_key(parquet_key): manifest}

        class Store:
            def __init__(self, data):
                self.data = data

            def list(self, root):
                return [k for k in self.data if k.startswith(root)]

            def read_json(self, key):
                return self.data[key]

            def put_json(self, key, data):
                self.data[key] = data

        catalog = Catalog(Store(objects), timedelta(minutes=10), s3_prefix=raw_prefix)
        catalog.restore()
        assert catalog.covered(prefix, "ready", "raw-test", 1, 1)
        catalog.seal("jetstream")

        seal_key = f"{prefix}_SEALED-ready.json"
        assert seal_key in objects
        assert objects[seal_key]["files"] == [parquet_key]
        assert catalog.target(prefix, "ready") == late_prefix(prefix, s3_prefix)
        assert (
            catalog.target(prefix, "ready")
            == f"{s3_prefix}raw-late/profile=synth-small/dt=2026-11-23/hour=10/"
        )
