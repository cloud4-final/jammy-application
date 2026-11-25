import io
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pyarrow.parquet as pq
import pytest
from archiver.parquet_writer import ArchiveRow, ObjectStore, Stage, manifest_key
from botocore.response import StreamingBody

FIXTURE = Path(__file__).resolve().parents[3] / "contracts/testdata/parquet_schema.json"
STAMP = datetime(2026, 11, 23, 10, 15, tzinfo=UTC)


def row(event_id="event-1", event_time=STAMP):
    return ArchiveRow(
        event_id,
        "asset",
        "synth-small",
        1,
        event_time,
        STAMP,
        "fixture",
        12.5,
        b"payload",
    )


def test_parquet_schema_matches_contract(tmp_path):
    with Stage(tmp_path, 1) as stage:
        stage.write([row()])
        stage.finish()
        parquet = pq.ParquetFile(stage.path)
        reference = json.loads(FIXTURE.read_text())
        assert [
            {
                "name": field.name,
                "type": str(field.type),
                "required": not field.nullable,
            }
            for field in parquet.schema_arrow
        ] == [dict(column, required=True) for column in reference["columns"]]
        assert all(
            parquet.schema.column(i).max_definition_level == 0
            for i in range(parquet.metadata.num_columns)
        )
        assert {
            parquet.metadata.row_group(0).column(i).compression.lower()
            for i in range(parquet.metadata.num_columns)
        } == {reference["compression"]}
        assert parquet.read()["event_id"].to_pylist() == ["event-1"]


def test_manifest_rows_and_sha256(tmp_path):
    import hashlib

    with Stage(tmp_path, 1) as stage:
        stage.write([row(), row("event-2")])
        stage.finish()
        manifest = stage.manifest(
            source="jetstream",
            stream="raw-test",
            first_seq=11,
            last_seq=12,
            partition_name=None,
        )
        assert manifest["rows"] == 2
        assert manifest["sha256"] == hashlib.sha256(stage.path.read_bytes()).hexdigest()
        assert manifest["schema_version"] == manifest["manifest_version"] == 1
        assert manifest["min_event_time"] == "2026-11-23T10:15:00.000Z"
        assert manifest["max_event_time"] == "2026-11-23T10:15:00.000Z"


def test_commit_overwrites_and_verifies_parquet(tmp_path):
    objects = {}

    def upload_file(filename, _bucket, key, **_options):
        objects[key] = Path(filename).read_bytes()

    def get_object(Bucket, Key):
        data = objects[Key]
        return {"Body": StreamingBody(io.BytesIO(data), len(data))}

    def put_object(Bucket, Key, Body, **_options):
        objects[Key] = Body

    storage_client = Mock(
        **{
            "upload_file.side_effect": upload_file,
            "get_object.side_effect": get_object,
            "put_object.side_effect": put_object,
        }
    )
    store = ObjectStore(storage_client, "archive-bucket", tmp_path, 1, 0, Mock())
    key = "raw/archive.parquet"
    identity = {
        "source": "direct-db",
        "stream": None,
        "first_seq": None,
        "last_seq": None,
        "partition_name": "raw_events_2026112310",
    }
    with store.stage() as stage:
        stage.write([row()])
        original = store.commit(stage, key, **identity)
    with store.stage() as stage:
        stage.write([row("different"), row("another")])
        replacement = store.commit(stage, key, **identity)
    assert replacement["sha256"] != original["sha256"]
    assert replacement["rows"] == 2
    assert storage_client.upload_file.call_count == 2
    assert storage_client.get_object.call_count == 2
    for upload_call in storage_client.upload_file.call_args_list:
        config = upload_call.kwargs["Config"]
        assert config.multipart_threshold == 64 * 1024**2
        assert config.multipart_chunksize == 64 * 1024**2
    assert json.loads(objects[manifest_key(key)]) == replacement

    storage_client.upload_file = Mock()
    with store.stage() as stage:
        stage.write([row("failed-upload")])
        with pytest.raises(OSError, match="checksum differs"):
            store.commit(stage, key, **identity)
    assert json.loads(objects[manifest_key(key)]) == replacement


def test_multipart_part_size_scales_above_10000_parts():
    import math

    from archiver.ratelimit import MIN_PART_SIZE, multipart_part_size, upload_config

    assert multipart_part_size(0) == MIN_PART_SIZE
    assert multipart_part_size(64 * 1024**2) == MIN_PART_SIZE
    assert multipart_part_size(500 * 1024**3) == MIN_PART_SIZE
    huge = 1000 * 1024**3
    expected = math.ceil(huge / 10_000)
    assert expected > MIN_PART_SIZE
    assert multipart_part_size(huge) == expected
    cfg = upload_config(1024, huge)
    assert cfg.multipart_threshold == MIN_PART_SIZE
    assert cfg.multipart_chunksize == expected
    assert cfg.max_bandwidth == 1024


def test_commit_validates_staged_parquet_before_upload(tmp_path):
    import pyarrow as pa

    storage_client = Mock()
    store = ObjectStore(storage_client, "archive-bucket", tmp_path, 1, 0, Mock())
    identity = {
        "source": "direct-db",
        "stream": None,
        "first_seq": None,
        "last_seq": None,
        "partition_name": "raw_events_2026112310",
    }

    # Corrupt staged file fails before upload.
    with store.stage() as stage:
        stage.write([row()])
        stage.finish()
        stage.path.write_bytes(b"corrupt")
        with pytest.raises(OSError, match="Invalid Parquet"):
            store.commit(stage, "raw/corrupt.parquet", **identity)

    # Wrong schema fails before upload.
    with store.stage() as stage:
        stage.write([row()])
        stage.finish()
        pq.write_table(pa.table({"event_id": ["e1"]}), stage.path, compression="zstd")
        with pytest.raises(OSError, match="schema"):
            store.commit(stage, "raw/wrong-schema.parquet", **identity)

    # Wrong compression fails before upload.
    with store.stage() as stage:
        from archiver.parquet_writer import SCHEMA

        stage.write([row()])
        stage.finish()
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "event_id": "e1",
                        "routing_key": "asset",
                        "profile": "synth-small",
                        "schema_version": 1,
                        "event_time": STAMP,
                        "received_at": STAMP,
                        "ingest_id": "fixture",
                        "primary_value": 12.5,
                        "payload": b"payload",
                    }
                ],
                schema=SCHEMA,
            ),
            stage.path,
            compression="none",
        )
        with pytest.raises(OSError, match="compression"):
            store.commit(stage, "raw/wrong-codec.parquet", **identity)

    # Row count mismatch fails before upload.
    with store.stage() as stage:
        stage.write([row("r1"), row("r2")])
        stage.finish()
        stage.rows = 1
        with pytest.raises(OSError, match="row count"):
            store.commit(stage, "raw/wrong-rows.parquet", **identity)

    storage_client.upload_file.assert_not_called()
    storage_client.put_object.assert_not_called()
