import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pyarrow.parquet as pq
import pytest
from archiver.parquet_writer import ArchiveRow, ObjectStore, Stage, manifest_key
from test_archiver_jetstream import (  # noqa: F401 - pytest fixtures
    _s3_fixture,
    _seaweed_endpoint,
    get,
)

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


@pytest.mark.parametrize("same_content", [True, False])
def test_commit_preserves_existing_parquet(tmp_path, s3_fixture, same_content):
    client, bucket, _ = s3_fixture
    storage_client = Mock(wraps=client)
    store = ObjectStore(storage_client, bucket, tmp_path, 1, 0, Mock())
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
    original_parquet = get(client, bucket, key)
    original_manifest = get(client, bucket, manifest_key(key))

    with store.stage() as stage:
        stage.write([row() if same_content else row("different")])
        if same_content:
            client.delete_object(Bucket=bucket, Key=manifest_key(key))
            assert store.commit(stage, key, **identity) == original
        else:
            with pytest.raises(OSError, match="checksum differs"):
                store.commit(stage, key, **identity)
    storage_client.upload_file.assert_called_once()
    assert get(client, bucket, key) == original_parquet
    assert get(client, bucket, manifest_key(key)) == original_manifest
