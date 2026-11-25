import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import psycopg
import pytest
from archiver.export_db import DbArchive, PreparedExport
from archiver.parquet_writer import ObjectStore, timestamp
from archiver.seal import Catalog
from psycopg import sql
from psycopg.errors import LockNotAvailable
from psycopg_pool import AsyncConnectionPool
from test_archiver_jetstream import (  # noqa: F401 - pytest fixtures
    _s3_fixture,
    _seaweed_endpoint,
    archiver,
    get,
    rows,
    until,
)
from testcontainers.postgres import PostgresContainer

MIGRATIONS = Path(__file__).resolve().parents[3] / "contracts/db/migrations"


def test_db_export_sets_lock_timeout_before_locking():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    async def run():
        conn = SimpleNamespace(
            execute=AsyncMock(side_effect=[None, LockNotAvailable]),
        )
        store = SimpleNamespace(put_json=Mock(return_value=b"{}"))
        archive = DbArchive(None, store, None)
        name = "raw_events_2026112300"
        prepared = PreparedExport(name, (1, "0", "", 0), [], "2026-11-23T00:00:00Z")
        with pytest.raises(LockNotAvailable):
            await archive._finalize(conn, sql.Identifier("jammy", name), prepared)
        assert [call.args[0] for call in conn.execute.await_args_list] == [
            "SET LOCAL lock_timeout = '2s'",
            "LOCK TABLE ONLY jammy.raw_events IN ACCESS EXCLUSIVE MODE",
        ]

    asyncio.run(run())


def test_db_export_prepends_s3_prefix_to_manifest_key():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    async def run():
        conn = SimpleNamespace(
            execute=AsyncMock(side_effect=[None, LockNotAvailable]),
        )
        store = SimpleNamespace(put_json=Mock(return_value=b"{}"))
        archive = DbArchive(None, store, None, s3_prefix="/site-a/")
        name = "raw_events_2026112300"
        prepared = PreparedExport(name, (1, "0", "", 0), [], "2026-11-23T00:00:00Z")
        with pytest.raises(LockNotAvailable):
            await archive._finalize(conn, sql.Identifier("jammy", name), prepared)
        assert (
            store.put_json.call_args.args[0]
            == "site-a/exports/direct-db/raw_events_2026112300.json"
        )

    asyncio.run(run())


@pytest.mark.parametrize("lock_timeout", [False, True])
def test_db_mode_export_then_drop_partition(tmp_path, s3_fixture, lock_timeout):
    client, bucket, options = s3_fixture
    container = PostgresContainer("postgres:17.12-trixie", driver=None)
    with container:
        dsn = container.get_connection_url()
        hour = (datetime.now(UTC) - timedelta(hours=3)).replace(
            minute=0, second=0, microsecond=0
        )
        name = hour.strftime("raw_events_%Y%m%d%H")
        with psycopg.connect(dsn) as conn:
            for migration in sorted(MIGRATIONS.glob("*.up.sql")):
                conn.execute(migration.read_text())
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {} PARTITION OF jammy.raw_events FOR VALUES FROM ({}) TO ({})"
                ).format(
                    sql.Identifier("jammy", name),
                    sql.Literal(hour),
                    sql.Literal(hour + timedelta(hours=1)),
                )
            )
            conn.execute(
                """INSERT INTO jammy.raw_events
                   (event_id, routing_key, profile, schema_version, event_time, received_at,
                    ingest_id, primary_value, payload)
                   VALUES ('db-event', 'asset', 'synth-small', 1, %s, %s, 'fixture', 7, ''::bytea)""",
                (
                    hour,
                    hour,
                ),
            )
            conn.execute(
                """INSERT INTO jammy.consumer_cursor (consumer, last_xid, last_event_id) VALUES
                   ('processor', pg_current_xact_id(), 'db-event')"""
            )

        def dropped():
            with psycopg.connect(dsn) as conn:
                return conn.execute(
                    """SELECT s3_key, rows, sha256 FROM jammy.archive_exports
                       WHERE partition_name = %s AND dropped_at IS NOT NULL""",
                    (name,),
                ).fetchone()

        if lock_timeout:

            async def retry():
                storage_client = Mock(wraps=client)
                store = ObjectStore(storage_client, bucket, tmp_path, 1, 0, Mock())
                store.stage = Mock(wraps=store.stage)
                async with AsyncConnectionPool(dsn, min_size=1) as pool:
                    archive = DbArchive(pool, store, Catalog(store, timedelta(0)))
                    with psycopg.connect(dsn) as blocker:
                        blocker.execute(
                            sql.SQL("LOCK TABLE ONLY {} IN ACCESS SHARE MODE").format(
                                sql.Identifier("jammy", name)
                            )
                        )
                        with pytest.raises(LockNotAvailable):
                            await archive.poll()
                        assert dropped() is None
                    assert await archive.poll()
                store.stage.assert_called_once()
                storage_client.upload_file.assert_called_once()
                data_key = storage_client.upload_file.call_args.args[2]
                parquet_reads = [
                    call
                    for call in storage_client.get_object.call_args_list
                    if call.kwargs["Key"] == data_key
                ]
                assert len(parquet_reads) == 2
                assert not list(tmp_path.iterdir())

            asyncio.run(retry())
            manifest_key, count, sha = dropped()
        else:
            with archiver(
                tmp_path,
                **(options | {"PIPELINE_MODE": "direct-db", "DATABASE_URL": dsn}),
            ):
                manifest_key, count, sha = until(dropped, timeout=60)
        with psycopg.connect(dsn) as conn:
            assert conn.execute(
                "SELECT to_regclass(%s)", (f"jammy.{name}",)
            ).fetchone() == (None,)
        body = get(client, bucket, manifest_key)
        assert count == 1
        assert json.loads(body)["rows"] == count
        assert hashlib.sha256(body).hexdigest() == sha
        assert rows(client, bucket) == ["db-event"]


def test_direct_db_seal_uses_exported_hours_and_routes_late_rows(tmp_path, s3_fixture):
    from archiver.seal import hour_prefix

    client, bucket, options = s3_fixture
    event_hour = datetime.now(UTC).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=4)
    second = event_hour + timedelta(hours=1)
    third = event_hour + timedelta(hours=2)
    prefix = hour_prefix("synth-small", event_hour)
    second_prefix = hour_prefix("solar", second)
    seal_key = f"{prefix}_SEALED-db.json"

    with PostgresContainer("postgres:17.12-trixie", driver=None) as container:
        dsn = container.get_connection_url()

        def partition(conn, hour):
            name = hour.strftime("raw_events_%Y%m%d%H")
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {} PARTITION OF jammy.raw_events FOR VALUES FROM ({}) TO ({})"
                ).format(
                    sql.Identifier("jammy", name),
                    sql.Literal(hour),
                    sql.Literal(hour + timedelta(hours=1)),
                )
            )
            return name

        def insert(conn, event_id, stamp, received, profile):
            conn.execute(
                """INSERT INTO jammy.raw_events
                   (event_id, routing_key, profile, schema_version, event_time, received_at,
                    ingest_id, primary_value, payload)
                   VALUES (%s, 'asset', %s, 1, %s, %s, 'fixture', 7, ''::bytea)""",
                (event_id, profile, stamp, received),
            )
            conn.execute(
                """INSERT INTO jammy.consumer_cursor (consumer, last_xid, last_event_id)
                   VALUES ('processor', pg_current_xact_id(), %s)
                   ON CONFLICT (consumer) DO UPDATE SET last_xid = EXCLUDED.last_xid,
                     last_event_id = EXCLUDED.last_event_id""",
                (event_id,),
            )

        def dropped(name):
            with psycopg.connect(dsn) as conn:
                return conn.execute(
                    "SELECT dropped_at FROM jammy.archive_exports WHERE partition_name = %s",
                    (name,),
                ).fetchone()

        with psycopg.connect(dsn) as conn:
            for migration in sorted(MIGRATIONS.glob("*.up.sql")):
                conn.execute(migration.read_text())
            name = partition(conn, event_hour)
            insert(conn, "early", event_hour, event_hour, "synth-small")
            insert(conn, "second-group", second, event_hour, "solar")

        with archiver(
            tmp_path, **(options | {"PIPELINE_MODE": "direct-db", "DATABASE_URL": dsn})
        ):
            until(lambda: dropped(name))
            partition_manifest = json.loads(
                get(client, bucket, f"exports/direct-db/{name}.json")
            )
            assert {item["key"] for item in partition_manifest["files"]} == {
                f"{prefix}db-{name}.parquet",
                f"{second_prefix}db-{name}.parquet",
            }
            assert (
                client.list_objects_v2(Bucket=bucket, Prefix=seal_key)["KeyCount"] == 0
            )

            with psycopg.connect(dsn) as conn:
                next_name = partition(conn, second)
            until(lambda: dropped(next_name))

            def read_seal():
                objects = client.list_objects_v2(Bucket=bucket, Prefix=seal_key)
                return (
                    get(client, bucket, seal_key) if objects.get("KeyCount") else None
                )

            original = until(read_seal)
            seal = json.loads(original)
            assert seal["watermark"] == timestamp(third)
            assert seal["rows"] == 1

            with psycopg.connect(dsn) as conn:
                late_name = partition(conn, third)
                insert(conn, "late", event_hour, third, "synth-small")
            until(lambda: dropped(late_name))
            assert rows(client, bucket, "raw-late/") == ["late"]
            assert get(client, bucket, seal_key) == original
