import asyncio
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import psycopg
import pytest
from jammy_common import db
from jammy_common.config import CommonConfig
from jammy_common.write_fence import Fence, Fenced
from processor.anomaly import AnomalyDetector
from processor.source_db import DbSource
from testcontainers.postgres import PostgresContainer

MIGRATIONS = Path(__file__).resolve().parents[3] / "contracts/db/migrations"


@contextmanager
def database():
    with PostgresContainer("postgres:17.12-trixie", driver=None) as container:
        dsn = container.get_connection_url()
        with psycopg.connect(dsn) as conn:
            for migration in sorted(MIGRATIONS.glob("*.up.sql")):
                conn.execute(migration.read_text())
        yield dsn


def until(check, *, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.1)
    raise AssertionError("Condition did not become true before the deadline")


@contextmanager
def running_processor(dsn, **options):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = os.environ | {
        "SITE_ID": "fixture",
        "PIPELINE_MODE": "direct-db",
        "DATABASE_URL": dsn,
        "ADMIN_PORT": str(port),
        **options,
    }
    with subprocess.Popen([sys.executable, "-m", "processor.main"], env=env) as process:
        try:

            def ready():
                if process.poll() is not None:
                    raise AssertionError(f"Processor exited with {process.returncode}")
                try:
                    return (
                        httpx.get(
                            f"http://127.0.0.1:{port}/readyz", timeout=0.5
                        ).status_code
                        == 200
                    )
                except httpx.TransportError:
                    return False

            until(ready)
            yield process, port
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=26)


def summary_rows(dsn):
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT coalesce(sum(count), 0) FROM jammy.summary_1m"
        ).fetchone()[0]


def insert_raw(conn, event_id, received):
    conn.execute(
        """INSERT INTO jammy.raw_events
           (event_id, routing_key, profile, schema_version, event_time, received_at,
            ingest_id, primary_value, payload)
           VALUES (%s, 'asset', 'synth-small', 1, %s, %s, 'fixture', 4, ''::bytea)""",
        (event_id, received, received),
    )


def test_db_mode_cursor_progress_exact():
    with database() as dsn:
        stamp = datetime.now(UTC)
        for index in range(3):
            with psycopg.connect(dsn) as conn:
                insert_raw(conn, f"event-{index}", stamp)
            with running_processor(dsn, FETCH_BATCH="1"):
                until(lambda expected=index + 1: summary_rows(dsn) == expected)
        assert summary_rows(dsn) == 3


def test_db_mode_late_commit_not_skipped():
    with database() as dsn, running_processor(dsn, FETCH_BATCH="1"):
        now = datetime.now(UTC)
        with psycopg.connect(dsn) as first:
            insert_raw(first, "early-commit-last", now)
            with psycopg.connect(dsn) as second:
                insert_raw(second, "received-later", now + timedelta(milliseconds=1))
            time.sleep(1)
            assert summary_rows(dsn) == 0
        until(lambda: summary_rows(dsn) == 2)
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                "SELECT last_event_id FROM jammy.consumer_cursor WHERE consumer = 'processor'"
            ).fetchone() == ("received-later",)


def test_db_poll_waits_for_fresh_local_lease(monkeypatch):
    with database() as dsn:
        monkeypatch.setenv("SITE_ID", "fixture")
        monkeypatch.setenv("DATABASE_URL", dsn)
        with psycopg.connect(dsn) as conn:
            insert_raw(conn, "fenced-event", datetime.now(UTC))
            conn.execute(
                """INSERT INTO jammy.site_lease_state
                   VALUES ('fixture', true, 1, false, now() - interval '11 seconds')"""
            )

        async def exercise():
            tick = 0.0
            pool = await db.pool(CommonConfig.from_env(), workers=1)
            try:
                source = DbSource(
                    pool, consumer="processor", batch_size=1, detector=AnomalyDetector(0.5),
                    fence=Fence("fixture", 10, clock=lambda: tick),
                )
                with pytest.raises(Fenced):
                    await source.poll()
                assert summary_rows(dsn) == 0
                async with pool.connection() as conn:
                    await conn.execute("UPDATE jammy.site_lease_state SET renewed_at = now()")
                tick += 1
                readings, _ = await source.poll()
                assert len(readings) == 1
                assert summary_rows(dsn) == 1
            finally:
                await pool.close()

        asyncio.run(exercise())


def test_processed_batches_pruned_after_1d():
    with database() as dsn:
        with psycopg.connect(dsn) as conn:
            conn.execute(
                """INSERT INTO jammy.processed_batches VALUES
                   ('processor', 'old', now() - interval '2 days'),
                   ('processor', 'fresh', now()),
                   ('ingest', 'old', now() - interval '2 days')"""
            )
        with running_processor(dsn), psycopg.connect(dsn) as conn:
            assert conn.execute(
                "SELECT consumer, batch_id FROM jammy.processed_batches ORDER BY consumer"
            ).fetchall() == [("ingest", "old"), ("processor", "fresh")]


def test_database_poll_metrics_record_last_row_lag_and_reset_on_idle(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock, Mock, call

    from processor.main import Processor
    from processor.source_db import DbSource

    now = datetime(2026, 11, 23, tzinfo=UTC)
    clock = Mock()
    clock.now.return_value = now
    monkeypatch.setattr("processor.source_db.datetime", clock)

    async def run():
        stop = asyncio.Event()

        async def pause(seconds):
            if seconds == 0.2:
                stop.set()

        monkeypatch.setattr("processor.main.asyncio.sleep", pause)
        pool = MagicMock()
        conn = pool.connection.return_value.__aenter__.return_value
        cursor = SimpleNamespace(fetchone=AsyncMock(return_value=("0", "")))
        rows = [
            ("1", "a", "solar", "a", now, 1, now - timedelta(seconds=10)),
            ("2", "b", "solar", "b", now, 2, now - timedelta(seconds=4)),
        ]
        conn.execute = AsyncMock(
            side_effect=[
                psycopg.OperationalError,
                None,
                cursor,
                SimpleNamespace(fetchall=AsyncMock(return_value=rows)),
                None,
                None,
                cursor,
                SimpleNamespace(fetchall=AsyncMock(return_value=[])),
            ]
        )
        monkeypatch.setattr("processor.source_db.upsert", AsyncMock())
        detector = SimpleNamespace(write=AsyncMock())
        source = DbSource(pool, consumer="processor", batch_size=2, detector=detector)
        meter = Mock()
        meter.create_gauge.side_effect = [Mock(), Mock()]
        processor = Processor(SimpleNamespace(drop_ratio=0.5), pool, meter)
        processor.database_ready.set()

        await processor.poll_database(stop, source)

        assert processor.lag.set.call_args_list == [call(4), call(0)]
        assert processor.batches.add.call_args_list == [
            call(1, {"result": "error"}),
            call(1, {"result": "ok"}),
        ]

    asyncio.run(run())
