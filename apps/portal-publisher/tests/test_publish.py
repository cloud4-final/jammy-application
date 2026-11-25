import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.error import HTTPError
from urllib.request import urlopen

import psycopg
import pytest
from botocore.exceptions import ClientError
from jammy_common.admin import AdminServer
from jammy_common.config import CommonConfig, ConfigError
from portal_publisher import main, publish
from portal_publisher.publish import Publisher, S3Operation
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="module")
def postgres():
    container = DockerContainer("postgres:17.12-trixie").with_env(
        "POSTGRES_PASSWORD", "test"
    )
    with container.with_exposed_ports(5432):
        host, port = container.get_container_host_ip(), container.get_exposed_port(5432)
        dsn = f"postgresql://postgres:test@{host}:{port}/postgres?connect_timeout=2"
        deadline = time.monotonic() + 30
        while True:
            try:
                connection = psycopg.connect(dsn)
                break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        with connection:
            root = Path(__file__).resolve().parents[3]
            connection.execute(
                (root / "contracts/db/migrations/0001_init.up.sql").read_text()
            )
        yield dsn


@pytest.fixture
def minute(postgres):
    minute = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
    with psycopg.connect(postgres) as conn:
        conn.execute("TRUNCATE jammy.assets, jammy.summary_1m")
        conn.execute(
            """
            INSERT INTO jammy.assets VALUES
                ('a','team-1','solar',10), ('b','team-2','solar',10),
                ('c','team-1','solar',10), ('old','old-team','solar',10)
            """
        )
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO jammy.summary_1m (profile,routing_key,minute,count,sum,min,max)
                VALUES ('solar',%s,%s,2,%s,1,5)
                """,
                [
                    ("a", minute, 6),
                    ("b", minute, 8),
                    ("c", minute, 10),
                    ("a", minute + timedelta(minutes=1), 90),
                    ("old", minute - timedelta(minutes=1), 6),
                    ("unknown", minute, 12),
                ],
            )
    return minute


class ObjectStore:
    def __init__(self):
        self.objects = {}
        self.fail_next = False

    def put_object(self, *, Bucket, Key, Body, ContentType):
        assert Bucket == "test-bucket"
        assert ContentType == "application/json"
        if self.fail_next:
            self.fail_next = False
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
        self.objects[Key] = json.loads(Body)


class Counter:
    def __init__(self):
        self.results = []

    def add(self, value, attributes):
        assert value == 1
        self.results.append(attributes["result"])


@pytest.mark.parametrize(
    ("name", "value"),
    [("INSTANCE_ID", ""), ("PORTAL_INTERVAL_SECONDS", "0")],
)
def test_portal_rejects_invalid_env(monkeypatch, name, value):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv(name, value)

    def unexpected_store(cfg):
        pytest.fail("Configuration must be validated before opening the S3 client")

    monkeypatch.setattr(main.s3, "client", unexpected_store)

    async def exercise():
        with pytest.raises(ConfigError, match=name):
            await main.serve(asyncio.Event(), CommonConfig.from_env())

    asyncio.run(exercise())


def test_portal_retries_errors_after_one_second(postgres, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", postgres)
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("PORTAL_INTERVAL_SECONDS", "60")

    class Store:
        def close(self):
            pass

    async def exercise():
        stop = asyncio.Event()
        attempts = []

        async def publish(self, now):
            attempts.append(time.monotonic())
            if len(attempts) == 3:
                stop.set()
            return "error"

        monkeypatch.setattr(main.s3, "client", lambda cfg: Store())
        monkeypatch.setattr(Publisher, "publish_once", publish)
        cfg = CommonConfig("site-a", "portal", 8080, 0, "info", 25, "jetstream")
        async with asyncio.timeout(4):
            await main.serve(stop, cfg)
        assert [b - a for a, b in pairwise(attempts)] == pytest.approx(
            [1, 1], abs=0.3
        )

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("start_offset_s", "publish_duration_s", "expected_delays", "minute_offsets"),
    [
        (60, 35, [25.0, 25.0], [0, 1, 2]),
        (59, 2, [0.0, 57.0], [-1, 0, 1]),
        (60, 125, [0.0, 0.0], [0, 1, 2]),
    ],
)
def test_portal_schedule_aligns_to_minute_boundaries_without_skipping(
    monkeypatch,
    start_offset_s,
    publish_duration_s,
    expected_delays,
    minute_offsets,
):
    minute = datetime(2026, 11, 23, 1, 0, tzinfo=UTC)
    expected_minutes = [
        (minute + timedelta(minutes=offset)).strftime("%Y%m%d%H%M")
        for offset in minute_offsets
    ]
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("PORTAL_INTERVAL_SECONDS", "60")

    class Store(ObjectStore):
        def close(self):
            pass

    store = Store()
    clock = [minute + timedelta(seconds=start_offset_s)]
    delays = []
    calls = 0
    original_publish = Publisher.publish_once

    class FakeDatetime:
        @staticmethod
        def now(_tz=None):
            return clock[0]

    async def timed_publish(self, now):
        nonlocal calls
        result = await original_publish(self, now)
        clock[0] += timedelta(seconds=publish_duration_s)
        calls += 1
        if calls == len(expected_minutes):
            stop.set()
        return result

    async def fake_wait_for(stop_waiter, *, timeout):
        if stop.is_set():
            return await stop_waiter
        stop_waiter.close()
        delays.append(timeout)
        clock[0] += timedelta(seconds=timeout)
        raise TimeoutError

    stop = asyncio.Event()
    rows = {"team-1": [{"asset_id": "a", "count": 2, "avg_kw": 3, "min_kw": 1, "max_kw": 5}]}
    monkeypatch.setattr(main.db, "pool", AsyncMock(return_value=AsyncMock()))
    monkeypatch.setattr(Publisher, "_load", AsyncMock(return_value=rows))
    monkeypatch.setattr(main.s3, "client", lambda _cfg: store)
    monkeypatch.setattr(main, "datetime", FakeDatetime)
    monkeypatch.setattr(Publisher, "publish_once", timed_publish)
    monkeypatch.setattr(
        main,
        "asyncio",
        SimpleNamespace(
            Event=asyncio.Event,
            TaskGroup=asyncio.TaskGroup,
            timeout=asyncio.timeout,
            to_thread=asyncio.to_thread,
            wait_for=fake_wait_for,
        ),
    )

    async def exercise():
        cfg = CommonConfig("site-a", "portal", 8080, 0, "info", 25, "jetstream")
        await main.serve(stop, cfg)

    asyncio.run(exercise())
    assert delays == pytest.approx(expected_delays)
    for suffix in expected_minutes:
        assert f"portal/team-1/1m/{suffix}.json" in store.objects


def test_publishes_per_tenant(postgres, minute):
    async def exercise():
        store, counter = ObjectStore(), Counter()
        now = minute + timedelta(minutes=1, seconds=15)
        async with AsyncConnectionPool(postgres, open=False) as pool:
            publisher = Publisher(pool, store, "test-bucket", counter)
            assert await publisher.publish_once(now) == "ok"
        suffix = minute.strftime("%Y%m%d%H%M")
        assert set(store.objects) == {
            "portal/team-1/latest.json",
            f"portal/team-1/1m/{suffix}.json",
            "portal/team-2/latest.json",
            f"portal/team-2/1m/{suffix}.json",
        }
        team_1 = store.objects["portal/team-1/latest.json"]
        assert team_1 == store.objects[f"portal/team-1/1m/{suffix}.json"]
        assert team_1 == {
            "tenant_id": "team-1",
            "minute": minute.isoformat(),
            "generated_at": now.isoformat(),
            "assets": [
                {"asset_id": "a", "count": 2, "avg_kw": 3, "min_kw": 1, "max_kw": 5},
                {"asset_id": "c", "count": 2, "avg_kw": 5, "min_kw": 1, "max_kw": 5},
            ],
        }
        assert counter.results == ["ok"]

    asyncio.run(exercise())


def test_republish_same_minute_overwrites(postgres, minute):
    async def exercise():
        store, counter = ObjectStore(), Counter()
        now = minute + timedelta(minutes=1, seconds=10)
        async with AsyncConnectionPool(postgres, open=False) as pool:
            publisher = Publisher(pool, store, "test-bucket", counter, prefix="reports")
            assert await publisher.publish_once(now) == "ok"
            original = dict(store.objects)
            assert await publisher.publish_once(now + timedelta(seconds=10)) == "ok"
        assert store.objects.keys() == original.keys()
        assert len(store.objects) == 4
        assert all(key.startswith("reports/") for key in store.objects)
        assert (
            store.objects["reports/team-1/latest.json"]
            != original["reports/team-1/latest.json"]
        )

    asyncio.run(exercise())


def test_no_data_skips(postgres, minute):
    async def exercise():
        store, counter = ObjectStore(), Counter()
        async with AsyncConnectionPool(postgres, open=False) as pool:
            publisher = Publisher(pool, store, "test-bucket", counter)
            assert (
                await publisher.publish_once(minute + timedelta(minutes=3)) == "skipped"
            )
        assert store.objects == {}
        assert counter.results == ["skipped"]

    asyncio.run(exercise())


def test_s3_failure_counts_error_and_retries(postgres, minute):
    async def exercise():
        store, counter = ObjectStore(), Counter()
        store.fail_next = True
        now = minute + timedelta(minutes=1, seconds=50)
        async with AsyncConnectionPool(postgres, open=False) as pool:
            publisher = Publisher(pool, store, "test-bucket", counter)
            assert await publisher.publish_once(now) == "error"
            assert await publisher.publish_once(now + timedelta(seconds=20)) == "ok"
        assert counter.results == ["error", "ok"]
        assert len(store.objects) == 4
        assert (
            store.objects["portal/team-1/latest.json"]["minute"] == minute.isoformat()
        )

    asyncio.run(exercise())


def test_publishes_with_s3_prefix(postgres, minute, monkeypatch):
    async def exercise():
        store, counter = ObjectStore(), Counter()
        now = minute + timedelta(minutes=1, seconds=15)
        async with AsyncConnectionPool(postgres, open=False) as pool:
            publisher = Publisher(pool, store, "test-bucket", counter, s3_prefix="/site-a/")
            assert await publisher.publish_once(now) == "ok"
        suffix = minute.strftime("%Y%m%d%H%M")
        assert set(store.objects) == {
            "site-a/portal/team-1/latest.json",
            f"site-a/portal/team-1/1m/{suffix}.json",
            "site-a/portal/team-2/latest.json",
            f"site-a/portal/team-2/1m/{suffix}.json",
        }

        store_empty_portal = ObjectStore()
        async with AsyncConnectionPool(postgres, open=False) as pool:
            publisher = Publisher(
                pool, store_empty_portal, "test-bucket", Counter(), prefix="", s3_prefix="site-a"
            )
            assert await publisher.publish_once(now) == "ok"
        assert set(store_empty_portal.objects) == {
            "site-a/team-1/latest.json",
            f"site-a/team-1/1m/{suffix}.json",
            "site-a/team-2/latest.json",
            f"site-a/team-2/1m/{suffix}.json",
        }

    asyncio.run(exercise())

    monkeypatch.setenv("DATABASE_URL", postgres)
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("S3_PREFIX", "/site-b/")

    class Store:
        def close(self):
            pass

    captured = {}

    stop = asyncio.Event()

    async def fake_publish(self, _now):
        captured["prefix"] = self.prefix
        stop.set()
        return "ok"

    monkeypatch.setattr(main.s3, "client", lambda _cfg: Store())
    monkeypatch.setattr(Publisher, "publish_once", fake_publish)

    async def serve_once():
        cfg = CommonConfig("site-a", "portal", 8080, 0, "info", 25, "jetstream")
        await main.serve(stop, cfg)

    asyncio.run(serve_once())
    assert captured["prefix"] == "site-b/portal"


def test_s3_ready_bounds_stalled_bucket_check():
    assert main.S3_READY_TIMEOUT_SECONDS > 0
    release = Event()

    class StalledStore:
        def __init__(self):
            self.stalled = True
            self.buckets = []

        def head_bucket(self, *, Bucket):
            self.buckets.append(Bucket)
            if self.stalled:
                release.wait(5)
            return {}

    def get_status(port: int) -> int:
        try:
            with urlopen(f"http://127.0.0.1:{port}/readyz", timeout=1) as resp:
                return resp.status
        except HTTPError as error:
            return error.code

    async def exercise():
        store = StalledStore()
        operation = S3Operation()
        admin = AdminServer(
            0, {"s3": lambda: main.s3_ready(operation, store, "test-bucket", timeout=0.05)}
        )
        await admin.start()
        try:
            async with asyncio.timeout(1):
                assert await asyncio.to_thread(get_status, admin.port) == 503
                assert await asyncio.to_thread(get_status, admin.port) == 503
                assert store.buckets == ["test-bucket"]
                store.stalled = False
                release.set()
                assert await asyncio.to_thread(get_status, admin.port) == 200
        finally:
            release.set()
            await admin.close()
        assert store.buckets == ["test-bucket", "test-bucket"]

    asyncio.run(exercise())


def test_s3_upload_timeout_counts_error_and_retries(monkeypatch):
    assert publish.S3_UPLOAD_TIMEOUT_SECONDS > 0
    monkeypatch.setattr(publish, "S3_UPLOAD_TIMEOUT_SECONDS", 0.05)
    release = Event()

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        async def execute(self, sql, params):
            self.params = params

        async def fetchall(self):
            return [dict(row) for row in self._rows]

    class FakeConnection:
        def __init__(self, rows):
            self._rows = rows

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        def cursor(self, **_kwargs):
            return FakeCursor(self._rows)

    class FakePool:
        def __init__(self, rows):
            self._rows = rows

        def connection(self, **_kwargs):
            return FakeConnection(self._rows)

    class StallingStore(ObjectStore):
        def __init__(self):
            super().__init__()
            self.stall_next = True
            self.put_calls = []

        def put_object(self, *, Bucket, Key, Body, ContentType):
            self.put_calls.append(Key)
            if self.stall_next:
                release.wait(5)
            super().put_object(Bucket=Bucket, Key=Key, Body=Body, ContentType=ContentType)

    async def exercise():
        minute = datetime(2026, 11, 23, 1, 0, tzinfo=UTC)
        now = minute + timedelta(minutes=1, seconds=10)
        rows = [
            {
                "tenant_id": "team-1",
                "asset_id": "a",
                "count": 2,
                "avg_kw": 3,
                "min_kw": 1,
                "max_kw": 5,
            }
        ]
        store, counter = StallingStore(), Counter()
        publisher = Publisher(FakePool(rows), store, "test-bucket", counter)
        try:
            async with asyncio.timeout(1):
                assert await publisher.publish_once(now) == "error"
                assert await publisher.publish_once(now) == "error"
            assert len(store.put_calls) == 1
            assert publisher._retry_minute == minute
            assert store.objects == {}

            store.stall_next = False
            release.set()
            assert await publisher.publish_once(now + timedelta(seconds=20)) == "ok"
            assert publisher._retry_minute is None
            assert counter.results == ["error", "error", "ok"]
            assert (
                store.objects["portal/team-1/latest.json"]["minute"]
                == minute.isoformat()
            )
        finally:
            release.set()

    asyncio.run(exercise())
