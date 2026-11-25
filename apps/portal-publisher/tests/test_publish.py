import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import psycopg
import pytest
from botocore.exceptions import ClientError
from jammy_common.config import CommonConfig, ConfigError
from portal_publisher import main
from portal_publisher.publish import Publisher
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
