import asyncio
import time
from pathlib import Path

import psycopg
import pytest
from dispatcher.leader import Leader
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="module")
def dsn():
    container = DockerContainer("postgres:17.12-trixie").with_env(
        "POSTGRES_PASSWORD", "test"
    )
    with container.with_exposed_ports(5432):
        uri = f"postgresql://postgres:test@{container.get_container_host_ip()}:{container.get_exposed_port(5432)}/postgres?connect_timeout=2"
        end = time.monotonic() + 120
        while True:
            try:
                with psycopg.connect(uri):
                    break
            except psycopg.OperationalError:
                if time.monotonic() > end:
                    raise
                time.sleep(0.25)
        with psycopg.connect(uri) as conn:
            root = Path(__file__).resolve().parents[3]
            conn.execute(
                (root / "contracts/db/migrations/0001_init.up.sql").read_text()
            )
        yield uri


def test_only_one_leader(dsn):
    async def exercise():
        async with (
            AsyncConnectionPool(dsn, open=False) as one,
            AsyncConnectionPool(dsn, open=False) as two,
        ):
            leaders = [Leader(one, holder="one"), Leader(two, holder="two")]
            values = await asyncio.gather(*(item.try_acquire() for item in leaders))
            try:
                assert sum(value is not None for value in values) == 1
            finally:
                await asyncio.gather(*(item.release() for item in leaders))

    asyncio.run(exercise())


def test_generation_increments_on_takeover(dsn):
    async def exercise():
        async with (
            AsyncConnectionPool(dsn, open=False) as one,
            AsyncConnectionPool(dsn, open=False) as two,
        ):
            first, second = Leader(one, holder="first"), Leader(two, holder="second")
            initial = await first.try_acquire()
            assert initial is not None
            await first.release()
            try:
                assert await second.try_acquire() == initial + 1
            finally:
                await second.release()

    asyncio.run(exercise())


def test_release_unlocks_before_close(dsn, monkeypatch):
    async def exercise():
        async with (
            AsyncConnectionPool(dsn, open=False) as one,
            AsyncConnectionPool(dsn, open=False) as two,
        ):
            first, second = Leader(one, holder="first"), Leader(two, holder="second")
            initial = await first.try_acquire()
            assert initial is not None
            close = first.connection.close
            acquired = None

            async def close_after_takeover():
                nonlocal acquired
                try:
                    acquired = await second.try_acquire()
                finally:
                    await close()

            monkeypatch.setattr(first.connection, "close", close_after_takeover)
            try:
                await first.release()
                assert acquired == initial + 1
            finally:
                await second.release()

    asyncio.run(exercise())


def test_leader_ping_reports_health_and_releases_on_error(dsn):
    async def exercise():
        async with AsyncConnectionPool(dsn, open=False) as pool:
            leader = Leader(pool, holder="ping-test")
            assert await leader.ping() is None
            generation = await leader.try_acquire()
            assert generation is not None
            try:
                assert await leader.ping() is True
                await leader.connection.close()
                assert await leader.ping() is False
                assert leader.generation is None
                assert await leader.is_held() is False
            finally:
                await leader.release()

    asyncio.run(exercise())
