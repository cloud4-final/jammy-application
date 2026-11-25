import asyncio
from contextlib import asynccontextmanager
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import test_dispatch
from dispatcher.commands import Dispatcher
from dispatcher.leader import Leader
from dispatcher.lease import Lease
from jammy_common.config import ConfigError
from psycopg import OperationalError
from psycopg_pool import AsyncConnectionPool

postgres = test_dispatch.postgres
dsn = test_dispatch.dsn


def test_fence_interval_must_be_shorter_than_ttl(monkeypatch):
    for name, value in {
        "SITE_ID": "site-a", "LEASE_MODE": "dynamodb",
        "LEASE_TABLE": "lease", "LEASE_AUTO_ACQUIRE": "true",
        "LEASE_FENCE_SECONDS": "15", "LEASE_TTL_SECONDS": "15",
    }.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ConfigError, match="LEASE_FENCE_SECONDS"):
        Lease.from_env()


@pytest.mark.parametrize("failed_publish", [0, OperationalError("database unavailable")])
@pytest.mark.parametrize("retry_after", [1, 11])
def test_unpublished_renewal_retries_only_while_fresh(failed_publish, retry_after):
    async def exercise():
        now = 100
        table = SimpleNamespace(
            get_item=lambda **kwargs: {"Item": {
                "holder": "site-a", "epoch": 4, "expires_at": float("inf"),
            }},
            update_item=lambda **kwargs: None,
        )
        lease = Lease(table, holder="site-a", auto_acquire=True, clock=lambda: now)
        leader = SimpleNamespace(
            generation=1,
            is_held=AsyncMock(return_value=True),
            release=AsyncMock(),
            execute=AsyncMock(side_effect=[
                failed_publish if isinstance(failed_publish, Exception)
                else SimpleNamespace(rowcount=failed_publish),
                SimpleNamespace(rowcount=1),
            ]),
        )

        @asynccontextmanager
        async def transaction():
            yield leader

        leader.transaction = transaction
        dispatcher = Dispatcher(None, "", leader, lease, site_id="site-a", clock=lambda: now)
        if isinstance(failed_publish, Exception):
            with pytest.raises(OperationalError):
                await dispatcher.renew_lease()
        else:
            await dispatcher.renew_lease()
        renewal = lease.renewal
        assert not (await dispatcher.active_lease()).active
        await dispatcher.fence_if_stale()
        leader.release.assert_not_awaited()

        now += retry_after
        if retry_after < lease.fence_seconds:
            await dispatcher.renew_lease()
            assert lease.renewal is renewal
            assert (await dispatcher.active_lease()).active
            assert leader.execute.await_args.args[1] == ("site-a", True, 4, False, 1)
        else:
            await dispatcher.fence_if_stale()
            assert not (await dispatcher.active_lease()).active
            assert leader.execute.await_args.args[1] == ("site-a", False, 4, True, 11)
            leader.release.assert_awaited_once_with(generation=1)
        assert leader.execute.await_count == 2

    asyncio.run(exercise())


def test_leader_fences_during_stalled_renewal_and_recovers():
    async def exercise():
        now = 100
        pending = asyncio.Event()
        complete = Event()
        loop = asyncio.get_running_loop()

        class Table:
            item = None
            stall = False

            def get_item(self, **kwargs):
                if self.stall:
                    loop.call_soon_threadsafe(pending.set)
                    complete.wait(5)
                return {"Item": self.item}

            def update_item(self, **kwargs):
                values = kwargs["ExpressionAttributeValues"]
                self.item = {"holder": "site-a", "epoch": 1, "expires_at": values[":expires"]}
                return {"Attributes": {"epoch": 1}}

        class Leader:
            generation = 1
            execute = AsyncMock(return_value=SimpleNamespace(rowcount=1))

            async def is_held(self):
                return self.generation is not None

            async def release(self, *, generation):
                if self.generation == generation:
                    self.generation = None

            @asynccontextmanager
            async def transaction(self):
                yield self

        table = Table()
        leader = Leader()
        lease = Lease(table, holder="site-a", auto_acquire=True, fence_seconds=10, clock=lambda: now)
        dispatcher = Dispatcher(None, "", leader, lease, site_id="site-a", clock=lambda: now)
        await dispatcher.renew_lease()
        assert (await dispatcher.active_lease()).active
        first_row = leader.execute.await_args.args[1]
        assert first_row == ("site-a", True, 1, False, 0)

        table.stall = True
        now += 5
        blocked = asyncio.create_task(dispatcher.renew_lease())
        try:
            async with asyncio.timeout(2):
                await pending.wait()
                now += 6
                await dispatcher.fence_if_stale()
                assert leader.generation is None
                assert not (await dispatcher.active_lease()).active
                assert not await dispatcher.dispatch_one()
                assert leader.execute.await_args.args[1] == ("site-a", False, 1, True, 11)
        finally:
            table.stall = False
            complete.set()
            await blocked

        now += 1
        leader.generation = 2
        await dispatcher.renew_lease()
        assert (await dispatcher.active_lease()).active
        assert leader.execute.await_args.args[1][:4] == ("site-a", True, 1, False)

        table.item |= {"holder": "site-b", "epoch": 2}
        await dispatcher.renew_lease()
        assert leader.generation is None
        assert leader.execute.await_args.args[1][:4] == ("site-a", False, 1, True)
        await dispatcher.renew_lease()
        assert not (await dispatcher.active_lease()).active
        assert leader.execute.await_count == 4

    asyncio.run(exercise())


def test_leader_publishes_renewal_and_fence_to_postgres(dsn):
    async def exercise():
        tick = 100.0

        class Table:
            def get_item(self, **kwargs):
                return {}

            def update_item(self, **kwargs):
                return {"Attributes": {"epoch": 4}}

        async with AsyncConnectionPool(dsn, open=False) as pool:
            leader = Leader(pool)
            await leader.try_acquire()
            try:
                lease = Lease(Table(), holder="aws-site-a", auto_acquire=True, clock=lambda: tick)
                dispatcher = Dispatcher(None, "", leader, lease, site_id="site-a", clock=lambda: tick)
                await dispatcher.renew_lease()
                async with pool.connection() as conn:
                    async def row():
                        cursor = await conn.execute(
                            """SELECT holder, epoch, fenced, renewed_at
                               FROM jammy.site_lease_state WHERE site_id = 'site-a'"""
                        )
                        return await cursor.fetchone()

                    renewed = await row()
                    assert renewed[:3] == (True, 4, False)
                    tick += 11
                    await dispatcher.fence_if_stale()
                    assert await row() == (False, 4, True, renewed[3])
            finally:
                await leader.release()

    asyncio.run(exercise())
