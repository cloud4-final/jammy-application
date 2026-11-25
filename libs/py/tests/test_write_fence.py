import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from jammy_common.write_fence import Fence, Fenced


def test_write_fence_cache_expires_at_lease_deadline_and_recovers():
    async def exercise():
        tick = 0.0
        fence = Fence("site-a", 10, clock=lambda: tick)
        db_now = datetime(2026, 11, 25, tzinfo=UTC)
        execute = AsyncMock()
        execute.return_value.fetchone.return_value = (
            True, False, db_now - timedelta(seconds=9.75), db_now
        )
        conn = type("Connection", (), {"execute": execute})()
        await fence.check(conn)
        tick += 0.1
        await fence.check(conn)
        assert execute.await_count == 1

        execute.return_value.fetchone.return_value = (True, False, db_now - timedelta(seconds=10), db_now)
        tick += 0.15
        with pytest.raises(Fenced):
            await fence.check(conn)
        assert execute.await_count == 2

        execute.return_value.fetchone.return_value = (True, False, db_now, db_now)
        tick += 0.5
        with pytest.raises(Fenced):
            await fence.check(conn)
        tick += 0.5
        await fence.check(conn)
        assert execute.await_count == 3

        execute.return_value.fetchone.return_value = None
        tick += 1
        with pytest.raises(Fenced):
            await fence.check(conn)

    asyncio.run(exercise())
