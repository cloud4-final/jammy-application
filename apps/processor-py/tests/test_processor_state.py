import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, Mock

from processor.aggregate import Reading
from processor.main import Processor, ProcessorConfig


def test_latest_state_coalesces_until_configured_flush(monkeypatch):
    for name, value in {
        "SITE_ID": "test",
        "PIPELINE_MODE": "direct-db",
        "DATABASE_URL": "postgresql://localhost/test",
        "STATE_FLUSH_SECONDS": "3",
    }.items():
        monkeypatch.setenv(name, value)

    async def run():
        stop = asyncio.Event()
        cursor = AsyncMock()
        cursor.executemany.side_effect = lambda *_: stop.set()
        pool = MagicMock()
        conn = Mock()
        pool.connection.return_value.__aenter__.return_value = conn
        conn.cursor.return_value = MagicMock()
        conn.cursor.return_value.__aenter__.return_value = cursor
        service = Processor(ProcessorConfig.from_env(), pool, Mock())
        oldest = datetime(2026, 11, 23, tzinfo=UTC)
        newest = oldest + timedelta(seconds=1)
        service.state.offer([Reading("solar", "a", oldest, 1)])
        service.state.offer(
            [
                Reading("solar", "a", newest, 2),
                Reading("solar", "a", oldest, 3),
                Reading("solar", "b", oldest, 4),
            ]
        )

        async def interval_elapsed(waiter, *, timeout):
            waiter.close()
            assert timeout == 3
            cursor.executemany.assert_not_awaited()
            raise TimeoutError

        monkeypatch.setattr("processor.main.asyncio.wait_for", interval_elapsed)
        await service.flush_state(stop)
        cursor.executemany.assert_awaited_once()
        assert cursor.executemany.await_args.args[1] == [
            ("a", "solar", 2, newest),
            ("b", "solar", 4, oldest),
        ]
        assert not service.state.pending

    asyncio.run(run())


def test_latest_state_equal_timestamp_uses_strict_less_than_on_offer_and_flush_retry():
    from processor.state import LatestState

    async def run():
        t0 = datetime(2026, 11, 25, 10, 0, tzinfo=UTC)
        state = LatestState()
        state.offer(
            [
                Reading("solar", "a", t0, 10),
                Reading("solar", "a", t0, 20),
            ]
        )
        assert state.pending["a"].value == 10

        cursor = AsyncMock()
        pool = MagicMock()
        conn = Mock()
        pool.connection.return_value.__aenter__.return_value = conn
        conn.cursor.return_value = MagicMock()
        conn.cursor.return_value.__aenter__.return_value = cursor

        async def fail_and_offer_newer(sql, _rows):
            assert "WHERE latest_state.event_time < EXCLUDED.event_time" in sql
            state.offer([Reading("solar", "a", t0, 99)])
            raise RuntimeError("transient flush failure")

        cursor.executemany.side_effect = fail_and_offer_newer
        try:
            await state.flush(pool)
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected flush failure")

        assert state.pending["a"].value == 99

    asyncio.run(run())
