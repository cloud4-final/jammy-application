from datetime import UTC, datetime

from processor.aggregate import Reading
from processor.anomaly import drop_detected, over_capacity

NOW = datetime(2026, 11, 23, tzinfo=UTC)


def test_anomaly_drop_detected():
    assert drop_detected(last_mean=20, window_mean=40, ratio=0.5)
    assert not drop_detected(last_mean=20.01, window_mean=40, ratio=0.5)


def test_anomaly_over_capacity():
    reading = Reading("solar", "asset", NOW, 110.01)
    assert over_capacity(reading, {"asset": 100_000})
    assert not over_capacity(reading, {"asset": 101_000})
    assert not over_capacity(reading, {})


def test_capacity_milli_kw_boundary():
    assert not over_capacity(
        Reading("solar", "asset", NOW, 110.0004), {"asset": 100_000}
    )
    assert over_capacity(Reading("solar", "asset", NOW, 110.0006), {"asset": 100_000})


def test_asset_cache_refreshes_on_background_schedule(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from processor.main import Processor

    async def run():
        stop = asyncio.Event()
        times = iter([0, 0, 0, 0, 59, 59, 60, 60, 60])
        service = Processor(
            SimpleNamespace(drop_ratio=0.5, consumer="processor", gate_max_past=None),
            None,
            Mock(),
        )
        service.detector.refresh = AsyncMock()
        conn = SimpleNamespace(execute=AsyncMock())
        context = AsyncMock()
        context.__aenter__.return_value = conn
        service.pool = SimpleNamespace(connection=Mock(return_value=context))
        monkeypatch.setattr("processor.main.monotonic", lambda: next(times))

        async def pause(seconds):
            assert seconds == 1
            if service.detector.refresh.await_count == 2:
                stop.set()

        monkeypatch.setattr("processor.main.asyncio.sleep", pause)
        await service.maintain(stop)
        assert service.detector.refresh.await_count == 2
        assert conn.execute.await_count == 3

    asyncio.run(run())


def test_drop_incident_has_contract_detail_keys():
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from processor.anomaly import AnomalyDetector

    async def run():
        cursor = AsyncMock()
        cursor.__aenter__.return_value = cursor
        rows = SimpleNamespace(
            fetchall=AsyncMock(return_value=[("synth-small", "asset", NOW, 40, 20)])
        )
        conn = SimpleNamespace(
            execute=AsyncMock(return_value=rows), cursor=lambda: cursor
        )
        detector = AnomalyDetector(0.5)
        await detector.write(conn, [], {("synth-small", "asset", NOW): object()})
        assert "unnest" in conn.execute.await_args.args[0]
        (incident,) = cursor.executemany.await_args.args[1]
        assert incident[-1].obj == {"last_mean": 20, "window_mean": 40, "ratio": 0.5}

    asyncio.run(run())
