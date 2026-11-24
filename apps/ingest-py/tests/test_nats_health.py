import asyncio
from itertools import chain, repeat
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import httpx
import pytest
from ingest import main
from nats.aio.client import Client
from nats.errors import ConnectionReconnectingError
from nats.errors import TimeoutError as NatsTimeout
from test_ingest_jetstream import batch, post


class LatePongClient(Client):
    def __init__(self):
        super().__init__()
        self._status = Client.CONNECTED
        self.js = SimpleNamespace(stream_info=AsyncMock(), update_stream=AsyncMock())
        self.read_loop_failed = False

    def jetstream(self, **kwargs):
        return self.js

    async def flush(self, timeout=1):
        # Use a short clock while exercising the real client's flush/PONG futures.
        await super().flush(timeout=0.01)

    async def _send_ping(self, future):
        self._pongs.append(future)
        asyncio.get_running_loop().call_later(0.02, self.deliver_pong)

    def deliver_pong(self):
        asyncio.create_task(self.process_pong())

    async def process_pong(self):
        try:
            await self._process_pong()
        except asyncio.InvalidStateError:
            self.read_loop_failed = True
            self.js.stream_info.side_effect = NatsTimeout


@pytest.fixture
def config(monkeypatch):
    monkeypatch.setenv("SITE_ID", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost")
    monkeypatch.setenv("INGEST_STREAM", "raw")
    monkeypatch.setenv("PIPELINE_MODE", "jetstream")
    return main.IngestConfig.from_env()


def test_worker_readiness_survives_delayed_pong(monkeypatch, config):
    async def run():
        client = LatePongClient()
        connect = AsyncMock(return_value=client)
        monkeypatch.setattr(main.nats, "connect", connect)
        state = main.IntakeState(config)
        readiness = []
        background_ping = asyncio.get_running_loop().create_future()
        await client._send_ping(background_ping)

        async def tick(seconds):
            readiness.append(await state.is_ready())
            if len(readiness) == 2:
                raise asyncio.CancelledError
            await asyncio.sleep(0.03)

        monkeypatch.setattr(main, "asyncio", SimpleNamespace(sleep=tick))
        with pytest.raises(asyncio.CancelledError):
            await main._maintain(config, state, worker_index=0)

        assert background_ping.result()
        assert not client.read_loop_failed
        assert readiness == [True, True]
        connect.assert_awaited_once_with(config.common)

    asyncio.run(run())


def test_worker_reconnects_after_three_consecutive_nats_timeouts(monkeypatch, config):
    async def run():
        first_info = AsyncMock(
            side_effect=chain(
                repeat(None, 4), (NatsTimeout, TimeoutError, NatsTimeout), repeat(None)
            )
        )
        next_info = AsyncMock()

        def fake_client(info):
            js = SimpleNamespace(stream_info=info, update_stream=AsyncMock())
            return SimpleNamespace(
                is_connected=True,
                jetstream=lambda **kwargs: js,
                flush=AsyncMock(),
                close=AsyncMock(),
            )

        first, replacement = fake_client(first_info), fake_client(next_info)
        connect = AsyncMock(side_effect=[first, replacement])
        monkeypatch.setattr(main.nats, "connect", connect)
        state = main.IntakeState(config)
        readiness = []

        async def tick(seconds):
            assert seconds == 1
            readiness.append(await state.is_ready())
            if len(readiness) == 5:
                raise asyncio.CancelledError

        monkeypatch.setattr(main, "asyncio", SimpleNamespace(sleep=tick))
        with pytest.raises(asyncio.CancelledError):
            await main._maintain(config, state, worker_index=0)
        assert readiness == [True, False, False, False, True]
        assert connect.await_args_list == [call(config.common)] * 2
        first.close.assert_awaited_once()
        assert state.sink.client is replacement
        assert next_info.await_args_list == [call("raw"), call("quarantine-raw")] * 2
        assert replacement.jetstream().update_stream.await_count == 2

    asyncio.run(run())


def test_publish_during_reconnect_returns_retryable_503(config):
    async def run():
        client = Client()
        client.request = AsyncMock(side_effect=ConnectionReconnectingError)
        state = main.IntakeState(config)
        state.sink = main.JetStreamSink(
            client, config.stream, config.max_age, config.js_max_bytes
        )
        # The publish observes the disconnect before the readiness check does.
        state.update_ready(True)
        transport = httpx.ASGITransport(app=main.create_app(config, state))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://ingest"
        ) as http:
            response, _ = await post(http, batch())

        client.request.assert_awaited_once()
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "2"

    asyncio.run(run())


def test_check_streams_times_out_after_one_second_across_both_streams():
    async def stream_info(name):
        await asyncio.sleep(0.6)

    async def run():
        info = AsyncMock(side_effect=stream_info)
        client = SimpleNamespace(
            is_connected=True, jetstream=lambda: SimpleNamespace(stream_info=info)
        )
        async with asyncio.timeout(2):
            with pytest.raises(TimeoutError):
                await main.nats.check_streams(client, ("raw", "quarantine-raw"))
        assert info.await_args_list == [call("raw"), call("quarantine-raw")]

    asyncio.run(run())
