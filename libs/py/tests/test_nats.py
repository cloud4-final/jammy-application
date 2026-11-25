import asyncio
import socket
from contextlib import suppress
from unittest.mock import AsyncMock

import pytest

from jammy_common import nats
from jammy_common.config import CommonConfig, ConfigError


@pytest.mark.parametrize("endpoint", ["http://localhost:4222", "nats://localhost:0"])
def test_nats_connect_rejects_invalid_url(monkeypatch, endpoint):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("NATS_URL", endpoint)
    cfg = CommonConfig.from_env()

    async def exercise():
        with pytest.raises(ConfigError, match="^invalid NATS_URL$"):
            await nats.connect(cfg)

    asyncio.run(exercise())


def test_nats_connect_preserves_tls_and_credentials_when_defaulting_port(monkeypatch):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("NATS_URL", "tls://user:password@[::1]:, nats://localhost")
    connect = AsyncMock()
    monkeypatch.setattr(nats.Client, "connect", connect)

    asyncio.run(nats.connect(CommonConfig.from_env()))

    assert connect.call_args.kwargs["servers"] == [
        "tls://user:password@[::1]:4222",
        "nats://localhost:4222",
    ]
    assert connect.call_args.kwargs["reconnect_time_wait"] == 1
    assert connect.call_args.kwargs["max_reconnect_attempts"] == -1


def test_nats_connect_supports_comma_separated_urls(monkeypatch):
    monkeypatch.setenv("SITE_ID", "site-a")

    async def exercise():
        with socket.socket() as unused:
            unused.bind(("127.0.0.1", 0))
            dead_port = unused.getsockname()[1]

        async def minimal_nats(reader, writer):
            writer.write(b'INFO {"max_payload":1048576}\r\n')
            try:
                while line := await reader.readline():
                    if line == b"PING\r\n":
                        writer.write(b"PONG\r\n")
                        await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(minimal_nats, "127.0.0.1", 0)
        live_port = server.sockets[0].getsockname()[1]
        monkeypatch.setenv(
            "NATS_URL", f"nats://127.0.0.1:{dead_port}, nats://127.0.0.1:{live_port}"
        )
        try:
            client = await asyncio.wait_for(nats.connect(CommonConfig.from_env()), timeout=8)
            try:
                assert client.is_connected
            finally:
                with suppress(Exception):
                    await client.close()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(exercise())
