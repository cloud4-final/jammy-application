"""Connect to NATS with retries before and after the first connection."""

import asyncio
import logging
import os
from urllib.parse import urlsplit

from nats.aio.client import Client

from .config import CommonConfig, env, validate_nats_url

logger = logging.getLogger(__name__)


def _server_urls(value: str) -> list[str]:
    urls: list[str] = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        parsed = urlsplit(part)
        if parsed.scheme.lower() in {"nats", "tls"} and parsed.port is None:
            netloc = parsed.netloc.removesuffix(":")
            part = parsed._replace(netloc=f"{netloc}:4222").geturl()
        urls.append(part)
    return urls


async def connect(cfg: CommonConfig) -> Client:
    """Await a connection. Start this as a task alongside the admin server."""
    servers = _server_urls(validate_nats_url(env("NATS_URL", required=True)))
    credentials = os.environ.get("NATS_CREDS_FILE") or None
    client = Client()

    async def on_error(error: Exception) -> None:
        logger.warning("NATS connection error (%s)", type(error).__name__)

    try:
        await client.connect(
            servers=servers,
            user_credentials=credentials,
            name=cfg.instance_id,
            # The client applies this fixed delay to the initial retry loop too.
            max_reconnect_attempts=-1,
            reconnect_time_wait=1,
            error_cb=on_error,
        )
    except BaseException:
        # Before the transport connects, close() can access a missing writer.
        if client.is_connecting or client.is_connected or client.is_reconnecting:
            await client.close()
        raise
    return client


async def check_streams(client: Client, streams: tuple[str, ...]) -> bool:
    if not client.is_connected:
        return False
    # A timed-out flush can leave a cancelled PONG future in nats-py 2.13.1.
    js = client.jetstream()
    async with asyncio.timeout(1):
        for stream in streams:
            await js.stream_info(stream)
    return True


async def connect_until_stop(cfg: CommonConfig, stop: asyncio.Event) -> Client | None:
    connecting = asyncio.create_task(connect(cfg))
    stopping = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            {connecting, stopping}, return_when=asyncio.FIRST_COMPLETED
        )
        if connecting in done:
            return connecting.result()
        return None
    finally:
        for task in (connecting, stopping):
            if not task.done():
                task.cancel()
        await asyncio.gather(connecting, stopping, return_exceptions=True)
