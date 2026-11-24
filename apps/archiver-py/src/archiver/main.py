"""Archive JetStream or closed PostgreSQL partitions to the B1 handoff layout."""

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import psycopg
from archiver.export_db import DbArchive
from archiver.parquet_writer import ObjectStore
from archiver.seal import Catalog
from archiver.source_jetstream import BatchIdMismatch, JetStreamArchive
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import BotoCoreError, ClientError
from jammy_common import db, lifecycle, nats, obs, s3
from jammy_common.admin import AdminServer
from jammy_common.config import (
    CommonConfig,
    ConfigError,
    config_error_exit,
    env,
    positive_int,
)
from jammy_common.logging import setup as setup_logging
from nats.errors import Error as NatsError

LOGGER = logging.getLogger(__name__)
RETRY_ERRORS = (
    BotoCoreError,
    ClientError,
    S3UploadFailedError,
    NatsError,
    psycopg.Error,
    OSError,
    TimeoutError,
    ValueError,
    KeyError,
    OverflowError,
)


def _size(name: str, default: str) -> int:
    size = re.fullmatch(r"([0-9]+)(KiB|MiB|GiB)?", env(name, default))
    if size:
        try:
            amount = int(size[1])
        except ValueError:
            raise ConfigError(f"{name} must fit in a signed 64-bit byte size") from None
        multiplier = {None: 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[size[2]]
        if amount <= (2**63 - 1) // multiplier:
            return amount * multiplier
    raise ConfigError(f"{name} must be a valid signed 64-bit byte size")


@dataclass(frozen=True)
class ArchiveConfig:
    common: CommonConfig
    consumer: str
    streams: tuple[str, ...]
    bucket: str
    stage: Path
    flush_seconds: int
    flush_bytes: int
    level: int
    bandwidth: int
    ack_wait: int
    lateness: timedelta

    @classmethod
    def from_env(cls):
        common = CommonConfig.from_env()
        streams = ()
        if common.pipeline_mode == "jetstream":
            env("NATS_URL", required=True)
            streams = tuple(env("NATS_STREAMS", required=True).split(","))
            if len(set(streams)) != len(streams) or any(
                not stream or any(char.isspace() for char in stream)
                for stream in streams
            ):
                raise ConfigError("NATS_STREAMS must be a list without whitespace")
        else:
            env("DATABASE_URL", required=True)
            db.worker_pool_limits(1, 0)
        lateness = re.fullmatch(r"([0-9]+)([smh])", env("SEAL_LATENESS", "10m"))
        if not lateness:
            raise ConfigError("SEAL_LATENESS must be a duration in s, m or h")
        seconds = int(lateness[1]) * {"s": 1, "m": 60, "h": 3600}[lateness[2]]
        flush_bytes = _size("ARCHIVE_FLUSH_BYTES", "64MiB")
        if not flush_bytes:
            raise ConfigError("ARCHIVE_FLUSH_BYTES must be positive")
        flush_seconds = positive_int("ARCHIVE_FLUSH_SECONDS", 10)
        level = positive_int("ARCHIVE_ZSTD_LEVEL", 1)
        if level > 22:
            raise ConfigError("ARCHIVE_ZSTD_LEVEL must be in 1..22")
        return cls(
            common,
            env("CONSUMER_NAME", "archiver", required=True),
            streams,
            env("S3_BUCKET", required=True),
            Path(env("STAGE_DIR", "/stage", required=True)),
            flush_seconds,
            flush_bytes,
            level,
            _size("ARCHIVE_MAX_UPLOAD_BPS", "0"),
            positive_int("ACK_WAIT_SECONDS", flush_seconds * 3 + 60),
            timedelta(seconds=seconds),
        )


class Archiver:
    def __init__(self, config, store, meter):
        self.config = config
        self.store = store
        self.catalog = Catalog(store, config.lateness)
        self.restored = False
        self.client = None
        self.pool = None
        self.streams = []
        self.backlog = meter.create_gauge("jammy_archiver_unacked_messages")
        self.pending = {}

    async def is_ready(self):
        if not self.restored:
            return False
        await asyncio.to_thread(self.store.ready)
        if self.config.streams:
            return (
                bool(self.streams)
                and all(source.subscription is not None for source in self.streams)
                and await nats.check_streams(self.client, self.config.streams)
            )
        return await db.ping(self.pool)

    def observe(self, stream, pending):
        self.pending[stream] = pending
        self.backlog.set(sum(self.pending.values()))

    async def restore(self, stop):
        while not stop.is_set():
            try:
                await asyncio.to_thread(self.store.ready)
                await asyncio.to_thread(self.catalog.restore)
                self.restored = True
                return True
            except RETRY_ERRORS as error:
                LOGGER.warning("Archive catalog will retry (%s)", type(error).__name__)
                await asyncio.sleep(1)
        return False

    async def consume(self, stop, source):
        while not stop.is_set():
            if not self.client.is_connected:
                await asyncio.sleep(1)
                continue
            try:
                await source.prepare()
                if not await source.poll():
                    await asyncio.sleep(0.2)
            except RETRY_ERRORS as error:
                if isinstance(error, NatsError):
                    await source.reset()
                LOGGER.warning("Archive stream will retry (%s)", type(error).__name__)
                await asyncio.sleep(1)
        while True:
            try:
                await source.flush()
                return
            except BatchIdMismatch:
                raise
            except RETRY_ERRORS as error:
                LOGGER.warning(
                    "Shutdown archive flush will retry (%s)", type(error).__name__
                )
                await asyncio.sleep(1)

    async def export(self, stop):
        source = DbArchive(self.pool, self.store, self.catalog)
        while not stop.is_set():
            try:
                if not await source.poll():
                    await asyncio.sleep(1)
            except RETRY_ERRORS as error:
                LOGGER.warning("Archive export will retry (%s)", type(error).__name__)
                await asyncio.sleep(1)

    async def run(self, stop):
        cfg = self.config
        if cfg.streams:
            self.client = await nats.connect_until_stop(cfg.common, stop)
            if self.client is None:
                return
        else:
            self.pool = await db.pool(cfg.common, workers=1)
        try:
            if not await self.restore(stop):
                return
            if self.pool is not None:
                await self.export(stop)
                return
            persist_lock = asyncio.Lock()
            self.streams = [
                JetStreamArchive(
                    self.client,
                    cfg,
                    self.store,
                    self.catalog,
                    self.observe,
                    stream=stream,
                    persist_lock=persist_lock,
                )
                for stream in cfg.streams
            ]
            async with asyncio.TaskGroup() as tasks:
                for source in self.streams:
                    tasks.create_task(self.consume(stop, source))
        finally:
            if self.client is not None:
                await self.client.close()
            if self.pool is not None:
                await self.pool.close()


async def serve(config, stop):
    config.stage.mkdir(parents=True, exist_ok=True)
    for previous in config.stage.glob("jammy-archive-*.parquet"):
        if previous.is_file():
            previous.unlink()
    meter = obs.setup("archiver", config.common)
    store = ObjectStore(
        s3.client(config.common),
        config.bucket,
        config.stage,
        config.level,
        config.bandwidth,
        meter,
    )
    service = Archiver(config, store, meter)
    admin = AdminServer(config.common.admin_port, {"archive": service.is_ready})
    await admin.start()
    worker = asyncio.create_task(service.run(stop))
    stopping = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({worker, stopping}, return_when=asyncio.FIRST_COMPLETED)
        admin.set_draining()
        await worker
    finally:
        stopping.cancel()
        await admin.close()


@config_error_exit
def main() -> None:
    config = ArchiveConfig.from_env()
    setup_logging("archiver", config.common)
    lifecycle.run(lambda stop: serve(config, stop), config.common)


if __name__ == "__main__":
    main()
