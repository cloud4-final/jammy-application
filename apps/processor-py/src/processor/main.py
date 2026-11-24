"""Consume raw events and maintain the operational summaries."""

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from math import isfinite
from time import monotonic

import psycopg
from jammy_common import db, lifecycle, nats, obs
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
from processor.anomaly import AnomalyDetector
from processor.source_db import DbSource
from processor.source_jetstream import JetStreamSource
from processor.state import LatestState

LOGGER = logging.getLogger(__name__)
RETRY_ERRORS = (NatsError, psycopg.Error, TimeoutError, ValueError, KeyError)


@dataclass(frozen=True)
class ProcessorConfig:
    common: CommonConfig
    consumer: str
    streams: tuple[str, ...]
    ack_wait: int
    max_pending: int
    batch_size: int
    state_flush: int
    gate_max_past: timedelta
    drop_ratio: float

    @classmethod
    def from_env(cls):
        common = CommonConfig.from_env()
        env("DATABASE_URL", required=True)
        streams = ()
        if common.pipeline_mode == "jetstream":
            env("NATS_URL", required=True)
            streams = tuple(env("NATS_STREAMS", required=True).split(","))
            if len(set(streams)) != len(streams) or any(
                not stream or any(char.isspace() for char in stream)
                for stream in streams
            ):
                raise ConfigError("NATS_STREAMS must be a list without whitespace")
        raw = env("GATE_MAX_PAST", "24h")
        match = re.fullmatch(r"([0-9]+)([smh])", raw)
        if not match:
            raise ConfigError("GATE_MAX_PAST must be a duration in s, m or h")
        past = timedelta(seconds=int(match[1]) * {"s": 1, "m": 60, "h": 3600}[match[2]])
        try:
            ratio = float(env("ANOMALY_DROP_RATIO", "0.5"))
        except ValueError:
            raise ConfigError("ANOMALY_DROP_RATIO must be in (0, 1]") from None
        if not isfinite(ratio) or not 0 < ratio <= 1:
            raise ConfigError("ANOMALY_DROP_RATIO must be in (0, 1]")
        db.worker_pool_limits(1, 0)
        return cls(
            common,
            env("CONSUMER_NAME", "processor", required=True),
            streams,
            positive_int("ACK_WAIT_SECONDS", 30),
            positive_int("MAX_ACK_PENDING", 1000),
            positive_int("FETCH_BATCH", 64),
            positive_int("STATE_FLUSH_SECONDS", 1),
            past,
            ratio,
        )


class Processor:
    def __init__(self, config, pool, meter) -> None:
        self.config = config
        self.pool = pool
        self.detector = AnomalyDetector(config.drop_ratio)
        self.state = LatestState()
        self.batches = meter.create_counter("jammy_processor_batches")
        self.duration = meter.create_histogram(
            "jammy_processor_commit_duration", unit="s"
        )
        self.pending = meter.create_gauge("jammy_processor_consumer_pending")
        self.lag = meter.create_gauge("jammy_processor_lag", unit="s")
        self.client = None
        self.database_ready = asyncio.Event()
        self.prepared = set()
        self.pending_by_stream = {}
        self.lag_by_stream = {}

    async def is_ready(self):
        if not self.database_ready.is_set():
            return False
        if self.config.streams and (
            self.client is None or len(self.prepared) != len(self.config.streams)
        ):
            return False
        if self.client is not None and not await nats.check_streams(
            self.client, self.config.streams
        ):
            return False
        return await db.ping(self.pool)

    async def maintain(self, stop):
        maintenance_at = assets_at = 0.0
        while not stop.is_set():
            try:
                if monotonic() >= maintenance_at:
                    async with self.pool.connection(timeout=1) as conn:
                        await conn.execute("SET LOCAL statement_timeout = '1s'")
                        await conn.execute(
                            "SELECT jammy.ensure_partitions('3 hours', %s)",
                            (self.config.gate_max_past,),
                        )
                        await conn.execute(
                            """DELETE FROM jammy.processed_batches
                               WHERE consumer = %s AND processed_at < now() - interval '1 day'""",
                            (self.config.consumer,),
                        )
                    maintenance_at = monotonic() + 600
                if monotonic() >= assets_at:
                    await self.detector.refresh(self.pool)
                    assets_at = monotonic() + 60
                self.database_ready.set()
            except RETRY_ERRORS as error:
                self.database_ready.clear()
                LOGGER.warning(
                    "Database preparation will retry (%s)", type(error).__name__
                )
            await asyncio.sleep(1)

    async def flush_state(self, stop):
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.config.state_flush)
            except TimeoutError:
                try:
                    await self.state.flush(self.pool)
                except RETRY_ERRORS as error:
                    LOGGER.warning("State flush will retry (%s)", type(error).__name__)
                    await asyncio.sleep(1)

    def observe_stream(self, stream, pending, lag):
        self.pending_by_stream[stream] = pending
        self.lag_by_stream[stream] = lag
        self.pending.set(sum(self.pending_by_stream.values()))
        self.lag.set(max(self.lag_by_stream.values(), default=0))

    async def consume(self, stop, source):
        while not stop.is_set():
            if not self.database_ready.is_set() or not self.client.is_connected:
                await asyncio.sleep(1)
                continue
            try:
                await source.prepare()
                self.prepared.add(source.stream)
                messages = await source.fetch()
            except RETRY_ERRORS as error:
                self.prepared.discard(source.stream)
                await source.reset()
                LOGGER.warning("Fetch will retry (%s)", type(error).__name__)
                await asyncio.sleep(1)
                continue
            lag = self.lag_by_stream.get(source.stream, 0)
            if messages:
                started = monotonic()
                try:
                    readings, results, committed_lag = await source.commit(messages)
                except RETRY_ERRORS as error:
                    self.batches.add(len(messages), {"result": "error"})
                    LOGGER.warning("Processing will retry (%s)", type(error).__name__)
                    await asyncio.sleep(1)
                    continue
                if committed_lag is not None:
                    lag = committed_lag
                    self.duration.record(monotonic() - started)
                    self.state.offer(readings)
                for message, result in zip(messages, results, strict=True):
                    if result != "error":
                        try:
                            await message.ack()
                        except NatsError:
                            # Redelivery is safe because the claim is durable.
                            pass
                    self.batches.add(1, {"result": result})
                if "error" in results:
                    self.observe_stream(
                        source.stream, self.pending_by_stream.get(source.stream, 0), lag
                    )
                    LOGGER.warning("JetStream batch contains invalid messages")
                    await asyncio.sleep(1)
                    continue
            try:
                pending = await source.pending()
            except RETRY_ERRORS as error:
                self.prepared.discard(source.stream)
                await source.reset()
                LOGGER.warning("Consumer status unavailable (%s)", type(error).__name__)
                await asyncio.sleep(1)
                continue
            self.observe_stream(source.stream, pending, lag if pending else 0)
            if not messages:
                await asyncio.sleep(0.2)

    async def poll_database(self, stop, source):
        self.pending.set(0)
        while not stop.is_set():
            if not self.database_ready.is_set():
                await asyncio.sleep(1)
                continue
            started = monotonic()
            try:
                readings, lag = await source.poll()
                self.lag.set(lag)
                if readings:
                    self.duration.record(monotonic() - started)
                    self.batches.add(1, {"result": "ok"})
                    self.state.offer(readings)
                else:
                    await asyncio.sleep(0.2)
            except RETRY_ERRORS as error:
                self.batches.add(1, {"result": "error"})
                LOGGER.warning("Database poll will retry (%s)", type(error).__name__)
                await asyncio.sleep(1)

    async def loop(self, stop):
        cfg = self.config
        if cfg.streams:
            self.client = await nats.connect_until_stop(cfg.common, stop)
            if self.client is None:
                return
        options = {
            "consumer": cfg.consumer,
            "batch_size": cfg.batch_size,
            "detector": self.detector,
        }
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self.maintain(stop))
                tasks.create_task(self.flush_state(stop))
                if self.client is None:
                    tasks.create_task(
                        self.poll_database(stop, DbSource(self.pool, **options))
                    )
                for stream in cfg.streams:
                    source = JetStreamSource(
                        self.pool,
                        self.client,
                        stream=stream,
                        ack_wait=cfg.ack_wait,
                        max_pending=cfg.max_pending,
                        **options,
                    )
                    tasks.create_task(self.consume(stop, source))
            await self.state.flush(self.pool)
        finally:
            if self.client is not None:
                await self.client.close()


async def serve(config, stop):
    pool = await db.pool(config.common, workers=1)
    processor = Processor(config, pool, obs.setup("processor", config.common))
    admin = AdminServer(config.common.admin_port, {"source": processor.is_ready})
    await admin.start()
    task = asyncio.create_task(processor.loop(stop))
    stopping = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({stopping, task}, return_when=asyncio.FIRST_COMPLETED)
        admin.set_draining()
        await task
    finally:
        stopping.cancel()
        await admin.close()
        await pool.close()


@config_error_exit
def main() -> None:
    config = ProcessorConfig.from_env()
    setup_logging("processor", config.common)
    lifecycle.run(lambda stop: serve(config, stop), config.common)


if __name__ == "__main__":
    main()
