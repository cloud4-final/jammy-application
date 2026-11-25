"""Consume raw events and maintain the operational summaries."""

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from time import monotonic

import psycopg
from jammy_common import db, lifecycle, nats, obs
from jammy_common.admin import AdminServer
from jammy_common.config import (
    CommonConfig,
    config_error_exit,
    env,
    env_duration,
    env_ratio,
    parse_list,
    positive_int,
    validate_database_url,
    validate_nats_url,
)
from jammy_common.logging import setup as setup_logging
from jammy_common.write_fence import Fence, Fenced, write_connection, write_fence_age
from nats.errors import Error as NatsError
from opentelemetry import trace
from opentelemetry.propagate import get_global_textmap
from opentelemetry.propagators.textmap import Getter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from processor.anomaly import AnomalyDetector
from processor.source_db import DbSource
from processor.source_jetstream import JetStreamSource
from processor.state import LatestState

LOGGER = logging.getLogger(__name__)
RETRY_ERRORS = (NatsError, psycopg.Error, TimeoutError)
_TRACE_PROPAGATOR = TraceContextTextMapPropagator()


class _HeaderGetter(Getter[dict[str, str]]):
    def get(self, carrier: dict[str, str], key: str) -> list[str] | None:
        if not carrier:
            return None
        if key in carrier:
            return [carrier[key]]
        lower = key.lower()
        for k, v in carrier.items():
            if k.lower() == lower:
                return [v]
        return None

    def keys(self, carrier: dict[str, str]) -> list[str]:
        return list(carrier.keys()) if carrier else []


_HEADER_GETTER = _HeaderGetter()


def _extract_context(headers: dict[str, str] | None):
    if not headers:
        return None
    propagator = get_global_textmap()
    if not propagator.fields:
        propagator = _TRACE_PROPAGATOR
    return propagator.extract(headers, getter=_HEADER_GETTER)


def _start_batch_span(tracer: trace.Tracer, stream: str, messages):
    parent = None
    links = []
    for message in messages:
        headers = getattr(message, "headers", None)
        extracted = _extract_context(headers)
        if extracted is None:
            continue
        sc = trace.get_current_span(extracted).get_span_context()
        if sc.is_valid:
            if len(messages) == 1:
                parent = extracted
            else:
                links.append(trace.Link(sc))
    attributes = {
        "messaging.system": "nats",
        "messaging.destination.name": stream,
        "messaging.batch.message_count": len(messages),
    }
    if len(messages) == 1:
        headers = getattr(messages[0], "headers", None)
        if batch_id := (_HEADER_GETTER.get(headers, "Nats-Msg-Id") if headers else None):
            attributes["batch_id"] = batch_id[0]
    return tracer.start_as_current_span(
        "processor.commit",
        context=parent,
        kind=trace.SpanKind.CONSUMER,
        attributes=attributes,
        links=links,
    )


@dataclass(frozen=True)
class ProcessorConfig:
    common: CommonConfig
    consumer: str
    streams: tuple[str, ...]
    ack_wait: int
    max_pending: int
    max_deliver: int
    batch_size: int
    state_flush: int
    gate_max_past: timedelta
    drop_ratio: float
    fence_age: int | None = None

    @classmethod
    def from_env(cls):
        common = CommonConfig.from_env()
        validate_database_url(env("DATABASE_URL", required=True))
        streams = ()
        if common.pipeline_mode == "jetstream":
            validate_nats_url(env("NATS_URL", required=True))
            streams = tuple(
                parse_list("NATS_STREAMS", env("NATS_STREAMS", required=True), unique=True)
            )
        past = timedelta(seconds=env_duration("GATE_MAX_PAST", "24h"))
        ratio = env_ratio("ANOMALY_DROP_RATIO", "0.5", float)
        db.worker_pool_limits(1, 0)
        return cls(
            common,
            env("CONSUMER_NAME", "processor", required=True),
            streams,
            positive_int("ACK_WAIT_SECONDS", 30),
            positive_int("MAX_ACK_PENDING", 1000),
            positive_int("PROCESSOR_MAX_DELIVER", 10),
            positive_int("FETCH_BATCH", 64),
            positive_int("STATE_FLUSH_SECONDS", 1),
            past,
            ratio,
            write_fence_age(),
        )


class Processor:
    def __init__(self, config, pool, meter, *, fence: Fence | None = None) -> None:
        self.config = config
        self.pool = pool
        self.detector = AnomalyDetector(config.drop_ratio)
        self.fence = fence
        self.state = LatestState(fence)
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
                    try:
                        async with write_connection(self.pool, self.fence, timeout=1) as conn:
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
                    except Fenced:
                        pass
                    else:
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
                retry = False
                with trace.get_tracer("jammy.processor").start_as_current_span(
                    "processor.flush"
                ) as span:
                    try:
                        await self.state.flush(self.pool)
                    except Fenced:
                        pass
                    except RETRY_ERRORS as error:
                        span.record_exception(error)
                        span.set_status(trace.StatusCode.ERROR, str(error))
                        LOGGER.warning("State flush will retry (%s)", type(error).__name__)
                        retry = True
                if retry:
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
                poisoned = 0

                def record_poison():
                    nonlocal poisoned
                    poisoned += 1
                    self.batches.add(1, {"result": "poison"})

                retry = False
                with _start_batch_span(
                    trace.get_tracer("jammy.processor"), source.stream, messages
                ) as span:
                    try:
                        readings, results, committed_lag = await source.commit(
                            messages, record_poison
                        )
                    except Fenced:
                        self.batches.add(len(messages) - poisoned, {"result": "fenced"})
                        retry = True
                    except RETRY_ERRORS as error:
                        span.record_exception(error)
                        span.set_status(trace.StatusCode.ERROR, str(error))
                        self.batches.add(len(messages) - poisoned, {"result": "error"})
                        LOGGER.warning("Processing will retry (%s)", type(error).__name__)
                        retry = True
                    else:
                        if committed_lag is not None:
                            lag = committed_lag
                            self.duration.record(monotonic() - started)
                            self.state.offer(readings)
                        for message, result in zip(messages, results, strict=True):
                            if result == "poison":
                                continue
                            if result != "error":
                                try:
                                    await message.ack()
                                except NatsError:
                                    # Redelivery is safe because the claim is durable.
                                    pass
                            self.batches.add(1, {"result": result})
                        if "error" in results:
                            self.observe_stream(
                                source.stream,
                                self.pending_by_stream.get(source.stream, 0),
                                lag,
                            )
                            span.set_status(
                                trace.StatusCode.ERROR,
                                "JetStream batch contains invalid messages",
                            )
                            LOGGER.warning("JetStream batch contains invalid messages")
                            retry = True
                if retry:
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
            retry = False
            idle = False
            with trace.get_tracer("jammy.processor").start_as_current_span(
                "processor.poll", kind=trace.SpanKind.CONSUMER
            ) as span:
                try:
                    readings, lag = await source.poll()
                    self.lag.set(lag)
                    if readings:
                        span.set_attribute("batch.events", len(readings))
                        self.duration.record(monotonic() - started)
                        self.batches.add(1, {"result": "ok"})
                        self.state.offer(readings)
                    else:
                        idle = True
                except Fenced:
                    self.batches.add(1, {"result": "fenced"})
                    retry = True
                except RETRY_ERRORS as error:
                    span.record_exception(error)
                    span.set_status(trace.StatusCode.ERROR, str(error))
                    self.batches.add(1, {"result": "error"})
                    LOGGER.warning("Database poll will retry (%s)", type(error).__name__)
                    retry = True
            if retry:
                await asyncio.sleep(1)
            elif idle:
                await asyncio.sleep(0.2)

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
            "fence": self.fence,
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
                        max_deliver=cfg.max_deliver,
                        **options,
                    )
                    tasks.create_task(self.consume(stop, source))
            with suppress(Fenced):
                await self.state.flush(self.pool)
        finally:
            if self.client is not None:
                await self.client.close()


async def serve(config, stop):
    setup_logging("processor", config.common)
    pool = await db.pool(config.common, workers=1)
    fence = Fence(config.common.site_id, config.fence_age) if config.fence_age else None
    processor = Processor(config, pool, obs.setup("processor", config.common), fence=fence)
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
    lifecycle.run(lambda stop: serve(config, stop), config.common)


if __name__ == "__main__":
    main()
