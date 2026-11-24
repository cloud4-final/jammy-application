"""HTTP entry point for durable ingest in JetStream and direct-db modes."""

import asyncio
import logging
import multiprocessing as mp
import os
import re
import signal
import socket
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from queue import Empty
from threading import BoundedSemaphore
from time import monotonic
from typing import Protocol

import h11
import psycopg
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from ingest.sink_db import EPOCH, DbSink
from ingest.sink_jetstream import JetStreamSink
from jammy_common import db, lifecycle, nats, obs
from jammy_common.admin import AdminServer
from jammy_common.codec import HTTPError, decode_batch
from jammy_common.config import (
    CommonConfig,
    ConfigError,
    config_error_exit,
    env,
    positive_int,
    positive_size,
)
from jammy_common.gate import GateConfig, GateResult, validate
from jammy_common.logging import setup as setup_logging
from jammy_common.pb.envelope_pb2 import EventBatch
from nats.errors import Error as NatsError
from opentelemetry import trace
from opentelemetry.metrics import Meter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from starlette.requests import ClientDisconnect
from uvicorn.protocols.http.h11_impl import H11Protocol

LOGGER = logging.getLogger(__name__)
ROUTE = "/v1/batches"
STORE_ERRORS = (NatsError, psycopg.Error, TimeoutError)


def _duration(name: str, default: str) -> int:
    value = env(name, default)
    match = re.fullmatch(r"([0-9]+)([smh])", value)
    if not match:
        raise ConfigError(f"{name} must be a duration in s, m or h")
    return int(match[1]) * {"s": 1, "m": 60, "h": 3600}[match[2]]


@dataclass(frozen=True)
class IngestConfig:
    common: CommonConfig
    workers: int
    max_body: int
    max_decompressed: int
    max_inflight: int
    gate: GateConfig
    stream: str
    max_age: int
    js_max_bytes: int

    @classmethod
    def from_env(cls):
        common = CommonConfig.from_env()
        workers = positive_int("WEB_CONCURRENCY", os.process_cpu_count() or 1)
        stream = ""
        if common.pipeline_mode == "jetstream":
            env("NATS_URL", required=True)
            stream = env("INGEST_STREAM", required=True)
        else:
            env("DATABASE_URL", required=True)
            db.worker_pool_limits(workers, 0)
        return cls(
            common,
            workers,
            positive_size("INGEST_MAX_BODY_BYTES", "16MiB"),
            positive_size("INGEST_MAX_DECOMPRESSED_BYTES", "64MiB"),
            positive_int("INGEST_MAX_INFLIGHT", 64),
            GateConfig(
                timedelta(seconds=_duration("GATE_MAX_PAST", "24h")),
                timedelta(seconds=_duration("GATE_MAX_FUTURE", "5m")),
            ),
            stream,
            _duration("JS_MAX_AGE", "45m"),
            positive_size("JS_MAX_BYTES", "60GiB"),
        )


@dataclass(frozen=True, slots=True)
class Intake:
    batch: EventBatch
    wire: bytes
    content_type: str
    encoding: str
    ingest_id: str
    received_at: datetime
    result: GateResult

    @property
    def received_at_ms(self) -> int:
        return (self.received_at - EPOCH) // timedelta(milliseconds=1)


class Sink(Protocol):
    async def prepare(self) -> None: ...
    async def check(self) -> bool: ...
    async def close(self) -> None: ...
    async def store(self, intake: Intake) -> bool: ...


@dataclass(slots=True)
class RequestMetrics:
    started: float
    status: int = 500
    http_duration: float = 0
    wire_size: int | None = None
    decoded_size: int | None = None
    accepted: int = 0
    rejected: Counter | None = None
    store_duration: float | None = None


class IntakeMetrics:
    def __init__(self, meter: Meter) -> None:
        self.http = meter.create_histogram("http.server.request.duration", unit="s")
        self.store = meter.create_histogram("jammy_ingest_store_duration", unit="s")
        self.events = meter.create_counter("jammy_ingest_events")
        self.size = meter.create_counter("jammy_ingest_bytes")

    def record(self, sample: RequestMetrics) -> None:
        self.http.record(
            sample.http_duration,
            {
                "http.request.method": "POST",
                "http.route": ROUTE,
                "http.response.status_code": sample.status,
            },
        )
        if sample.wire_size is not None:
            self.size.add(sample.wire_size, {"kind": "wire"})
        if sample.decoded_size is not None:
            self.size.add(sample.decoded_size, {"kind": "decompressed"})
        if sample.store_duration is not None:
            self.store.record(sample.store_duration)
        if sample.accepted:
            self.events.add(sample.accepted, {"result": "accepted", "reason": ""})
        for reason, count in (sample.rejected or {}).items():
            self.events.add(count, {"result": "rejected", "reason": reason})


class IntakeState:
    def __init__(
        self, config: IngestConfig, *, slots=None, ready_event=None, observe=None
    ) -> None:
        self.slots = (
            slots if slots is not None else BoundedSemaphore(config.max_inflight)
        )
        self.ready_event = ready_event
        self.observe = observe
        self.sink: Sink | None = None
        self.ready = False
        self.draining = False

    def update_ready(self, ready: bool) -> None:
        self.ready = ready
        if self.ready_event is not None:
            if ready and not self.draining:
                self.ready_event.set()
            else:
                self.ready_event.clear()

    async def is_ready(self) -> bool:
        return self.ready and not self.draining


def _problem(status: int, code: str, title: str, detail: str, retry: int | None = None):
    return JSONResponse(
        status_code=status,
        media_type="application/problem+json",
        headers={} if retry is None else {"Retry-After": str(retry)},
        content={
            "type": f"https://jammy.dev/problems/{code}",
            "title": title,
            "status": status,
            "detail": detail,
        },
    )


def _unavailable():
    return _problem(
        503, "unavailable", "Unavailable", "Storage cannot accept the batch", 2
    )


async def _read_wire(request: Request, max_body: int) -> bytes:
    wire = bytearray()
    async for part in request.stream():
        if len(wire) + len(part) > max_body:
            raise HTTPError(413, "Request body exceeds the wire size limit")
        wire.extend(part)
    return bytes(wire)


def create_app(config: IngestConfig, state: IntakeState) -> FastAPI:
    app = FastAPI(docs_url=None, openapi_url=None, redoc_url=None)
    tracer = trace.get_tracer(__name__)
    propagator = TraceContextTextMapPropagator()

    @app.post(ROUTE)
    async def submit_batch(request: Request):
        with tracer.start_as_current_span(
            f"POST {ROUTE}",
            context=propagator.extract(request.headers),
            kind=trace.SpanKind.SERVER,
            attributes={"http.request.method": "POST", "http.route": ROUTE},
        ) as span:
            response = await handle_batch(request)
            span.set_attribute("http.response.status_code", response.status_code)
            if response.status_code >= 500:
                span.set_status(trace.StatusCode.ERROR)
            return response

    async def handle_batch(request: Request):
        sample = RequestMetrics(started=monotonic())
        received_at = datetime.now(UTC)
        acquired = False
        try:
            response = None
            if not await state.is_ready() or state.sink is None:
                response = _unavailable()
            elif not state.slots.acquire(False):
                response = _problem(
                    429, "too_many_requests", "Too Many Requests", "Ingest is busy", 1
                )
            else:
                acquired = True
                response = await _accept(
                    request, config, state.sink, received_at, sample
                )
            sample.status = response.status_code
            return response
        finally:
            if acquired:
                state.slots.release()
            if state.observe is not None:
                sample.http_duration = monotonic() - sample.started
                state.observe(sample)

    return app


async def _accept(request, config, sink, received_at, sample):
    try:
        wire = await _read_wire(request, config.max_body)
        sample.wire_size = len(wire)
        media = (
            request.headers.get("content-type", "").partition(";")[0].strip().lower()
        )
        encoding = request.headers.get("content-encoding", "").strip().lower()
        batch, decoded = decode_batch(
            wire, media, encoding, config.max_body, config.max_decompressed
        )
        sample.decoded_size = len(decoded)
    except HTTPError as error:
        if error.status == 413:
            return _problem(413, "payload_too_large", "Payload Too Large", error.msg)
        return _problem(400, "bad_request", "Bad Request", error.msg)
    except ClientDisconnect:
        return _problem(
            400, "bad_request", "Bad Request", "Request body was interrupted"
        )

    result = validate(batch, received_at, config.gate)
    intake = Intake(
        batch,
        wire,
        media,
        encoding,
        config.common.instance_id,
        received_at,
        result,
    )
    started = monotonic()
    try:
        duplicate = await sink.store(intake)
    except STORE_ERRORS as error:
        LOGGER.warning("Batch storage failed (%s)", type(error).__name__)
        return _unavailable()
    finally:
        sample.store_duration = monotonic() - started
    if not duplicate:
        sample.accepted = len(result.accepted)
        sample.rejected = Counter(reason for _, reason in result.rejected)
    return JSONResponse(
        status_code=202,
        content={
            "batch_id": batch.batch_id,
            "accepted": sample.accepted,
            "rejected": sum((sample.rejected or {}).values()),
            "reasons": sample.rejected or {},
            "duplicate": duplicate,
        },
    )


async def _maintain_jetstream(config: IngestConfig, state: IntakeState) -> None:
    sink = None
    prepared = False
    consecutive_timeouts = 0
    while True:
        if sink is None:
            client = await nats.connect(config.common)
            sink = JetStreamSink(
                client, config.stream, config.max_age, config.js_max_bytes
            )
            state.sink = sink
        try:
            if not prepared:
                await sink.prepare()
            prepared = await sink.check()
            state.update_ready(prepared)
            consecutive_timeouts = 0
        except (NatsError, TimeoutError) as error:
            prepared = False
            state.update_ready(False)
            LOGGER.warning("Storage dependency unavailable (%s)", type(error).__name__)
            if isinstance(error, TimeoutError):
                consecutive_timeouts += 1
            else:
                consecutive_timeouts = 0
            if consecutive_timeouts == 3:
                LOGGER.warning("Reconnecting NATS after three consecutive timeouts")
                await sink.close()
                sink = state.sink = None
                consecutive_timeouts = 0
        await asyncio.sleep(1)


async def _maintain(
    config: IngestConfig, state: IntakeState, worker_index: int
) -> None:
    if config.common.pipeline_mode == "jetstream":
        await _maintain_jetstream(config, state)
        return

    pool = await db.pool(
        config.common, workers=config.workers, worker_index=worker_index
    )
    sink = DbSink(pool, config.gate.max_past)
    state.sink = sink
    refresh = 0.0
    while True:
        try:
            if monotonic() >= refresh:
                await sink.prepare()
                refresh = monotonic() + 600
            ready = await sink.check()
            if not ready:
                refresh = 0.0
            state.update_ready(ready)
        except STORE_ERRORS as error:
            state.update_ready(False)
            refresh = 0.0
            LOGGER.warning("Storage dependency unavailable (%s)", type(error).__name__)
        await asyncio.sleep(1)


class Server(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        # The common lifecycle owns signals and the drain deadline.
        yield


class HeaderTimeoutProtocol(H11Protocol):
    header_timer: asyncio.TimerHandle | None = None

    def connection_made(self, transport):
        super().connection_made(transport)
        self.header_timer = self.loop.call_later(5, transport.close)

    def connection_lost(self, exc):
        self.cancel_header_timer()
        super().connection_lost(exc)

    def cancel_header_timer(self):
        if self.header_timer is not None:
            self.header_timer.cancel()
            self.header_timer = None

    def handle_events(self):
        if self.conn.their_state is h11.IDLE and self.header_timer is None:
            self.header_timer = self.loop.call_later(5, self.transport.close)
        prior_cycle = self.cycle
        super().handle_events()
        if self.cycle is not prior_cycle:
            self.cancel_header_timer()


async def _serve_worker(
    config, state, stop, listener=None, admin=None, worker_index=0
) -> None:
    server = Server(
        uvicorn.Config(
            create_app(config, state),
            host="0.0.0.0",
            port=config.common.http_port,
            lifespan="off",
            http=HeaderTimeoutProtocol,
            log_config=None,
            access_log=False,
        )
    )
    serving = asyncio.create_task(
        server.serve(sockets=[listener] if listener else None)
    )
    dependency = asyncio.create_task(_maintain(config, state, worker_index))
    stopping = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            {serving, stopping, dependency}, return_when=asyncio.FIRST_COMPLETED
        )
        if dependency in done:
            await dependency
        state.draining = True
        state.update_ready(False)
        if admin is not None:
            admin.set_draining()
        server.should_exit = True
        await serving
    finally:
        stopping.cancel()
        dependency.cancel()
        await asyncio.gather(stopping, dependency, return_exceptions=True)
        if state.sink is not None:
            await state.sink.close()


def _child(config, slots, ready, samples, listener, worker_index) -> None:
    setup_logging("ingest", config.common)
    obs.setup_tracing("ingest", config.common)
    state = IntakeState(config, slots=slots, ready_event=ready, observe=samples.put)
    lifecycle.run(
        lambda stop: _serve_worker(
            config, state, stop, listener, worker_index=worker_index
        ),
        config.common,
    )


async def _supervise(config, stop, metrics) -> None:
    ctx = mp.get_context("spawn")
    slots = ctx.BoundedSemaphore(config.max_inflight)
    ready = [ctx.Event() for _ in range(config.workers)]
    samples = ctx.Queue()

    async def all_ready() -> bool:
        return all(flag.is_set() for flag in ready)

    admin = AdminServer(config.common.admin_port, {"workers": all_ready})
    await admin.start()
    workers = []
    draining = False
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("0.0.0.0", config.common.http_port))
        listener.listen()
        try:
            for index, flag in enumerate(ready):
                child = ctx.Process(
                    target=_child, args=(config, slots, flag, samples, listener, index)
                )
                child.start()
                workers.append(child)
            while workers:
                if stop.is_set() and not draining:
                    draining = True
                    admin.set_draining()
                    for child in workers:
                        if child.is_alive():
                            child.terminate()
                while True:
                    try:
                        metrics.record(samples.get_nowait())
                    except Empty:
                        break
                exited = [child for child in workers if child.exitcode is not None]
                # Spawned workers can receive SIGTERM before installing their handler.
                if exited and (
                    not draining
                    or any(child.exitcode not in (0, -signal.SIGTERM) for child in exited)
                ):
                    raise RuntimeError("An ingest worker exited unexpectedly")
                for child in exited:
                    child.join()
                    workers.remove(child)
                if workers:
                    await asyncio.sleep(0.05)
        finally:
            for child in workers:
                if child.is_alive():
                    child.terminate()
                await asyncio.to_thread(child.join)
            await admin.close()
            samples.close()


async def _run(config: IngestConfig, stop: asyncio.Event) -> None:
    metrics = IntakeMetrics(obs.setup("ingest", config.common))
    if config.workers > 1:
        await _supervise(config, stop, metrics)
    else:
        state = IntakeState(config, observe=metrics.record)
        admin = AdminServer(config.common.admin_port, {"storage": state.is_ready})
        await admin.start()
        try:
            await _serve_worker(config, state, stop, admin=admin)
        finally:
            await admin.close()


@config_error_exit
def main() -> None:
    config = IngestConfig.from_env()
    setup_logging("ingest", config.common)
    lifecycle.run(lambda stop: _run(config, stop), config.common)


if __name__ == "__main__":
    main()
