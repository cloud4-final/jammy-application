import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from uuid import uuid4

import httpx
import nats
import pytest
from test_ingest_jetstream import PROTOBUF, batch, broker_url, post
from testcontainers.core.container import DockerContainer


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@contextmanager
def service(nats_url, workers=1, entrypoint=None):
    public = free_port()
    admin = free_port()
    stream = f"raw-{uuid4().hex[:8]}"
    env = os.environ | {
        "NATS_URL": nats_url,
        "SITE_ID": "fixture",
        "INGEST_STREAM": stream,
        "WEB_CONCURRENCY": str(workers),
        "HTTP_PORT": str(public),
        "ADMIN_PORT": str(admin),
        "PIPELINE_MODE": "jetstream",
    }
    command = [str(entrypoint)] if entrypoint else ["-m", "ingest.main"]
    with subprocess.Popen([sys.executable, *command], env=env) as process:
        try:
            yield (
                process,
                f"http://localhost:{public}",
                f"http://localhost:{admin}",
                stream,
            )
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=26)


def wait_http(url, status):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=0.5).status_code == status:
                return
        except httpx.TransportError:
            pass
        time.sleep(0.1)
    raise AssertionError(f"{url} never returned {status}")


def test_nats_down_readyz_503_then_recovers():
    broker = (
        DockerContainer("nats:2.15.3")
        .with_command("--jetstream --store_dir /data")
        .with_exposed_ports(4222)
    )
    with broker:
        url = broker_url(broker)
        container = broker.get_wrapped_container()
        container.pause()
        try:
            with service(url) as (_, _, admin, _):
                wait_http(f"{admin}/readyz", 503)
                container.unpause()
                wait_http(f"{admin}/readyz", 200)
        finally:
            container.reload()
            if container.status == "paused":
                container.unpause()


@pytest.mark.parametrize("workers", [1, 2])
def test_metrics_expose_http_semantic_labels(workers):
    broker = (
        DockerContainer("nats:2.15.3")
        .with_command("--jetstream --store_dir /data")
        .with_exposed_ports(4222)
    )
    with broker, service(broker_url(broker), workers) as (_, public, admin, _):
        wait_http(f"{admin}/readyz", 200)
        response = httpx.post(
            f"{public}/v1/batches",
            content=batch().SerializeToString(),
            headers={"Content-Type": PROTOBUF},
        )
        assert response.status_code == 202
        metrics = ""
        for _ in range(20):
            metrics = httpx.get(f"{admin}/metrics").text
            if "http_server_request_duration_seconds_count" in metrics:
                break
            time.sleep(0.05)
        histogram = next(
            line
            for line in metrics.splitlines()
            if line.startswith("http_server_request_duration_seconds_count{")
        )
        assert 'http_request_method="POST"' in histogram
        assert 'http_route="/v1/batches"' in histogram
        assert 'http_response_status_code="202"' in histogram
        assert 'result="accepted"' in metrics
        assert "jammy_ingest_inflight_requests{" in metrics
        assert "jammy_ingest_inflight_limit{" in metrics


def test_sigterm_drains_inflight():
    broker = DockerContainer("nats:2.15.3").with_command(
        "--jetstream --store_dir /data"
    )
    broker.with_exposed_ports(4222)
    with broker, service(broker_url(broker)) as (process, public, admin, stream):
        wait_http(f"{admin}/readyz", 200)
        item = batch(blob=os.urandom(65536))
        wire = item.SerializeToString()

        def body():
            yield wire[:1024]
            time.sleep(0.2)
            process.send_signal(signal.SIGTERM)
            wait_http(f"{admin}/readyz", 503)
            yield wire[1024:]

        with httpx.Client(timeout=10) as client:
            response = client.post(
                f"{public}/v1/batches",
                content=body(),
                headers={"Content-Type": PROTOBUF},
            )
            assert response.status_code == 202
        assert process.wait(timeout=26) == 0

        async def check():
            nc = await nats.connect(broker_url(broker))
            try:
                stored = await nc.jetstream().get_msg(stream, seq=1)
                assert stored.data == wire
            finally:
                await nc.close()

        asyncio.run(check())


def test_sigterm_during_worker_import_exits_cleanly(tmp_path):
    entrypoint = tmp_path / "start_ingest.py"
    marker = entrypoint.with_suffix(".importing")
    entrypoint.write_text(
        "import signal\n"
        "from pathlib import Path\n"
        "if __name__ == '__mp_main__':\n"
        "    Path(__file__).with_suffix('.importing').touch()\n"
        "    signal.pause()\n"
        "from ingest.main import main\n"
        "if __name__ == '__main__':\n"
        "    main()\n"
    )
    with service("nats://127.0.0.1:1", workers=2, entrypoint=entrypoint) as (
        process,
        _,
        _,
        _,
    ):
        deadline = time.monotonic() + 15
        while (
            not marker.exists()
            and process.poll() is None
            and time.monotonic() < deadline
        ):
            time.sleep(0.02)
        assert marker.exists(), "workers did not start importing"
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=10) == 0


def test_main_reports_config_error_without_traceback(monkeypatch):
    monkeypatch.setenv("SITE_ID", "")
    monkeypatch.delenv("SITE_ID_FILE", raising=False)
    result = subprocess.run(
        [sys.executable, "-m", "ingest.main"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 1
    assert result.stderr == "SITE_ID is required\n"


@pytest.mark.parametrize(
    "name",
    [
        "INGEST_MAX_INFLIGHT",
        "HTTP_PORT",
        "ADMIN_PORT",
        "INSTANCE_ID",
        "INGEST_MAX_BODY_BYTES",
        "INGEST_MAX_DECOMPRESSED_BYTES",
        "JS_MAX_BYTES",
        "QUARANTINE_MAX_AGE",
        "QUARANTINE_MAX_BYTES",
    ],
)
def test_invalid_parity_config(monkeypatch, name):
    from ingest.main import IngestConfig
    from jammy_common.config import ConfigError

    for key, value in {
        "SITE_ID": "test",
        "NATS_URL": "nats://localhost",
        "INGEST_STREAM": "raw",
    }.items():
        monkeypatch.setenv(key, value)
    value = {"INSTANCE_ID": "", "QUARANTINE_MAX_AGE": "0h"}.get(name, "0")
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigError, match=name):
        IngestConfig.from_env()


def test_direct_db_refuses_more_workers_than_pool_connections(monkeypatch):
    from ingest.main import IngestConfig
    from jammy_common.config import ConfigError

    for key, value in {
        "SITE_ID": "test",
        "PIPELINE_MODE": "direct-db",
        "DATABASE_URL": "postgresql://localhost/test",
        "DB_POOL_MAX": "2",
        "WEB_CONCURRENCY": "3",
    }.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(ConfigError, match="DB_POOL_MAX"):
        IngestConfig.from_env()


def test_unready_does_not_receive_body(monkeypatch):
    from ingest.main import IngestConfig, IntakeState, create_app

    for key, value in {
        "SITE_ID": "test",
        "NATS_URL": "nats://localhost",
        "INGEST_STREAM": "raw",
    }.items():
        monkeypatch.setenv(key, value)
    config = IngestConfig.from_env()
    app = create_app(config, IntakeState(config))

    async def run():
        async def body():
            pytest.fail("unready ingest read the request body")
            yield b""

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post("/v1/batches", content=body())
        assert response.status_code == 503

    asyncio.run(run())


def test_pool_limits_split_exactly(monkeypatch):
    from jammy_common.db import worker_pool_limits

    monkeypatch.setenv("DB_POOL_MIN", "2")
    monkeypatch.setenv("DB_POOL_MAX", "11")
    assert [worker_pool_limits(3, i) for i in range(3)] == [(1, 4), (1, 4), (0, 3)]


def test_incomplete_http_headers_close_at_deadline():
    with service("nats://127.0.0.1:1") as (_, public, admin, _):
        wait_http(f"{admin}/livez", 200)
        wait_http(f"{public}/v1/batches", 405)
        port = int(public.rsplit(":", 1)[1])
        with socket.create_connection(("127.0.0.1", port), timeout=7) as client:
            started = time.monotonic()
            client.sendall(b"POST /v1/batches HTTP/1.1\r\nHost: localhost\r\n")
            assert client.recv(1) == b""
            assert 4.5 <= time.monotonic() - started < 6.5


def test_request_opens_server_span_and_propagates_context(monkeypatch):
    from ingest.main import IngestConfig, IntakeState, create_app
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from opentelemetry.trace import SpanKind, StatusCode

    for key, value in {
        "SITE_ID": "test",
        "NATS_URL": "nats://localhost",
        "INGEST_STREAM": "raw",
    }.items():
        monkeypatch.setenv(key, value)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr("ingest.main.trace.get_tracer", provider.get_tracer)
    config = IngestConfig.from_env()

    async def run():
        app = create_app(config, IntakeState(config))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/v1/batches",
                headers={
                    "traceparent": "00-00000000000000000000000000000042-0000000000000023-01",
                },
            )
        assert response.status_code == 503

    asyncio.run(run())
    (span,) = exporter.get_finished_spans()
    assert span.kind == SpanKind.SERVER
    assert span.parent.span_id == 0x23
    assert span.context.trace_id == 0x42
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["http.response.status_code"] == 503
    provider.shutdown()


def test_child_initializes_tracing_and_uses_its_pool_index(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from ingest import main

    config = SimpleNamespace(common=object())
    calls = []
    serve = AsyncMock(side_effect=lambda *_args, **_kwargs: calls.append("serve"))
    monkeypatch.setattr(main, "setup_logging", lambda *_: None)
    monkeypatch.setattr(main.obs, "setup_tracing", lambda *_: calls.append("tracing"))
    monkeypatch.setattr(main, "_serve_worker", serve)
    monkeypatch.setattr(
        main.lifecycle, "run", lambda target, _: asyncio.run(target(None))
    )

    slots = object()
    monkeypatch.setattr(main, "IntakeState", lambda *_args, **_kwargs: object())
    main._child(config, slots, object(), SimpleNamespace(put=None), object(), 2)

    assert calls == ["tracing", "serve"]
    assert serve.await_args.kwargs == {"worker_index": 2}


def test_inflight_gauges_track_active_requests(monkeypatch):
    from ingest.main import IngestConfig, IntakeMetrics, IntakeState, create_app
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    for key, value in {
        "SITE_ID": "test",
        "PIPELINE_MODE": "direct-db",
        "DATABASE_URL": "postgresql://localhost/test",
        "INGEST_MAX_INFLIGHT": "3",
    }.items():
        monkeypatch.setenv(key, value)
    config = IngestConfig.from_env()
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    metrics = IntakeMetrics(provider.get_meter("ingest"), config.max_inflight)
    state = IntakeState(config, observe=metrics.record)
    metrics.set_inflight_reader(lambda: state.inflight_count)

    def gauge_value(name: str) -> int:
        data = reader.get_metrics_data()
        for resource_metrics in data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if metric.name == name:
                        return metric.data.data_points[0].value
        raise AssertionError(f"metric {name} not found")

    async def run():
        entered = asyncio.Event()
        release = asyncio.Event()

        class DelayedSink:
            async def store(self, intake):
                entered.set()
                await release.wait()
                return False

        state.sink = DelayedSink()
        state.ready = True
        assert gauge_value("jammy_ingest_inflight_requests") == 0
        assert gauge_value("jammy_ingest_inflight_limit") == 3

        transport = httpx.ASGITransport(app=create_app(config, state))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://ingest"
        ) as client:
            pending = asyncio.create_task(post(client, batch()))
            await asyncio.wait_for(entered.wait(), 2)
            try:
                assert gauge_value("jammy_ingest_inflight_requests") == 1
            finally:
                release.set()
                response, _ = await pending
            assert response.status_code == 202
        assert gauge_value("jammy_ingest_inflight_requests") == 0

    try:
        asyncio.run(run())
    finally:
        provider.shutdown()
