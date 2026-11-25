import logging as std_logging
import os
import select
import signal
import subprocess
import sys
import time

import pytest
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import CollectorRegistry

from jammy_common import lifecycle, logging, obs
from jammy_common.config import CommonConfig


def test_lifecycle_flushes_telemetry_before_returning(monkeypatch):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://unused:4317")
    monkeypatch.setenv("OTEL_BSP_SCHEDULE_DELAY", "60000")
    monkeypatch.setenv("OTEL_BLRP_SCHEDULE_DELAY", "60000")
    spans = InMemorySpanExporter()
    logs = InMemoryLogRecordExporter()
    registry = CollectorRegistry()
    tracer_providers = []
    monkeypatch.setattr(obs, "OTLPSpanExporter", lambda **_: spans)
    monkeypatch.setattr(logging, "OTLPLogExporter", lambda **_: logs)
    monkeypatch.setattr(obs.trace, "set_tracer_provider", tracer_providers.append)
    monkeypatch.setattr(obs.metrics, "set_meter_provider", lambda _: None)
    monkeypatch.setattr(obs, "REGISTRY", registry)
    cfg = CommonConfig.from_env()

    async def main(_stop):
        logging.setup("lifecycle-test", cfg)
        meter = obs.setup("lifecycle-test", cfg)
        with tracer_providers[0].get_tracer("test").start_as_current_span("tail-span"):
            std_logging.getLogger("test").info("tail-log")
            meter.create_counter("jammy_tail").add(1)
        assert registry.get_sample_value("jammy_tail_total", {
            "service": "lifecycle-test", "instance": cfg.instance_id, "site": "site-a",
        }) == 1
        assert not spans.get_finished_spans()
        assert not logs.get_finished_logs()

    root = std_logging.getLogger()
    original_handlers, original_level = root.handlers[:], root.level
    try:
        lifecycle.run(main, cfg)
    finally:
        root.handlers = original_handlers
        root.setLevel(original_level)

    assert [span.name for span in spans.get_finished_spans()] == ["tail-span"]
    assert len(logs.get_finished_logs()) == 1
    assert list(registry.collect()) == []


def test_lifecycle_bounds_stalled_shutdown_hook(monkeypatch):
    from threading import Event

    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("SHUTDOWN_TIMEOUT_SECONDS", "1")
    cfg = CommonConfig.from_env()
    release = Event()
    fast_ran = Event()

    async def main(_stop):
        lifecycle.on_shutdown(lambda: release.wait(5))
        lifecycle.on_shutdown(fast_ran.set)

    started = time.monotonic()
    try:
        lifecycle.run(main, cfg)
    finally:
        release.set()

    elapsed = time.monotonic() - started
    assert fast_ran.is_set()
    assert elapsed < 2.5


@pytest.mark.parametrize(
    ("name", "shutdown_timeout", "after_stop", "exit_code"),
    [
        ("sigterm_sets_stop_and_exits_zero", 5, "return", 0),
        ("shutdown_timeout_forces_exit_one", 1, "await asyncio.Event().wait()", 1),
    ],
    ids=["sigterm_sets_stop_and_exits_zero", "shutdown_timeout_forces_exit_one"],
)
def test_lifecycle(name, shutdown_timeout, after_stop, exit_code):
    script = f"""
import asyncio

from jammy_common.config import CommonConfig
from jammy_common.lifecycle import run

async def main(stop):
    print("started", flush=True)
    await stop.wait()
    print("stop set", flush=True)
    {after_stop}

run(main, CommonConfig.from_env())
"""
    with subprocess.Popen(
        [sys.executable, "-c", script],
        env={**os.environ, "SITE_ID": "site-a", "SHUTDOWN_TIMEOUT_SECONDS": str(shutdown_timeout)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert select.select([process.stdout], [], [], 10)[0], name
            assert process.stdout.readline().strip() == "started", name
            started = time.monotonic()
            process.send_signal(signal.SIGTERM)
            out, err = process.communicate(timeout=shutdown_timeout + 3)
            assert process.returncode == exit_code, err
            assert "stop set" in out
            if exit_code == 0:
                assert time.monotonic() - started < shutdown_timeout
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
