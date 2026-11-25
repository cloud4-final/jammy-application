import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import httpx
from botocore.exceptions import EndpointConnectionError
from dispatcher import main
from dispatcher.commands import Dispatcher
from jammy_common import admin as admin_module
from jammy_common import obs
from jammy_common.config import CommonConfig
from prometheus_client import CollectorRegistry
from prometheus_client.parser import text_string_to_metric_families


def test_dispatcher_metrics_expose_expected_names_labels_and_buckets(monkeypatch):
    registry = CollectorRegistry()
    monkeypatch.setattr(obs, "REGISTRY", registry)
    monkeypatch.setattr(admin_module, "REGISTRY", registry)
    monkeypatch.setattr(obs.metrics, "set_meter_provider", lambda provider: None)

    monkeypatch.setenv("SITE_ID", "test")
    monkeypatch.setenv("INSTANCE_ID", "dispatcher-test")
    monkeypatch.setenv("PLANT_GATEWAY_URL", "http://plantsim.test")
    monkeypatch.setenv("LEASE_MODE", "disabled")
    monkeypatch.setenv("SQS_QUEUE_URL", "http://127.0.0.1:1/queue")
    monkeypatch.delenv("ADMIN_PORT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    payload = json.dumps(
        {
            "instruction_id": "inst-sqs-1",
            "target_reduction_kw": "10.000",
            "valid_from": "2026-11-23T01:00:00Z",
            "valid_until": "2026-11-23T02:00:00Z",
        }
    )
    conflicting = json.dumps(
        {
            "instruction_id": "inst-sqs-1",
            "target_reduction_kw": "1.000",
            "valid_from": "2026-11-23T01:00:00Z",
            "valid_until": "2026-11-23T02:00:00Z",
        }
    )
    payload_hash = hashlib.sha256(payload.encode()).hexdigest()

    class FakeQueue:
        def __init__(self):
            self.calls = 0

        def receive_message(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "Messages": [
                        {"Body": payload, "ReceiptHandle": "r1"},
                        {"Body": payload, "ReceiptHandle": "r2"},
                        {"Body": conflicting, "ReceiptHandle": "r3"},
                        {"Body": "{}", "ReceiptHandle": "r4"},
                    ]
                }
            raise EndpointConnectionError(endpoint_url="http://127.0.0.1:1")

        def delete_message(self, **_kwargs):
            pass

    class FakeCursor:
        def __init__(self, row):
            self._row = row

        async def fetchone(self):
            return self._row

    class FakeConn:
        def __init__(self):
            self.inserted = False
            self.checked_duplicate = False

        async def execute(self, sql, params=None):
            if "INSERT INTO jammy.instructions" in sql:
                if not self.inserted:
                    self.inserted = True
                    return FakeCursor((params[0],))
                return FakeCursor(None)
            if "SELECT body_sha256" in sql:
                if not self.checked_duplicate:
                    self.checked_duplicate = True
                    return FakeCursor((payload_hash,))
                return FakeCursor(("mismatch",))
            return FakeCursor(None)

    class FakeLeader:
        def __init__(self, _pool, holder="dispatcher"):
            self.holder = holder
            self.generation = None
            self._conn = FakeConn()

        async def try_acquire(self):
            self.generation = 1
            return 1

        async def is_held(self):
            return self.generation is not None

        async def release(self, *, generation=None):
            if generation is None or generation == self.generation:
                self.generation = None

        @asynccontextmanager
        async def transaction(self):
            yield self._conn

    class FakePool:
        async def close(self):
            pass

    statuses = iter([200, 409, 500])

    def http_handler(request):
        code = next(statuses, None)
        if code is None:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(code)

    async def exercise():
        stop = asyncio.Event()
        admin_server = None
        real_admin = main.AdminServer

        def capture_admin(_port, checks):
            nonlocal admin_server
            admin_server = real_admin(0, checks)
            return admin_server

        async def fake_pool(_cfg, workers=1):
            return FakePool()

        async def fake_check_compliance(_conn, **_kwargs):
            pass

        dispatched = False

        async def fake_dispatch_one(self):
            nonlocal dispatched
            if dispatched:
                return False
            dispatched = True
            inst = {
                "instruction_id": str(uuid4()),
                "valid_until": datetime.now(UTC) + timedelta(minutes=5),
            }
            cmd = {
                "command_id": uuid4(),
                "asset_id": "solar-1",
                "reduction_kw": Decimal("10.000"),
            }
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(http_handler)
            ) as mock_http:
                self.http = mock_http
                for _ in range(4):
                    await self._send(inst, cmd, 1, 0)
            self.duration.record(0.015)
            return True

        monkeypatch.setattr(main, "AdminServer", capture_admin)
        monkeypatch.setattr(main.db, "pool", fake_pool)
        monkeypatch.setattr(main, "Leader", FakeLeader)
        monkeypatch.setattr(main, "check_compliance", fake_check_compliance)
        monkeypatch.setattr(main.boto3, "client", lambda *_a, **_kw: FakeQueue())
        monkeypatch.setattr(Dispatcher, "dispatch_one", fake_dispatch_one)

        scrapes = []

        async def wait_for_cycle(stop_waiter, *, timeout):
            stop_waiter.close()
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{admin_server.port}"
            ) as client:
                response = await client.get("/metrics")
                assert response.status_code == 200
                scrapes.append(response.text)
            if len(scrapes) == 2:
                stop.set()

        monkeypatch.setattr(
            main,
            "asyncio",
            SimpleNamespace(
                Event=asyncio.Event,
                TaskGroup=asyncio.TaskGroup,
                timeout=asyncio.timeout,
                wait_for=wait_for_cycle,
            ),
        )
        await main.serve(stop, CommonConfig.from_env())
        return scrapes

    first_scrape, second_scrape = asyncio.run(exercise())
    first_families = {
        family.name: family for family in text_string_to_metric_families(first_scrape)
    }
    second_families = {
        family.name: family for family in text_string_to_metric_families(second_scrape)
    }

    for gauge_name in ("jammy_dispatcher_is_leader", "jammy_dispatcher_lease_active"):
        sample = first_families[gauge_name].samples[0]
        assert sample.value == 1.0
        assert sample.labels == {
            "service": "dispatcher",
            "site": "test",
            "instance": "dispatcher-test",
        }

    command_results = {
        sample.labels["result"]: sample.value
        for sample in second_families["jammy_dispatcher_commands"].samples
        if sample.name == "jammy_dispatcher_commands_total"
    }
    assert command_results == {"acked": 1.0, "stale_epoch": 1.0, "timeout": 1.0, "error": 1.0}

    sqs_results = {
        sample.labels["result"]: sample.value
        for sample in second_families["jammy_dispatcher_sqs"].samples
        if sample.name == "jammy_dispatcher_sqs_total"
    }
    assert sqs_results == {
        "inserted": 1.0,
        "duplicate": 1.0,
        "conflict": 1.0,
        "invalid": 1.0,
        "error": 1.0,
    }

    buckets = [
        sample.labels
        for sample in second_families["jammy_dispatcher_dispatch_duration_seconds"].samples
        if sample.name == "jammy_dispatcher_dispatch_duration_seconds_bucket"
    ]
    assert [labels["le"] for labels in buckets] == [
        "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1",
        "0.25", "0.5", "1.0", "2.5", "5.0", "10.0", "+Inf",
    ]
    assert all(
        labels["service"] == "dispatcher"
        and labels["site"] == "test"
        and labels["instance"] == "dispatcher-test"
        for labels in buckets
    )


def test_leader_and_lease_gauges_do_not_flicker_to_zero_during_checks(monkeypatch):
    registry = CollectorRegistry()
    monkeypatch.setattr(obs, "REGISTRY", registry)
    monkeypatch.setattr(admin_module, "REGISTRY", registry)
    monkeypatch.setattr(obs.metrics, "set_meter_provider", lambda provider: None)

    monkeypatch.setenv("SITE_ID", "test")
    monkeypatch.setenv("INSTANCE_ID", "dispatcher-test")
    monkeypatch.setenv("PLANT_GATEWAY_URL", "http://plantsim.test")
    monkeypatch.setenv("LEASE_MODE", "disabled")
    monkeypatch.delenv("SQS_QUEUE_URL", raising=False)
    monkeypatch.delenv("ADMIN_PORT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    class FakeConn:
        async def execute(self, *_args, **_kwargs):
            return None

    admin_server = None
    real_admin = main.AdminServer
    observations = {}
    cycle = 0

    async def scrape_gauges():
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{admin_server.port}"
        ) as client:
            response = await client.get("/metrics")
        assert response.status_code == 200
        families = {
            family.name: family
            for family in text_string_to_metric_families(response.text)
        }
        return (
            families["jammy_dispatcher_is_leader"].samples[0].value,
            families["jammy_dispatcher_lease_active"].samples[0].value,
        )

    class FakeLeader:
        def __init__(self, _pool, holder="dispatcher"):
            self.holder = holder
            self.generation = None

        async def try_acquire(self):
            if cycle == 1:
                observations["during_try_acquire"] = await scrape_gauges()
            if cycle >= 2:
                self.generation = None
                return None
            self.generation = 1
            return 1

        async def release(self, *, generation=None):
            if generation is None or generation == self.generation:
                self.generation = None

        @asynccontextmanager
        async def transaction(self):
            yield FakeConn()

    class FakePool:
        async def close(self):
            pass

    async def exercise():
        nonlocal admin_server, cycle
        stop = asyncio.Event()

        def capture_admin(_port, checks):
            nonlocal admin_server
            admin_server = real_admin(0, checks)
            return admin_server

        async def fake_pool(_cfg, workers=1):
            return FakePool()

        async def fake_active_lease(self):
            if cycle == 1:
                observations["during_active_lease"] = await scrape_gauges()
            return SimpleNamespace(active=True, generation=1, epoch=0)

        async def fake_dispatch_one(self):
            return False

        async def fake_expire(self):
            pass

        async def fake_check_compliance(_conn, **_kwargs):
            pass

        async def wait_for_cycle(stop_waiter, *, timeout):
            nonlocal cycle
            stop_waiter.close()
            observations[f"after_cycle_{cycle}"] = await scrape_gauges()
            cycle += 1
            if cycle == 3:
                stop.set()

        monkeypatch.setattr(main, "AdminServer", capture_admin)
        monkeypatch.setattr(main.db, "pool", fake_pool)
        monkeypatch.setattr(main, "Leader", FakeLeader)
        monkeypatch.setattr(main, "check_compliance", fake_check_compliance)
        monkeypatch.setattr(Dispatcher, "active_lease", fake_active_lease)
        monkeypatch.setattr(Dispatcher, "dispatch_one", fake_dispatch_one)
        monkeypatch.setattr(Dispatcher, "expire", fake_expire)
        monkeypatch.setattr(
            main,
            "asyncio",
            SimpleNamespace(
                Event=asyncio.Event,
                TaskGroup=asyncio.TaskGroup,
                timeout=asyncio.timeout,
                wait_for=wait_for_cycle,
            ),
        )
        await main.serve(stop, CommonConfig.from_env())

    asyncio.run(exercise())
    assert observations == {
        "after_cycle_0": (1.0, 1.0),
        "during_try_acquire": (1.0, 1.0),
        "during_active_lease": (1.0, 1.0),
        "after_cycle_1": (1.0, 1.0),
        "after_cycle_2": (0.0, 0.0),
    }
