import asyncio
import json
import re
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import psycopg
import pytest
from instruction_api.hmac_auth import HMACAuth, sign
from instruction_api.main import create_app, serve
from jammy_common import db
from jammy_common.config import CommonConfig, ConfigError
from opentelemetry import trace
from opentelemetry.metrics import NoOpMeter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer

PATH = "/v1/instructions"
KEY = b"instruction-test-key"


@pytest.fixture(scope="module")
def dsn():
    root = Path(__file__).resolve().parents[3]
    container = DockerContainer("postgres:17.12-trixie").with_env("POSTGRES_PASSWORD", "test")
    with container.with_exposed_ports(5432):
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5432)
        uri = f"postgresql://postgres:test@{host}:{port}/postgres?connect_timeout=2"
        deadline = time.monotonic() + 120
        while True:
            try:
                connection = psycopg.connect(uri)
                break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        with connection:
            for migration in sorted((root / "contracts/db/migrations").glob("*.up.sql")):
                connection.execute(migration.read_text())
        yield uri


def body(**changes):
    return json.dumps({
        "instruction_id": str(uuid4()),
        "target_reduction_kw": "12.500",
        "valid_from": "2026-11-23T01:00:00Z",
        "valid_until": "2026-11-23T02:00:00Z",
        **changes,
    }).encode()


async def send(client, data=b"", *, path=PATH, method="POST", offset=0, key="k", signature=None,
               lease_epoch=None):
    timestamp = int(time.time()) + offset
    epoch_header = {} if lease_epoch is None else {"X-Jammy-Lease-Epoch": lease_epoch}
    return await client.request(method, path, content=data, headers={
        "Content-Type": "application/json",
        "X-Jammy-Key-Id": key,
        "X-Jammy-Timestamp": str(timestamp),
        "X-Jammy-Signature": signature or sign(KEY, timestamp, method, path, data),
        **epoch_header,
    })


def with_api(dsn, callback, meter=None):
    async def run():
        async with AsyncConnectionPool(dsn, open=False) as pool:
            app = create_app(
                pool, HMACAuth({"k": KEY}, 300), meter or NoOpMeter("instruction-api"), site_id="a",
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test",
            ) as client:
                await callback(client, pool)

    asyncio.run(run())


def problem(response, status, code):
    assert response.status_code == status
    assert response.headers["content-type"] == "application/problem+json"
    assert set(response.json()) == {"type", "title", "status", "detail"}
    assert response.json()["type"] == f"https://jammy.dev/problems/{code}"
    assert response.json()["status"] == status


def test_post_uses_optional_lease_epoch_without_changing_signature():
    async def exercise():
        pool = MagicMock()
        cursor = AsyncMock()
        conn = pool.connection.return_value.__aenter__.return_value
        conn.cursor = MagicMock()
        conn.cursor.return_value.__aenter__.return_value = cursor
        app = create_app(pool, HMACAuth({"k": KEY}, 300), NoOpMeter("instruction-api"), site_id="a")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test",
        ) as client:
            cursor.fetchone.return_value = {"epoch": 2}
            problem(await send(client, body(), lease_epoch="1"), 409, "stale_epoch")
            assert cursor.execute.await_count == 1
            assert cursor.execute.await_args.args[1] == ("a",)
            for value in ("", "-1", "1.5", "9223372036854775808"):
                problem(await send(client, body(), lease_epoch=value), 400, "bad_request")
            assert cursor.execute.await_count == 1

            for value, row in (("2", {"epoch": 2}), ("3", {"epoch": 2}), ("1", None), (None, None)):
                cursor.reset_mock()
                cursor.fetchone.side_effect = (
                    [{"status": "accepted"}]
                    if value is None
                    else [row, {"status": "accepted"}]
                )
                assert (await send(client, body(), lease_epoch=value)).status_code == 202
                assert cursor.execute.await_count == (1 if value is None else 2)
                assert cursor.execute.await_args.args[1][-1] == (None if value is None else int(value))

    asyncio.run(exercise())


@pytest.mark.parametrize("method", ["POST", "GET"])
@pytest.mark.parametrize(("setting", "limit"), [(None, 65536), ("1KiB", 1024)])
def test_oversized_body_stops_at_first_chunk_over_limit(method, setting, limit, monkeypatch):
    monkeypatch.delenv("INSTRUCTION_MAX_BODY_BYTES_FILE", raising=False)
    if setting is None:
        monkeypatch.delenv("INSTRUCTION_MAX_BODY_BYTES", raising=False)
    else:
        monkeypatch.setenv("INSTRUCTION_MAX_BODY_BYTES", setting)

    async def exercise():
        async def content():
            yield b"x" * limit
            yield b"x"
            pytest.fail("read another chunk after exceeding the body limit")

        path = PATH if method == "POST" else f"{PATH}/{uuid4()}"
        timestamp = int(time.time())
        app = create_app(None, HMACAuth({"k": KEY}, 300), NoOpMeter("instruction-api"), site_id="a")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test",
        ) as client:
            response = await client.request(method, path, content=content(), headers={
                "X-Jammy-Key-Id": "k",
                "X-Jammy-Timestamp": str(timestamp),
                "X-Jammy-Signature": "0" * 64,
            })
        problem(response, 413, "body_too_large")

    asyncio.run(exercise())


@pytest.mark.parametrize("method", ["POST", "GET"])
@pytest.mark.parametrize(("header", "value", "code"), [
    ("X-Jammy-Key-Id", None, "bad_signature"),
    ("X-Jammy-Key-Id", "unknown", "unknown_key"),
    ("X-Jammy-Timestamp", None, "bad_signature"),
    ("X-Jammy-Timestamp", "invalid", "bad_signature"),
    ("X-Jammy-Timestamp", "0", "stale_timestamp"),
    ("X-Jammy-Signature", None, "bad_signature"),
])
def test_rejected_hmac_headers_do_not_read_body(method, header, value, code):
    async def exercise():
        async def content():
            pytest.fail("read the body before rejecting the HMAC headers")
            yield b""

        headers = {
            "X-Jammy-Key-Id": "k",
            "X-Jammy-Timestamp": str(int(time.time())),
            "X-Jammy-Signature": "0" * 64,
        }
        if value is None:
            del headers[header]
        else:
            headers[header] = value
        path = PATH if method == "POST" else f"{PATH}/{uuid4()}"
        app = create_app(None, HMACAuth({"k": KEY}, 300), NoOpMeter("instruction-api"), site_id="a")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test",
        ) as client:
            request = client.build_request(method, path, content=content(), headers=headers)
            response = await client.send(request)
        problem(response, 401, code)

    asyncio.run(exercise())


def test_new_instruction_202(dsn):
    async def exercise(client, pool):
        data = body()
        response = await send(client, data)
        assert response.status_code == 202
        receipt = response.json()
        assert set(receipt) == {"instruction_id", "status", "received_at"}
        assert receipt["instruction_id"] == json.loads(data)["instruction_id"]
        assert receipt["status"] == "accepted"
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT body, source, lease_epoch FROM jammy.instructions WHERE instruction_id = %s",
                (receipt["instruction_id"],),
            )
            assert await cur.fetchone() == (data, "https", None)

    with_api(dsn, exercise)


def test_lease_epoch_is_persisted_and_stale_replays_are_rejected(dsn):
    async def exercise(client, pool):
        async with pool.connection() as conn:
            await conn.execute(
                """INSERT INTO jammy.site_lease_state VALUES ('a', true, 2, false, now())"""
            )
        data = body()
        problem(await send(client, data, lease_epoch="1"), 409, "stale_epoch")
        response = await send(client, data, lease_epoch="2")
        assert response.status_code == 202
        assert (await send(client, data, lease_epoch="3")).status_code == 200
        problem(await send(client, data, lease_epoch="1"), 409, "stale_epoch")
        async with pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT lease_epoch FROM jammy.instructions WHERE instruction_id = %s",
                (response.json()["instruction_id"],),
            )
            assert await cursor.fetchone() == (2,)

    with_api(dsn, exercise)


def test_instruction_request_creates_server_span(dsn, monkeypatch):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer", provider.get_tracer)

    async def exercise(client, pool):
        assert (await send(client, body())).status_code == 202

    with_api(dsn, exercise)
    (span,) = exporter.get_finished_spans()
    assert span.kind == trace.SpanKind.SERVER
    assert span.attributes["http.response.status_code"] == 202


def test_instruction_api_rejects_empty_instance_id(monkeypatch):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("INSTANCE_ID", "")
    monkeypatch.delenv("DATABASE_URL", raising=False)

    async def exercise():
        with pytest.raises(ConfigError, match="INSTANCE_ID"):
            await serve(asyncio.Event(), CommonConfig.from_env(), HMACAuth({"k": KEY}, 300))

    asyncio.run(exercise())


def test_instruction_api_uses_full_pool_in_its_single_worker(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://127.0.0.1:1/test")
    monkeypatch.setenv("DB_POOL_MIN", "0")
    monkeypatch.setenv("DB_POOL_MAX", "3")
    monkeypatch.setenv("WEB_CONCURRENCY", "2")
    pool_sizes = []
    create_pool = db.pool

    async def record_pool(cfg, **kwargs):
        pool = await create_pool(cfg, **kwargs)
        pool_sizes.append(pool.get_stats()["pool_max"])
        return pool

    monkeypatch.setattr(db, "pool", record_pool)

    async def exercise():
        stop = asyncio.Event()
        stop.set()
        # The API embeds one Uvicorn server and does not consume WEB_CONCURRENCY.
        cfg = CommonConfig("site-a", "api", 0, 0, "info", 25, "jetstream")
        await serve(stop, cfg, HMACAuth({"k": KEY}, 300))

    asyncio.run(exercise())
    assert pool_sizes == [3]


def test_same_id_same_body_200(dsn):
    async def exercise(client, pool):
        data = body()
        first = await send(client, data)
        async with pool.connection() as conn:
            await conn.execute(
                "UPDATE jammy.instructions SET status = 'dispatched' WHERE instruction_id = %s",
                (json.loads(data)["instruction_id"],),
            )
        repeated = await send(client, data)
        assert repeated.status_code == 200
        assert repeated.json() == {**first.json(), "status": "dispatched"}

    with_api(dsn, exercise)


def test_same_id_diff_body_409(dsn):
    async def exercise(client, _):
        data = body()
        assert (await send(client, data)).status_code == 202
        problem(await send(client, data + b"\n"), 409, "conflict")

    with_api(dsn, exercise)


@pytest.mark.parametrize(("options", "code"), [
    ({"signature": "0" * 64}, "bad_signature"),
    ({"offset": -301}, "stale_timestamp"),
    ({"key": "unknown"}, "unknown_key"),
], ids=["bad_signature", "stale_timestamp", "unknown_key"])
def test_hmac_rejection_401(dsn, options, code):
    async def exercise(client, _):
        problem(await send(client, body(), **options), 401, code)

    with_api(dsn, exercise)


@pytest.mark.parametrize("change", [
    {"valid_until": "2026-11-23T01:00:00Z"},
    {"target_reduction_kw": "0.000"},
    {"target_reduction_kw": 12.5},
    {"valid_until": "2026-11-23T02:00:00"},
])
def test_invalid_body_422(dsn, change):
    async def exercise(client, _):
        problem(await send(client, body(**change)), 422, "invalid_body")

    with_api(dsn, exercise)


def test_get_status_returns_commands(dsn):
    async def exercise(client, pool):
        instruction_id = str(uuid4())
        assert (await send(client, body(instruction_id=instruction_id))).status_code == 202
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO jammy.commands
                    (command_id, instruction_id, asset_id, reduction_kw, generation, epoch, status)
                VALUES (%s, %s, %s, 2.500, 1, 0, 'pending')
                """,
                [(uuid4(), instruction_id, asset_id) for asset_id in ("solar-a", "solar-b")],
            )
        path = f"{PATH}/{instruction_id}"
        problem(await send(client, method="GET", path=path, key="invalid"), 401, "unknown_key")
        response = await send(client, method="GET", path=path)
        assert response.status_code == 200
        document = response.json()
        assert document["target_reduction_kw"] == "12.500"
        assert {item["asset_id"] for item in document["commands"]} == {"solar-a", "solar-b"}
        assert all(item["reduction_kw"] == "2.500" for item in document["commands"])
        assert all(item["sent_at"] is None and item["acked_at"] is None for item in document["commands"])
        missing_path = f"{PATH}/{uuid4()}"
        problem(await send(client, method="GET", path=missing_path), 404, "not_found")

    with_api(dsn, exercise)


def test_accept_p99_under_1s_local(dsn):
    async def exercise(client, _):
        samples = []
        for _ in range(200):
            data = body()
            started = time.monotonic()
            assert (await send(client, data)).status_code == 202
            samples.append(time.monotonic() - started)
        assert sorted(samples)[197] < 1

    with_api(dsn, exercise)


def test_metrics_use_closed_results_and_route_templates(dsn, monkeypatch):
    from jammy_common import admin as admin_module
    from jammy_common import obs
    from jammy_common.admin import AdminServer
    from jammy_common.config import CommonConfig
    from prometheus_client import CollectorRegistry

    registry = CollectorRegistry()
    monkeypatch.setattr(obs, "REGISTRY", registry)
    monkeypatch.setattr(admin_module, "REGISTRY", registry)
    monkeypatch.setattr(obs.metrics, "set_meter_provider", lambda provider: None)

    monkeypatch.setenv("SITE_ID", "test")
    monkeypatch.setenv("INSTANCE_ID", "instruction-test")

    async def exercise(client, _):
        data = body()
        instruction_id = json.loads(data)["instruction_id"]
        path = f"{PATH}/{instruction_id}"
        assert (await send(client, data)).status_code == 202
        assert (await send(client, data)).status_code == 200
        assert (await send(client, data + b" ")).status_code == 409
        assert (await send(client, data, signature="0" * 64)).status_code == 401
        assert (await send(client, data, offset=-301)).status_code == 401
        assert (await send(client, data, key="unknown")).status_code == 401
        assert (await send(client, b"{}")).status_code == 422
        assert (await send(client, path=path, method="GET")).status_code == 200
        assert (await send(client, path=path, method="GET", key="unknown")).status_code == 401
        assert (await send(client, path=f"{PATH}/{uuid4()}", method="GET")).status_code == 404

        async def unavailable(*args):
            raise psycopg.OperationalError("test")

        monkeypatch.setattr("instruction_api.repo.detail", unavailable)
        assert (await send(client, path=path, method="GET")).status_code == 503

        admin = AdminServer(0, {})
        await admin.start()
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin.port}") as metrics:
                response = await metrics.get("/metrics")
            assert response.status_code == 200
        finally:
            await admin.close()

        buckets = [
            line for line in response.text.splitlines()
            if line.startswith("http_server_request_duration_seconds_bucket{")
        ]
        assert any(
            'http_route="/v1/instructions"' in line
            and 'http_request_method="POST"' in line
            and 'http_response_status_code="202"' in line
            for line in buckets
        )
        assert any(
            'http_route="/v1/instructions/{instruction_id}"' in line
            and 'http_request_method="GET"' in line
            and 'http_response_status_code="503"' in line
            for line in buckets
        )
        assert all(
            'service="instruction-api"' in line
            and 'site="test"' in line
            and 'instance="instruction-test"' in line
            and instruction_id not in line
            for line in buckets
        )
        results = {}
        for line in response.text.splitlines():
            if line.startswith("jammy_instruction_requests_total{"):
                result = re.search(r'result="([^"]+)"', line).group(1)
                results[result] = float(line.split()[-1])
        assert results == {
            "accepted": 1, "existing": 2, "conflict": 1, "unauthorized": 4,
            "invalid": 1, "not_found": 1, "error": 1,
        }

    with_api(dsn, exercise, meter=obs.setup("instruction-api", CommonConfig.from_env()))
