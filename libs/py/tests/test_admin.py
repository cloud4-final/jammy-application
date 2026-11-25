import asyncio

import httpx
from prometheus_client.parser import text_string_to_metric_families

from jammy_common.admin import AdminServer
from jammy_common.config import CommonConfig
from jammy_common.obs import setup


def test_readyz_tracks_dependency():
    async def exercise():
        connected = False

        async def check():
            return connected

        async def healthy():
            return True

        admin = AdminServer(0, {"dependency": check, "healthy": healthy})
        await admin.start()
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin.port}") as client:
                response = await client.get("/readyz")
                assert response.status_code == 503
                assert response.headers["content-type"] == "application/json"
                assert response.json() == {
                    "status": "fail", "checks": {"dependency": "fail", "healthy": "ok"},
                }
                connected = True
                response = await client.get("/readyz")
                assert response.status_code == 200
                assert response.json() == {
                    "status": "ok", "checks": {"dependency": "ok", "healthy": "ok"},
                }
                admin.set_draining()
                response = await client.get("/readyz")
                assert response.status_code == 503
                assert response.json() == {"status": "fail", "checks": {}}
        finally:
            await admin.close()

    asyncio.run(exercise())


def test_admin_server_enforces_header_read_timeout(monkeypatch):
    import socket
    import time

    from jammy_common import admin as admin_module
    from jammy_common.http import HeaderTimeoutProtocol

    class _ShortHeaderTimeoutProtocol(HeaderTimeoutProtocol):
        header_timeout = 0.1

    async def exercise():
        default_admin = AdminServer(0, {})
        assert default_admin._server.config.http is HeaderTimeoutProtocol

        monkeypatch.setattr(admin_module, "HeaderTimeoutProtocol", _ShortHeaderTimeoutProtocol)
        admin = AdminServer(0, {})
        await admin.start()
        try:
            def probe_stall():
                with socket.create_connection(("127.0.0.1", admin.port), timeout=2) as client:
                    started = time.monotonic()
                    client.sendall(b"GET /livez HTTP/1.1\r\nHost: localhost\r\n")
                    assert client.recv(1) == b""
                    assert time.monotonic() - started < 1.0

            await asyncio.to_thread(probe_stall)
        finally:
            await admin.close()

    asyncio.run(exercise())


def test_readyz_reports_timeout_and_exception_independently():
    async def exercise():
        async def slow():
            await asyncio.sleep(3)
            return True

        async def broken():
            raise ConnectionError

        async def healthy():
            return True

        admin = AdminServer(0, {"slow": slow, "broken": broken, "healthy": healthy})
        await admin.start()
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin.port}") as client:
                async with asyncio.timeout(2):
                    response = await client.get("/readyz")
                assert response.status_code == 503
                assert response.json() == {
                    "status": "fail", "checks": {"slow": "fail", "broken": "fail", "healthy": "ok"},
                }
        finally:
            await admin.close()

    asyncio.run(exercise())


def test_livez_always_200():
    async def exercise():
        async def check():
            raise ConnectionError

        admin = AdminServer(0, {"dependency": check})
        await admin.start()
        try:
            admin.set_draining()
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin.port}") as client:
                assert (await client.get("/livez")).status_code == 200
        finally:
            await admin.close()

    asyncio.run(exercise())


def test_metrics_exposes_duration_histogram_buckets(monkeypatch):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("INSTANCE_ID", "instance-a")
    meter = setup("ingest-py", CommonConfig.from_env())
    histogram = meter.create_histogram("http.server.request.duration", unit="s")
    histogram.record(0.004, {"http.route": "/v1/instructions/{id}", "event_id": "high-cardinality"})
    duration = meter.create_histogram("jammy_x_duration", unit="s")
    duration.record(0.004, {"result": "stored", "http.route": "/must-not-appear"})

    async def exercise():
        admin = AdminServer(0, {})
        await admin.start()
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin.port}") as client:
                response = await client.get("/metrics")
                assert response.status_code == 200
                assert response.headers["content-type"].startswith("text/plain")
                openmetrics = await client.get(
                    "/metrics", headers={"Accept": "application/openmetrics-text; version=1.0.0"}
                )
                assert openmetrics.status_code == 200
                assert openmetrics.headers["content-type"].startswith("application/openmetrics-text")
                assert openmetrics.text.endswith("# EOF\n")
                assert "http_server_request_duration_seconds_bucket" in openmetrics.text
                malformed_accept = await client.get(
                    "/metrics", headers={"Accept": "application/openmetrics-text; version=bad"}
                )
                assert malformed_accept.status_code == 200
                assert malformed_accept.headers["content-type"].startswith("text/plain")
                families = {
                    family.name: family for family in text_string_to_metric_families(response.text)
                }
                expected = [
                    "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1",
                    "0.25", "0.5", "1.0", "2.5", "5.0", "10.0", "+Inf",
                ]
                for name in ("http_server_request_duration_seconds", "jammy_x_duration_seconds"):
                    buckets = [
                        sample.labels for sample in families[name].samples
                        if sample.name == f"{name}_bucket"
                    ]
                    assert [labels["le"] for labels in buckets] == expected
                    assert all(labels["site"] == "site-a" for labels in buckets)
                    assert all(labels["instance"] == "instance-a" for labels in buckets)
                    assert all(labels["service"] == "ingest-py" for labels in buckets)
                    assert all("event_id" not in labels for labels in buckets)
                    if name == "jammy_x_duration_seconds":
                        assert all(labels["result"] == "stored" for labels in buckets)
                        assert all("http_route" not in labels for labels in buckets)
                    else:
                        assert all(labels["http_route"] == "/v1/instructions/{id}" for labels in buckets)
        finally:
            await admin.close()

    asyncio.run(exercise())


def test_nats_down_at_startup_retries_then_ready(monkeypatch):
    import socket
    from contextlib import suppress

    from jammy_common.nats import connect

    monkeypatch.setenv("SITE_ID", "site-a")

    async def exercise():
        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            port = unavailable.getsockname()[1]
        monkeypatch.setenv("NATS_URL", f"nats://127.0.0.1:{port}")

        first = asyncio.create_task(connect(CommonConfig.from_env()))
        await asyncio.sleep(0.1)
        first.cancel()
        with suppress(asyncio.CancelledError):
            await first

        connection = asyncio.create_task(connect(CommonConfig.from_env()))

        async def nats_ready():
            return connection.done() and connection.result().is_connected

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

        admin = AdminServer(0, {"nats": nats_ready})
        await admin.start()
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin.port}") as http:
                assert (await http.get("/readyz")).status_code == 503
                async with await asyncio.start_server(minimal_nats, "127.0.0.1", port):
                    client = await asyncio.wait_for(connection, timeout=8)
                    try:
                        assert (await http.get("/readyz")).status_code == 200
                    finally:
                        await client.close()
        finally:
            if not connection.done():
                connection.cancel()
                with suppress(asyncio.CancelledError):
                    await connection
            await admin.close()

    asyncio.run(exercise())
