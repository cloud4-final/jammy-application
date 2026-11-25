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

        admin = AdminServer(0, {"dependency": check})
        await admin.start()
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin.port}") as client:
                assert (await client.get("/readyz")).status_code == 503
                connected = True
                assert (await client.get("/readyz")).status_code == 200
                admin.set_draining()
                assert (await client.get("/readyz")).status_code == 503
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
