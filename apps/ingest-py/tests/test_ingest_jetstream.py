import asyncio
import os
from compression.zstd import compress, decompress
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import nats
import pytest
from google.protobuf.json_format import MessageToJson
from ingest.main import IngestConfig, IntakeState, _maintain, create_app
from ingest.sink_jetstream import JetStreamSink
from jammy_common.pb.envelope_pb2 import Event, EventBatch
from jammy_common.pb.profiles_pb2 import SynthLarge, SynthSmall
from nats.js.api import StorageType
from testcontainers.core.container import DockerContainer

PROTOBUF = "application/x-protobuf"
TEST_RAW_MAX_BYTES = "4MiB"


@pytest.fixture
def broker():
    # JetStream reserves file storage per stream until the broker is torn down.
    container = (
        DockerContainer("nats:2.15.3")
        .with_command("--jetstream --store_dir /data")
        .with_exposed_ports(4222)
    )
    with container:
        yield container


def broker_url(container):
    return (
        f"nats://{container.get_container_host_ip()}:{container.get_exposed_port(4222)}"
    )


def batch(*, rejected=False, all_rejected=False, blob=b""):
    stamp = int(datetime.now(UTC).timestamp() * 1000)
    profile = "synth-large" if blob else "synth-small"
    payload = SynthLarge(value=3, blob=blob) if blob else SynthSmall(value=3)
    good = Event(
        event_id="event",
        routing_key="asset",
        profile=profile,
        schema_version=1,
        event_time_unix_ms=stamp,
        payload=payload.SerializeToString(),
    )
    bad = Event(event_id="invalid/id", routing_key="asset")
    events = [bad] if all_rejected else ([good, bad] if rejected else [good])
    return EventBatch(batch_id=uuid4().hex, producer_id="fixture", events=events)


async def post(client, item, *, zipped=False, json_body=False):
    wire = MessageToJson(item).encode() if json_body else item.SerializeToString()
    headers = {"Content-Type": "application/json" if json_body else PROTOBUF}
    if zipped:
        wire = compress(wire)
        headers["Content-Encoding"] = "zstd"
    return await client.post("/v1/batches", content=wire, headers=headers), wire


def js_config(broker, monkeypatch, *, max_bytes=TEST_RAW_MAX_BYTES, max_age="45m"):
    monkeypatch.setenv("SITE_ID", "fixture")
    monkeypatch.setenv("INSTANCE_ID", "ingest-fixture")
    monkeypatch.setenv("NATS_URL", broker_url(broker))
    monkeypatch.setenv("PIPELINE_MODE", "jetstream")
    monkeypatch.setenv("INGEST_STREAM", f"raw-{uuid4().hex[:8]}")
    monkeypatch.setenv("JS_MAX_BYTES", max_bytes)
    monkeypatch.setenv("JS_MAX_AGE", max_age)
    return IngestConfig.from_env()


async def js_case(broker, monkeypatch, check, *, max_bytes=TEST_RAW_MAX_BYTES):
    config = js_config(broker, monkeypatch, max_bytes=max_bytes)
    nc = await nats.connect(broker_url(broker), connect_timeout=1)
    try:
        sink = JetStreamSink(nc, config.stream, config.max_age, config.js_max_bytes)
        await sink.prepare()
        state = IntakeState(config)
        state.sink = sink
        state.ready = True
        transport = httpx.ASGITransport(app=create_app(config, state))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://ingest"
        ) as client:
            await check(client, nc.jetstream(), config.stream)
    finally:
        await nc.close()


def test_valid_batch_202_and_in_stream(broker, monkeypatch):
    async def check(client, js, stream):
        item = batch()
        response, _ = await post(client, item)
        assert response.status_code == 202
        assert response.json() == {
            "batch_id": item.batch_id,
            "accepted": 1,
            "rejected": 0,
            "reasons": {},
            "duplicate": False,
        }
        assert (await js.stream_info(stream)).state.messages == 1
        message = await js.get_msg(stream, seq=1)
        assert message.subject == f"jammy.raw.{stream}"
        assert message.headers["Nats-Msg-Id"] == item.batch_id
        assert message.headers["Jammy-Ingest-Id"] == "ingest-fixture"
        assert (
            int(message.headers["Jammy-Received-At-Ms"])
            >= item.events[0].event_time_unix_ms
        )

    asyncio.run(js_case(broker, monkeypatch, check))


def test_fully_valid_batch_passes_bytes_through(broker, monkeypatch):
    async def check(client, js, stream):
        response, wire = await post(client, batch(), zipped=True)
        assert response.status_code == 202
        message = await js.get_msg(stream, seq=1)
        assert message.data == wire
        assert message.headers["Content-Encoding"] == "zstd"

    asyncio.run(js_case(broker, monkeypatch, check))


@pytest.mark.parametrize("json_body", [False, True])
def test_partial_batch_reencodes_valid_only(broker, monkeypatch, json_body):
    async def check(client, js, stream):
        item = batch(rejected=True)
        response, _ = await post(client, item, json_body=json_body)
        assert response.status_code == 202
        message = await js.get_msg(stream, seq=1)
        assert message.headers["Content-Type"] == PROTOBUF
        assert message.headers["Content-Encoding"] == "zstd"
        stored = EventBatch.FromString(decompress(message.data))
        assert stored.batch_id == item.batch_id
        assert stored.producer_id == item.producer_id
        assert list(stored.events) == [item.events[0]]

    asyncio.run(js_case(broker, monkeypatch, check))


def test_rejected_go_to_quarantine_with_reason(broker, monkeypatch):
    async def check(client, js, stream):
        item = batch(rejected=True)
        response, _ = await post(client, item)
        assert response.json()["reasons"] == {"bad_event_id": 1}
        message = await js.get_msg(f"quarantine-{stream}", seq=1)
        assert message.subject == f"jammy.quarantine.{stream}"
        assert message.headers["Nats-Msg-Id"] == f"{item.batch_id}:q:1"
        assert message.headers["Jammy-Batch-Id"] == item.batch_id
        assert message.headers["Jammy-Reason"] == "bad_event_id"
        assert Event.FromString(message.data) == item.events[1]

    asyncio.run(js_case(broker, monkeypatch, check))


@pytest.mark.parametrize("all_rejected", [False, True])
def test_duplicate_batch_id_single_message(broker, monkeypatch, all_rejected):
    async def check(client, js, stream):
        item = batch(rejected=True, all_rejected=all_rejected)
        first, _ = await post(client, item)
        duplicate, _ = await post(client, item)
        assert first.status_code == duplicate.status_code == 202
        assert first.json()["duplicate"] is False
        assert duplicate.json() == {
            "batch_id": item.batch_id,
            "accepted": 0,
            "rejected": 0,
            "reasons": {},
            "duplicate": True,
        }
        assert (await js.stream_info(stream)).state.messages == (
            0 if all_rejected else 1
        )
        assert (await js.stream_info(f"quarantine-{stream}")).state.messages == 1

    asyncio.run(js_case(broker, monkeypatch, check))


def test_stream_full_returns_503(broker, monkeypatch):
    async def check(client, js, stream):
        for index in range(20):
            response, _ = await post(client, batch(blob=os.urandom(65536)))
            if response.status_code == 503:
                break
            assert response.status_code == 202
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "2"
        assert response.json()["type"] == "https://jammy.dev/problems/unavailable"
        assert 0 < (await js.stream_info(stream)).state.messages < index + 1

    asyncio.run(js_case(broker, monkeypatch, check, max_bytes="1MiB"))


def test_prepare_updates_existing_stream_to_env_limits(broker, monkeypatch):
    config = js_config(broker, monkeypatch, max_bytes="1MiB", max_age="5m")

    async def run():
        nc = await nats.connect(broker_url(broker))
        try:
            js = nc.jetstream()
            await js.add_stream(
                name=config.stream,
                subjects=[f"jammy.raw.{config.stream}"],
                max_age=600,
                max_bytes=1024,
                storage=StorageType.FILE,
            )
            sink = JetStreamSink(nc, config.stream, config.max_age, config.js_max_bytes)
            await sink.prepare()
            actual = (await js.stream_info(config.stream)).config
            assert actual.max_age == 300
            assert actual.max_bytes == 1024**2
        finally:
            await nc.close()

    asyncio.run(run())


def test_worker_recreates_deleted_stream_and_becomes_ready(broker, monkeypatch):
    config = js_config(broker, monkeypatch)

    async def run():
        ready = asyncio.Event()
        state = IntakeState(config, ready_event=ready)
        nc = await nats.connect(broker_url(broker))
        worker = asyncio.create_task(_maintain(config, state, worker_index=0))
        try:
            async with asyncio.timeout(5):
                await ready.wait()
                js = nc.jetstream()
                await js.delete_stream(config.stream)
                while ready.is_set():
                    await asyncio.sleep(0.02)
                await ready.wait()
                restored = await js.stream_info(config.stream)
                assert restored.config.name == config.stream
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            if state.sink is not None:
                await state.sink.close()
            await nc.close()

    asyncio.run(run())


def test_inflight_limit_429(monkeypatch):
    monkeypatch.setenv("SITE_ID", "fixture")
    monkeypatch.setenv("PIPELINE_MODE", "direct-db")
    monkeypatch.setenv("DATABASE_URL", "postgresql://fixture@localhost/fixture")
    monkeypatch.setenv("INGEST_MAX_INFLIGHT", "1")
    config = IngestConfig.from_env()

    async def run():
        entered = asyncio.Event()
        release = asyncio.Event()

        class DelayedSink:
            async def store(self, intake):
                entered.set()
                await release.wait()
                return False

        state = IntakeState(config)
        state.sink = DelayedSink()
        state.ready = True
        transport = httpx.ASGITransport(app=create_app(config, state))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://ingest"
        ) as client:
            first = asyncio.create_task(post(client, batch()))
            await asyncio.wait_for(entered.wait(), 2)
            try:
                response, _ = await post(client, batch())
                assert response.status_code == 429
                assert response.headers["Retry-After"] == "1"
            finally:
                release.set()
                sent, _ = await first
            assert sent.status_code == 202

    asyncio.run(run())
