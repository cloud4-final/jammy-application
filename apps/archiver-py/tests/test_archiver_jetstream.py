import asyncio
import io
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import boto3
import httpx
import nats
import pyarrow.parquet as pq
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from jammy_common.pb.envelope_pb2 import Event, EventBatch
from jammy_common.pb.profiles_pb2 import SynthLarge, SynthSmall
from testcontainers.core.container import DockerContainer

HOUR = datetime(2026, 11, 23, 10, tzinfo=UTC)


def until(check, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError("Condition did not become true before the deadline")


@pytest.fixture(scope="session", name="seaweed_endpoint")
def _seaweed_endpoint():
    seaweed = DockerContainer("chrislusf/seaweedfs:4.54").with_command(
        "mini -dir=/data"
    )
    with seaweed.with_exposed_ports(8333):
        yield f"http://{seaweed.get_container_host_ip()}:{seaweed.get_exposed_port(8333)}"


@pytest.fixture(name="s3_fixture")
def _s3_fixture(seaweed_endpoint):
    client = boto3.client(
        "s3",
        endpoint_url=seaweed_endpoint,
        region_name="ap-northeast-2",
        aws_access_key_id="fixture",
        aws_secret_access_key="fixture",
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=0.5,
            read_timeout=5,
            retries={"total_max_attempts": 1},
        ),
    )
    bucket = f"archive-{uuid4().hex}"

    def create_bucket():
        try:
            client.create_bucket(Bucket=bucket)
        except BotoCoreError, ClientError:
            return False
        return True

    until(create_bucket, timeout=60)
    return (
        client,
        bucket,
        {
            "S3_BUCKET": bucket,
            "S3_ENDPOINT": seaweed_endpoint,
            "S3_FORCE_PATH_STYLE": "true",
            "AWS_ACCESS_KEY_ID": "fixture",
            "AWS_SECRET_ACCESS_KEY": "fixture",
        },
    )


@contextmanager
def archiver(stage, **options):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = os.environ | {
        "SITE_ID": "fixture",
        "PIPELINE_MODE": "jetstream",
        "ADMIN_PORT": str(port),
        "STAGE_DIR": str(stage),
        "ARCHIVE_FLUSH_SECONDS": "1",
        **options,
    }
    with subprocess.Popen([sys.executable, "-m", "archiver.main"], env=env) as process:
        try:

            def ready():
                if process.poll() is not None:
                    raise AssertionError(f"Archiver exited with {process.returncode}")
                try:
                    return (
                        httpx.get(
                            f"http://127.0.0.1:{port}/readyz", timeout=0.5
                        ).status_code
                        == 200
                    )
                except httpx.TransportError:
                    return False

            until(ready, timeout=60)
            yield process, port
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=26)


async def publish(url, stream, count, prefix, stamp=HOUR, *, create=False):
    client = await nats.connect(url)
    try:
        js = client.jetstream()
        subject = f"jammy.raw.{stream}"
        if create:
            await js.add_stream(name=stream, subjects=[subject])
            return
        timestamp = int(stamp.timestamp() * 1000)
        template = Event(
            routing_key="asset",
            profile="synth-small",
            schema_version=1,
            event_time_unix_ms=timestamp,
            payload=SynthSmall(value=4).SerializeToString(),
        )
        for start in range(0, count, 200):
            item = EventBatch(batch_id=f"{prefix}-{start}")
            for index in range(start, min(start + 200, count)):
                item.events.add().CopyFrom(template)
                item.events[-1].event_id = f"{prefix}-{index}"
            await js.publish(
                subject,
                item.SerializeToString(),
                headers={
                    "Nats-Msg-Id": item.batch_id,
                    "Content-Type": "application/x-protobuf",
                    "Jammy-Ingest-Id": "fixture",
                    "Jammy-Received-At-Ms": str(timestamp),
                },
            )
    finally:
        await client.close()


def keys(client, bucket, prefix):
    return sorted(
        item["Key"]
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        )
        for item in page.get("Contents", ())
    )


def get(client, bucket, key):
    with client.get_object(Bucket=bucket, Key=key)["Body"] as body:
        return body.read()


def rows(client, bucket, prefix="raw/"):
    return [
        event_id
        for key in keys(client, bucket, prefix)
        if key.endswith(".parquet")
        for event_id in pq.read_table(io.BytesIO(get(client, bucket, key)))[
            "event_id"
        ].to_pylist()
    ]


@pytest.fixture(scope="session", name="nats_endpoint")
def _nats_endpoint():
    broker = (
        DockerContainer("nats:2.15.3")
        .with_command("--jetstream --store_dir /data")
        .with_exposed_ports(4222)
    )
    with broker:
        yield f"nats://{broker.get_container_host_ip()}:{broker.get_exposed_port(4222)}"


@pytest.fixture(name="pipeline")
def _pipeline(s3_fixture, nats_endpoint):
    client, bucket, options = s3_fixture
    stream = f"raw-{uuid4().hex[:8]}"
    asyncio.run(publish(nats_endpoint, stream, 0, "setup", create=True))
    return (
        client,
        bucket,
        nats_endpoint,
        stream,
        options
        | {
            "NATS_URL": nats_endpoint,
            "NATS_STREAMS": stream,
        },
    )


def test_s3_rows_equal_unique_published(tmp_path, pipeline):
    client, bucket, url, stream, options = pipeline
    with archiver(tmp_path, **options) as (_, port):
        asyncio.run(publish(url, stream, 20_000, "archive"))
        until(lambda: len(rows(client, bucket)) == 20_000, timeout=60)
        stored = rows(client, bucket)
        assert set(stored) == {f"archive-{i}" for i in range(20_000)}
        assert len(stored) == len(set(stored))
        metrics = httpx.get(f"http://127.0.0.1:{port}/metrics").text
        assert "jammy_archiver_files_total" in metrics
        assert "jammy_archiver_upload_duration_seconds_count" in metrics
        until(lambda: not list(tmp_path.glob("*.parquet")))


def test_kill9_mid_flush_no_loss_no_extra_keys(tmp_path, pipeline):
    client, bucket, url, stream, options = pipeline

    async def delivered():
        nc = await nats.connect(url)
        try:
            return (
                await nc.jetstream().consumer_info(stream, "archiver")
            ).num_ack_pending
        finally:
            await nc.close()

    with archiver(
        tmp_path,
        **(options | {"ARCHIVE_FLUSH_SECONDS": "10", "ACK_WAIT_SECONDS": "2"}),
    ) as (process, _):
        asyncio.run(publish(url, stream, 200, "restart"))
        until(lambda: asyncio.run(delivered()) == 1)
        assert not keys(client, bucket, "raw/")
        process.kill()
        process.wait(timeout=5)
    with archiver(tmp_path, **(options | {"ACK_WAIT_SECONDS": "2"})):
        until(lambda: len(rows(client, bucket)) == 200)
        stored = rows(client, bucket)
        assert set(stored) == {f"restart-{i}" for i in range(200)}
        assert len(keys(client, bucket, "raw/")) == 2
        assert keys(client, bucket, "raw/")[1].endswith("/fixture-1-1.parquet")


def test_late_event_goes_to_raw_late(tmp_path, pipeline):
    client, bucket, url, stream, options = pipeline
    with archiver(tmp_path, **options):
        asyncio.run(publish(url, stream, 1, "early"))
        until(lambda: rows(client, bucket))
        asyncio.run(publish(url, stream, 1, "watermark", HOUR + timedelta(hours=2)))
        prefix = "raw/profile=synth-small/dt=2026-11-23/hour=10/"
        seal = f"{prefix}_SEALED-fixture.json"
        until(lambda: seal in keys(client, bucket, prefix))
        original = get(client, bucket, seal)
        asyncio.run(publish(url, stream, 1, "late"))
        until(lambda: rows(client, bucket, "raw-late/") == ["late-0"])
        assert get(client, bucket, seal) == original
        assert rows(client, bucket, prefix) == ["early-0"]


def test_stage_file_removed_after_upload(tmp_path, pipeline):
    client, bucket, url, stream, options = pipeline
    with archiver(tmp_path, **options):
        asyncio.run(publish(url, stream, 1, "cleanup"))
        until(lambda: len(keys(client, bucket, "raw/")) == 2)
        until(lambda: not list(tmp_path.glob("*.parquet")))


@pytest.mark.parametrize("recovery", ["redelivery", "restart", "orphan", "legacy"])
def test_gap_is_written_before_ack(tmp_path, s3_fixture, recovery):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from archiver.main import Archiver
    from archiver.parquet_writer import ObjectStore
    from archiver.seal import hour_prefix
    from archiver.source_jetstream import Delivery, JetStreamArchive

    s3, bucket, _ = s3_fixture
    stored_at_ack = []
    store = ObjectStore(s3, bucket, tmp_path, 1, 0, Mock())
    config = SimpleNamespace(
        consumer="archiver",
        flush_bytes=1024**2,
        flush_seconds=100,
        ack_wait=30,
        lateness=timedelta(hours=1),
    )
    stream = "raw-fixture"
    stamp = int(HOUR.timestamp() * 1000)

    def message(sequence):
        event_id = f"event-{sequence}"
        batch = EventBatch(
            batch_id=f"batch-{sequence}",
            events=[
                Event(
                    event_id=event_id,
                    routing_key="asset",
                    profile="synth-small",
                    schema_version=1,
                    event_time_unix_ms=stamp,
                    payload=SynthSmall(value=4).SerializeToString(),
                )
            ],
        )

        async def ack():
            stored_at_ack.append((event_id, await asyncio.to_thread(rows, s3, bucket)))

        return SimpleNamespace(
            data=batch.SerializeToString(),
            headers={
                "Content-Type": "application/x-protobuf",
                "Nats-Msg-Id": batch.batch_id,
                "Jammy-Ingest-Id": "fixture",
                "Jammy-Received-At-Ms": str(stamp),
            },
            metadata=SimpleNamespace(sequence=SimpleNamespace(stream=sequence)),
            ack=ack,
            in_progress=AsyncMock(),
        )

    async def run():
        def start():
            service = Archiver(config, store, Mock())
            return service, JetStreamArchive(
                None,
                config,
                store,
                service.catalog,
                service.observe,
                stream=stream,
                persist_lock=asyncio.Lock(),
            )

        service, source = start()
        initial = [message(seq) for seq in (1, 2, 3, 5, 6, 7)]
        replay = [message(seq) for seq in (1, 4, 7)]
        original_key = f"{hour_prefix('synth-small', HOUR)}fixture-1-7.parquet"
        if recovery == "legacy":
            with store.stage() as stage:
                for msg in initial:
                    delivery = Delivery.decode(
                        msg, stream, msg.metadata.sequence.stream
                    )
                    stage.write(delivery.rows)
                store.commit(
                    stage,
                    original_key,
                    source="jetstream",
                    stream=stream,
                    first_seq=1,
                    last_seq=7,
                    partition_name=None,
                )
        else:
            source.subscription = SimpleNamespace(fetch=AsyncMock(return_value=initial))
            await source.poll()
            await source.flush()
        original = get(s3, bucket, original_key)
        if recovery != "legacy":
            from archiver.parquet_writer import manifest_key

            manifest = store.read_json(manifest_key(original_key))
            assert manifest["sequence_ranges"] == [[1, 3], [5, 7]]
            assert manifest["rows"] == 6
        if recovery == "orphan":
            manifest = next(key for key in store.list("raw/") if key.endswith(".json"))
            s3.delete_object(Bucket=bucket, Key=manifest)
        if recovery in {"restart", "orphan", "legacy"}:
            service, source = start()
            service.catalog.restore()

        source.subscription = SimpleNamespace(fetch=AsyncMock(return_value=replay))
        await source.poll()
        await source.flush()

        assert [event for event, _ in stored_at_ack[-3:]] == [
            "event-1",
            "event-4",
            "event-7",
        ]
        for event, stored in stored_at_ack:
            assert event in stored
        expected = [f"event-{seq}" for seq in range(1, 8)]
        if recovery == "legacy":
            expected += ["event-1", "event-7"]
        assert sorted(rows(s3, bucket)) == sorted(expected)
        assert get(s3, bucket, original_key) == original

    asyncio.run(run())


def test_slow_consumer_drop_is_redelivered_after_reconnect(tmp_path, pipeline):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from archiver.parquet_writer import ObjectStore
    from archiver.seal import Catalog
    from archiver.source_jetstream import JetStreamArchive

    s3, bucket, url, stream, _ = pipeline
    config = SimpleNamespace(
        consumer="archiver",
        flush_bytes=1024**2,
        flush_seconds=100,
        ack_wait=2,
    )
    store = ObjectStore(s3, bucket, tmp_path, 1, 0, Mock())

    async def run():
        for sequence in range(1, 8):
            await publish(url, stream, 1, f"event-{sequence}")

        client = await nats.connect(url)
        try:
            source = JetStreamArchive(
                client,
                config,
                store,
                Catalog(store, timedelta(hours=1)),
                Mock(),
                stream=stream,
                persist_lock=asyncio.Lock(),
            )
            await source.prepare()
            subscription = source.subscription

            async def fetch_with_drop(*_, **__):
                messages = await subscription.fetch(batch=7, timeout=2)
                assert [msg.metadata.sequence.stream for msg in messages] == list(
                    range(1, 8)
                )
                # nats-py drops individual deliveries if its pending queue is full.
                return [msg for msg in messages if msg.metadata.sequence.stream != 4]

            source.subscription = SimpleNamespace(fetch=fetch_with_drop)
            assert await source.poll()
            await source.flush()
            await client.flush()
        finally:
            await client.close()

        assert "event-4-0" not in rows(s3, bucket)
        client = await nats.connect(url)
        try:
            source.client = client
            source.subscription = None
            await source.prepare()

            async def poll_until_delivery():
                while not await source.poll():
                    pass

            await asyncio.wait_for(poll_until_delivery(), timeout=10)
            assert list(source.pending) == [4]
            await source.flush()
            await client.flush()
            consumer = await client.jetstream().consumer_info(stream, config.consumer)
            assert consumer.num_ack_pending == 0
            assert sorted(rows(s3, bucket)) == [f"event-{seq}-0" for seq in range(1, 8)]
        finally:
            await client.close()

    asyncio.run(run())


def test_flush_limit_counts_wire_batch_bytes(monkeypatch):
    from compression.zstd import compress
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from archiver.source_jetstream import JetStreamArchive

    async def check():
        event = Event(
            event_id="event",
            profile="synth-large",
            schema_version=1,
            payload=SynthLarge(blob=b"a" * 2048).SerializeToString(),
        )
        body = EventBatch(batch_id="batch", events=[event]).SerializeToString()
        wire = compress(body)
        headers = {
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "zstd",
            "Nats-Msg-Id": "batch",
            "Jammy-Ingest-Id": "fixture",
            "Jammy-Received-At-Ms": "1795392000000",
        }
        messages = [
            SimpleNamespace(
                data=wire,
                headers=headers,
                metadata=SimpleNamespace(sequence=SimpleNamespace(stream=sequence)),
                ack=AsyncMock(),
                in_progress=AsyncMock(),
            )
            for sequence in (1, 2)
        ]
        config = SimpleNamespace(
            flush_bytes=2 * len(wire), flush_seconds=100, ack_wait=30
        )
        archive = JetStreamArchive(
            None,
            config,
            None,
            None,
            lambda *_: None,
            stream="raw-fixture",
            persist_lock=asyncio.Lock(),
        )

        archive.subscription = SimpleNamespace(
            fetch=AsyncMock(side_effect=[[message] for message in messages])
        )
        monkeypatch.setattr(archive, "_persist", lambda: None)
        await archive.poll()
        messages[0].ack.assert_not_awaited()
        assert archive.size == len(wire)
        await archive.poll()
        for message in messages:
            message.ack.assert_awaited_once()
        assert archive.size == 0

    asyncio.run(check())


def test_archive_config_rejects_invalid_limits(monkeypatch):
    from archiver.main import ArchiveConfig
    from jammy_common.config import ConfigError

    for key, value in {
        "SITE_ID": "test",
        "PIPELINE_MODE": "jetstream",
        "NATS_URL": "nats://localhost",
        "NATS_STREAMS": "raw-fixture",
        "S3_BUCKET": "bucket",
    }.items():
        monkeypatch.setenv(key, value)
    with monkeypatch.context() as patch:
        for key in (
            "ARCHIVE_FLUSH_SECONDS",
            "ARCHIVE_FLUSH_BYTES",
            "ARCHIVE_ZSTD_LEVEL",
            "ACK_WAIT_SECONDS",
        ):
            patch.setenv(key, "0")
            with pytest.raises(ConfigError, match=key):
                ArchiveConfig.from_env()
            patch.delenv(key)
    assert ArchiveConfig.from_env().bandwidth == 0
    for value, expected in (
        ("2048", 2048),
        ("2KiB", 2 * 1024),
        ("2MiB", 2 * 1024**2),
        ("2GiB", 2 * 1024**3),
    ):
        monkeypatch.setenv("ARCHIVE_MAX_UPLOAD_BPS", value)
        assert ArchiveConfig.from_env().bandwidth == expected
    for value in ("-1", "+1", "1MB", "1.5MiB", "1 MiB", str(2**63), f"{2**33}GiB"):
        monkeypatch.setenv("ARCHIVE_MAX_UPLOAD_BPS", value)
        with pytest.raises(ConfigError, match="ARCHIVE_MAX_UPLOAD_BPS"):
            ArchiveConfig.from_env()

    monkeypatch.setenv("ARCHIVE_MAX_UPLOAD_BPS", "0")
    for value in ("1", "22"):
        monkeypatch.setenv("ARCHIVE_ZSTD_LEVEL", value)
        assert ArchiveConfig.from_env().level == int(value)
    monkeypatch.setenv("ARCHIVE_ZSTD_LEVEL", "23")
    with pytest.raises(ConfigError, match="ARCHIVE_ZSTD_LEVEL"):
        ArchiveConfig.from_env()

    monkeypatch.setenv("ARCHIVE_ZSTD_LEVEL", "1")
    assert ArchiveConfig.from_env().s3_prefix == ""
    monkeypatch.setenv("S3_PREFIX", "/site-a/")
    assert ArchiveConfig.from_env().s3_prefix == "site-a/"


def test_archive_config_validates_database_and_nats_url(monkeypatch):
    from archiver.main import ArchiveConfig, main
    from jammy_common.config import ConfigError

    monkeypatch.setenv("SITE_ID", "test")
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setenv("PIPELINE_MODE", "jetstream")
    monkeypatch.setenv("NATS_STREAMS", "raw-fixture")
    monkeypatch.setenv("NATS_URL", "http://localhost:4222")
    with pytest.raises(ConfigError, match="^invalid NATS_URL$"):
        ArchiveConfig.from_env()
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == "invalid NATS_URL"

    monkeypatch.setenv("NATS_URL", "ws://localhost:8080")
    assert ArchiveConfig.from_env().streams == ("raw-fixture",)

    monkeypatch.setenv("PIPELINE_MODE", "direct-db")
    monkeypatch.setenv("DATABASE_URL", "://bad")
    with pytest.raises(ConfigError, match="^invalid DATABASE_URL$"):
        ArchiveConfig.from_env()
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == "invalid DATABASE_URL"


def test_shutdown_retries_transient_flush_error(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from archiver.main import Archiver
    from archiver.source_jetstream import BatchIdMismatch

    async def run():
        stop = asyncio.Event()
        stop.set()
        service = Archiver(SimpleNamespace(lateness=timedelta(0)), Mock(), Mock())
        source = SimpleNamespace(flush=AsyncMock(side_effect=[OSError(), None]))
        pause = AsyncMock()
        monkeypatch.setattr("archiver.main.asyncio.sleep", pause)

        await service.consume(stop, source)

        assert source.flush.await_count == 2
        pause.assert_awaited_once_with(1)
        source.flush.reset_mock(side_effect=True)
        source.flush.side_effect = BatchIdMismatch("bad id")
        with pytest.raises(BatchIdMismatch):
            await service.consume(stop, source)
        source.flush.assert_awaited_once()

    asyncio.run(run())


def test_mismatched_message_id_fails_flush_without_acknowledging_batch():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from archiver.source_jetstream import BatchIdMismatch, JetStreamArchive

    async def run():
        def message(sequence, header_id):
            return SimpleNamespace(
                data=EventBatch(
                    batch_id=f"batch-{sequence}",
                    events=[Event(profile="synth-small")],
                ).SerializeToString(),
                headers={
                    "Content-Type": "application/x-protobuf",
                    "Nats-Msg-Id": header_id,
                    "Jammy-Ingest-Id": "fixture",
                    "Jammy-Received-At-Ms": "1795392000000",
                },
                metadata=SimpleNamespace(sequence=SimpleNamespace(stream=sequence)),
                ack=AsyncMock(),
                nak=AsyncMock(),
                term=AsyncMock(),
                in_progress=AsyncMock(),
            )

        valid = message(1, "batch-1")
        invalid = message(2, "wrong-id")
        store = Mock()
        source = JetStreamArchive(
            None,
            SimpleNamespace(flush_bytes=1024, flush_seconds=100, ack_wait=30),
            store,
            Mock(),
            Mock(),
            stream="raw-fixture",
            persist_lock=asyncio.Lock(),
        )
        source.subscription = SimpleNamespace(
            fetch=AsyncMock(return_value=[valid, invalid])
        )
        assert await source.poll()
        for _ in range(2):
            with pytest.raises(BatchIdMismatch, match="Nats-Msg-Id"):
                await source.flush()
            assert list(source.pending) == [1, 2]
            for delivery in (valid, invalid):
                delivery.ack.assert_not_awaited()
                delivery.nak.assert_not_awaited()
                delivery.term.assert_not_awaited()
        store.stage.assert_not_called()

    asyncio.run(run())


def test_configured_streams_run_concurrently(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from archiver.main import Archiver

    async def run():
        stop = asyncio.Event()
        config = SimpleNamespace(
            common=object(), streams=("raw-a", "raw-b"), lateness=timedelta(0)
        )
        nats_client = SimpleNamespace(close=AsyncMock())
        store = Mock()
        store.list.return_value = []
        service = Archiver(config, store, Mock())
        monkeypatch.setattr(
            "archiver.main.nats.connect_until_stop", AsyncMock(return_value=nats_client)
        )
        entered = set()

        async def consume(stop, source):
            entered.add(source.stream)
            if len(entered) == 2:
                stop.set()
            await stop.wait()

        monkeypatch.setattr(service, "consume", consume)
        await asyncio.wait_for(service.run(stop), timeout=2)
        assert entered == set(config.streams)
        nats_client.close.assert_awaited_once()

    asyncio.run(run())


def test_archive_ack_requires_successful_persistence(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from archiver.source_jetstream import JetStreamArchive

    async def run():
        source = JetStreamArchive(
            None,
            SimpleNamespace(ack_wait=30),
            None,
            None,
            Mock(),
            stream="raw-fixture",
            persist_lock=asyncio.Lock(),
        )
        message = SimpleNamespace(ack=AsyncMock(), in_progress=AsyncMock())
        source.pending[1] = SimpleNamespace(message=message, wire_bytes=10)
        source.size = 10
        persist = Mock(side_effect=[OSError("upload failed"), None])
        monkeypatch.setattr(source, "_persist", persist)
        with pytest.raises(OSError, match="upload failed"):
            await source.flush()
        message.ack.assert_not_awaited()
        await source.flush()
        message.ack.assert_awaited_once()
        assert source.size == 0
        assert not source.pending

    asyncio.run(run())


def test_archiver_terminates_undecodable_batch_without_staging():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from archiver.source_jetstream import JetStreamArchive

    async def run():
        message = SimpleNamespace(
            data=b"\xff",
            headers={"Content-Type": "application/x-protobuf"},
            metadata=SimpleNamespace(sequence=SimpleNamespace(stream=1)),
            term=AsyncMock(),
            ack=AsyncMock(),
        )
        store = Mock()
        worker = JetStreamArchive(
            None,
            SimpleNamespace(flush_bytes=1, flush_seconds=1, ack_wait=30),
            store,
            Mock(),
            Mock(),
            stream="raw-test",
            persist_lock=asyncio.Lock(),
        )
        worker.subscription = SimpleNamespace(fetch=AsyncMock(return_value=[message]))
        assert await worker.poll()
        await worker.flush()
        message.term.assert_awaited_once()
        message.ack.assert_not_awaited()
        store.files.add.assert_called_once_with(1, {"result": "poison"})
        store.stage.assert_not_called()

    asyncio.run(run())


def test_is_ready_times_out_when_store_stalls():
    import threading
    import time
    from types import SimpleNamespace
    from unittest.mock import Mock

    from archiver.main import Archiver

    release = threading.Event()

    def stalled_ready():
        release.wait(5)

    store = Mock()
    store.ready.side_effect = stalled_ready
    service = Archiver(
        SimpleNamespace(streams=(), lateness=timedelta(0)), store, Mock()
    )
    service.restored = True

    async def run():
        started = time.monotonic()
        assert not await service.is_ready()
        assert time.monotonic() - started < 1.5

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_flush_extracts_traceparent_and_creates_span(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from archiver.source_jetstream import JetStreamArchive
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from opentelemetry.trace import SpanKind

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(
        "archiver.source_jetstream.trace.get_tracer", provider.get_tracer
    )

    async def run():
        source = JetStreamArchive(
            None,
            SimpleNamespace(ack_wait=30),
            None,
            None,
            Mock(),
            stream="raw-fixture",
            persist_lock=asyncio.Lock(),
        )
        m1 = SimpleNamespace(
            headers={
                "Traceparent": "00-00000000000000000000000000000042-0000000000000023-01"
            },
            ack=AsyncMock(),
            in_progress=AsyncMock(),
        )
        m2 = SimpleNamespace(
            headers={
                "traceparent": "00-00000000000000000000000000000043-0000000000000024-01"
            },
            ack=AsyncMock(),
            in_progress=AsyncMock(),
        )
        source.pending[1] = SimpleNamespace(message=m1, wire_bytes=5)
        source.pending[2] = SimpleNamespace(message=m2, wire_bytes=5)
        source.size = 10
        monkeypatch.setattr(source, "_persist", lambda: None)
        await source.flush()

    asyncio.run(run())
    (span,) = exporter.get_finished_spans()
    assert span.name == "archive.flush"
    assert span.kind == SpanKind.CONSUMER
    assert span.context.trace_id == 0x42
    assert span.parent.span_id == 0x23
    assert {link.context.trace_id for link in span.links} == {0x42, 0x43}
    provider.shutdown()


def test_serve_initializes_unacked_gauge(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock, Mock

    from archiver import main

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    for key, value in {
        "SITE_ID": "site-a",
        "PIPELINE_MODE": "direct-db",
        "DATABASE_URL": "postgresql://postgres:test@127.0.0.1:1/jammy",
        "S3_BUCKET": "bucket",
        "STAGE_DIR": str(tmp_path),
        "ADMIN_PORT": str(port),
    }.items():
        monkeypatch.setenv(key, value)

    monkeypatch.setattr(main.s3, "client", lambda _cfg: Mock())
    monkeypatch.setattr(main.db, "pool", AsyncMock(return_value=AsyncMock()))

    async def fake_run(self, stop):
        from prometheus_client.parser import text_string_to_metric_families

        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as http:
            body = (await http.get("/metrics")).text
            families = {f.name: f for f in text_string_to_metric_families(body)}
            samples = families["jammy_archiver_unacked_messages"].samples
            assert [s.value for s in samples] == [0.0]
        stop.set()

    monkeypatch.setattr(main.Archiver, "run", fake_run)

    async def run():
        stop = asyncio.Event()
        await main.serve(main.ArchiveConfig.from_env(), stop)

    asyncio.run(run())


def test_export_uses_flush_seconds_and_creates_span(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from archiver import main
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr("archiver.main.trace.get_tracer", provider.get_tracer)

    wait_timeouts = []

    async def fake_wait_for(awaitable, timeout):
        wait_timeouts.append(timeout)
        awaitable.close()
        stop.set()
        raise TimeoutError

    stop = asyncio.Event()
    monkeypatch.setattr("archiver.main.asyncio.wait_for", fake_wait_for)
    monkeypatch.setattr(
        "archiver.main.DbArchive",
        lambda *_args, **_kwargs: SimpleNamespace(poll=AsyncMock(return_value=False)),
    )
    service = main.Archiver(
        SimpleNamespace(flush_seconds=17, lateness=timedelta(0)), Mock(), Mock()
    )

    asyncio.run(service.export(stop))
    assert wait_timeouts == [17]
    (span,) = exporter.get_finished_spans()
    assert span.name == "archive.export"
    provider.shutdown()
