import asyncio
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import nats
import psycopg
from jammy_common.pb.envelope_pb2 import Event, EventBatch
from jammy_common.pb.profiles_pb2 import SynthSmall
from test_processor_db import database, running_processor, summary_rows, until
from testcontainers.core.container import DockerContainer


async def create_stream(url, stream):
    client = await nats.connect(url)
    try:
        await client.jetstream().add_stream(
            name=stream, subjects=[f"jammy.raw.{stream}"]
        )
    finally:
        await client.close()


@contextmanager
def pipeline():
    broker = (
        DockerContainer("nats:2.15.3")
        .with_command("--jetstream --store_dir /data")
        .with_exposed_ports(4222)
    )
    with broker, database() as dsn:
        url = f"nats://{broker.get_container_host_ip()}:{broker.get_exposed_port(4222)}"
        stream = f"raw-{uuid4().hex[:8]}"
        asyncio.run(create_stream(url, stream))
        yield dsn, url, stream


def processor(dsn, url, stream, **extra):
    return running_processor(
        dsn, PIPELINE_MODE="jetstream", NATS_URL=url, NATS_STREAMS=stream, **extra
    )


async def publish(url, stream, count, *, batch_size=100):
    stamp = int(datetime.now(UTC).timestamp() * 1000)
    event = Event(
        routing_key="asset",
        profile="synth-small",
        schema_version=1,
        event_time_unix_ms=stamp,
        payload=SynthSmall(value=5).SerializeToString(),
    )
    client = await nats.connect(url)
    try:
        for start in range(0, count, batch_size):
            item = EventBatch(batch_id=uuid4().hex)
            for index in range(start, min(start + batch_size, count)):
                item.events.add().CopyFrom(event)
                item.events[-1].event_id = f"event-{index}"
            await client.jetstream().publish(
                f"jammy.raw.{stream}",
                item.SerializeToString(),
                headers={
                    "Nats-Msg-Id": item.batch_id,
                    "Content-Type": "application/x-protobuf",
                    "Jammy-Ingest-Id": "fixture",
                    "Jammy-Received-At-Ms": str(stamp),
                },
            )
    finally:
        await client.close()


async def consumer_info(url, stream):
    client = await nats.connect(url)
    try:
        return await client.jetstream().consumer_info(stream, "processor")
    finally:
        await client.close()


def test_summary_counts_match_published():
    with pipeline() as (dsn, url, stream), processor(dsn, url, stream) as (_, port):
        asyncio.run(publish(url, stream, 10_000))
        until(lambda: summary_rows(dsn) == 10_000, timeout=30)
        metrics = httpx.get(f"http://127.0.0.1:{port}/metrics").text
        assert "jammy_processor_batches_total" in metrics
        assert 'result="ok"' in metrics


def test_multiple_filtered_streams_and_idle_lag():
    with pipeline() as (dsn, url, stream):
        other = f"raw-{uuid4().hex[:8]}"
        asyncio.run(create_stream(url, other))
        with processor(dsn, url, f"{stream},{other}") as (_, port):
            for name in (stream, other):
                asyncio.run(publish(url, name, 1))
                assert (
                    asyncio.run(consumer_info(url, name)).config.filter_subject
                    == f"jammy.raw.{name}"
                )
            until(lambda: summary_rows(dsn) == 2)

            def idle():
                metrics = httpx.get(f"http://127.0.0.1:{port}/metrics").text
                return any(
                    line.startswith("jammy_processor_lag_seconds{")
                    and line.endswith(" 0.0")
                    for line in metrics.splitlines()
                )

            until(idle)


def test_ack_only_after_commit():
    with (
        pipeline() as (dsn, url, stream),
        processor(dsn, url, stream, ACK_WAIT_SECONDS="1"),
    ):
        with psycopg.connect(dsn) as blocker:
            blocker.execute("LOCK TABLE jammy.summary_1m IN SHARE MODE")
            asyncio.run(publish(url, stream, 1))
            until(lambda: asyncio.run(consumer_info(url, stream)).num_ack_pending == 1)
            with psycopg.connect(dsn, autocommit=True) as killer:

                def interrupt():
                    return killer.execute(
                        """SELECT COALESCE(bool_or(pg_terminate_backend(pid)), false)
                           FROM pg_stat_activity WHERE wait_event_type = 'Lock'
                             AND query LIKE 'INSERT INTO jammy.summary_1m%'"""
                    ).fetchone()[0]

                until(interrupt)
            assert summary_rows(dsn) == 0
            assert asyncio.run(consumer_info(url, stream)).ack_floor.stream_seq == 0
        until(lambda: summary_rows(dsn) == 1)


def test_redelivery_after_ackwait_no_double_count():
    with pipeline() as (dsn, url, stream):
        with psycopg.connect(dsn) as conn:
            conn.execute(
                """CREATE FUNCTION jammy.delay_summary() RETURNS trigger LANGUAGE plpgsql AS
                   $$ BEGIN PERFORM pg_sleep(2); RETURN NEW; END; $$;
                   CREATE TRIGGER delay_summary BEFORE INSERT ON jammy.summary_1m
                   FOR EACH ROW EXECUTE FUNCTION jammy.delay_summary()"""
            )
        with (
            processor(dsn, url, stream, ACK_WAIT_SECONDS="1"),
            processor(dsn, url, stream, ACK_WAIT_SECONDS="1"),
        ):
            asyncio.run(publish(url, stream, 1))
            until(lambda: asyncio.run(consumer_info(url, stream)).num_redelivered > 0)
            until(
                lambda: (
                    asyncio.run(consumer_info(url, stream)).ack_floor.stream_seq == 1
                )
            )
            assert summary_rows(dsn) == 1


def test_kill9_restart_exact_counts():
    with pipeline() as (dsn, url, stream):
        with psycopg.connect(dsn) as blocker:
            blocker.execute("LOCK TABLE jammy.summary_1m IN SHARE MODE")
            with processor(dsn, url, stream, ACK_WAIT_SECONDS="1") as (process, _):
                asyncio.run(publish(url, stream, 500))
                until(
                    lambda: asyncio.run(consumer_info(url, stream)).num_ack_pending > 0
                )
                process.kill()
                process.wait(timeout=5)
                assert summary_rows(dsn) == 0
        with processor(dsn, url, stream, ACK_WAIT_SECONDS="1"):
            until(lambda: summary_rows(dsn) == 500)


def test_bad_message_id_rejected_before_db():
    from types import SimpleNamespace

    import pytest
    from processor.source_jetstream import decode_message

    message = SimpleNamespace(
        data=EventBatch(batch_id="expected", events=[Event()]).SerializeToString(),
        headers={"Content-Type": "application/x-protobuf", "Nats-Msg-Id": "different"},
    )
    with pytest.raises(ValueError, match="Nats-Msg-Id"):
        decode_message(message)


def test_decode_limit_and_no_gate_validation():
    from compression.zstd import compress
    from types import SimpleNamespace

    import pytest
    from jammy_common.codec import HTTPError
    from processor.aggregate import Reading
    from processor.source_jetstream import decode_message

    message = SimpleNamespace(
        data=EventBatch(
            batch_id="valid-id", events=[Event(profile="solar", routing_key="asset")]
        ).SerializeToString(),
        headers={
            "Content-Type": "application/x-protobuf",
            "Nats-Msg-Id": "valid-id",
            "Jammy-Received-At-Ms": "0",
        },
    )
    decoded, _ = decode_message(message)
    assert decoded.events[0].schema_version == 0
    assert Reading.from_event(decoded.events[0]).value == 0
    message.headers["Content-Encoding"] = "zstd"
    message.data = compress(bytes(64 * 1024**2 + 1))
    with pytest.raises(HTTPError) as error:
        decode_message(message)
    assert error.value.status == 413


def test_claims_sorted_but_results_keep_delivery_order():
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from processor.source_jetstream import JetStreamSource
    from processor.state import LatestState

    claims = []

    class Connection:
        async def execute(self, _query, args):
            claims.append(args[1])
            row = None if args[1] == "batch-m" else (1,)
            return SimpleNamespace(fetchone=AsyncMock(return_value=row))

        def cursor(self):
            return AsyncMock()

    @asynccontextmanager
    async def connect(**_kwargs):
        yield Connection()

    def message(batch_id, value):
        return SimpleNamespace(
            data=EventBatch(
                batch_id=batch_id,
                events=[
                    Event(
                        profile="synth-small",
                        routing_key="asset",
                        payload=SynthSmall(value=value).SerializeToString(),
                    )
                ],
            ).SerializeToString(),
            headers={
                "Content-Type": "application/x-protobuf",
                "Nats-Msg-Id": batch_id,
                "Jammy-Received-At-Ms": "0",
            },
        )

    detector = SimpleNamespace(write=AsyncMock())
    source = JetStreamSource(
        SimpleNamespace(connection=connect),
        None,
        stream="raw",
        consumer="processor",
        ack_wait=30,
        max_pending=1000,
        batch_size=64,
        detector=detector,
    )
    readings, results, _ = asyncio.run(
        source.commit(
            [message("batch-z", 3), message("batch-m", 2), message("batch-a", 1)],
            lambda: None,
        )
    )
    assert claims == ["batch-a", "batch-m", "batch-z"]
    assert results == ["ok", "duplicate", "ok"]

    state = LatestState()
    state.offer(readings)
    assert state.pending["asset"].value == 1


def test_empty_fetch_clears_lag_with_unacknowledged_deliveries(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from processor.main import Processor

    async def run():
        stop = asyncio.Event()
        monkeypatch.setattr("processor.main.asyncio.sleep", AsyncMock())

        async def pending():
            stop.set()
            return 1

        meter = Mock()
        meter.create_gauge.side_effect = [Mock(), Mock()]
        consumer = Processor(SimpleNamespace(drop_ratio=0.5), None, meter)
        consumer.database_ready.set()
        consumer.client = SimpleNamespace(is_connected=True)
        consumer.observe_stream("raw", 1, 30)
        source = SimpleNamespace(
            stream="raw",
            prepare=AsyncMock(),
            fetch=AsyncMock(return_value=[]),
            pending=pending,
        )
        await consumer.consume(stop, source)
        consumer.lag.set.assert_called_with(0)
        consumer.pending.set.assert_called_with(1)

    asyncio.run(run())


def test_transaction_error_counts_messages_and_ack_follows_success(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from processor.main import Processor

    async def run():
        stop = asyncio.Event()
        sleep = AsyncMock()
        monkeypatch.setattr("processor.main.asyncio.sleep", sleep)
        messages = [SimpleNamespace(ack=AsyncMock()) for _ in range(2)]
        source = SimpleNamespace(
            stream="raw",
            prepare=AsyncMock(),
            fetch=AsyncMock(return_value=messages),
            commit=AsyncMock(
                side_effect=[
                    psycopg.OperationalError("transient"),
                    ([], ["ok", "ok"], 2),
                ]
            ),
            pending=AsyncMock(side_effect=lambda: stop.set() or 0),
        )
        consumer = Processor(SimpleNamespace(drop_ratio=0.5), None, Mock())
        consumer.database_ready.set()
        consumer.client = SimpleNamespace(is_connected=True)
        await consumer.consume(stop, source)
        assert source.commit.await_count == 2
        sleep.assert_awaited_once_with(1)
        assert [call.args for call in consumer.batches.add.call_args_list] == [
            (2, {"result": "error"}),
            (1, {"result": "ok"}),
            (1, {"result": "ok"}),
        ]
        for message in messages:
            message.ack.assert_awaited_once()

    asyncio.run(run())


def test_one_invalid_message_does_not_block_valid_delivery(monkeypatch):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from processor.main import Processor
    from processor.source_jetstream import JetStreamSource

    async def run():
        stop = asyncio.Event()
        upsert = AsyncMock()
        monkeypatch.setattr("processor.source_jetstream.upsert", upsert)
        claim = AsyncMock(
            return_value=SimpleNamespace(fetchone=AsyncMock(return_value=(1,)))
        )

        @asynccontextmanager
        async def connection(**_kwargs):
            yield SimpleNamespace(execute=claim)

        client = SimpleNamespace(is_connected=True)
        source = JetStreamSource(
            SimpleNamespace(connection=connection),
            client,
            stream="raw",
            consumer="processor",
            ack_wait=30,
            max_pending=1000,
            batch_size=64,
            detector=SimpleNamespace(write=AsyncMock()),
        )
        valid = Mock(
            data=EventBatch(
                batch_id="valid",
                events=[Event(profile="synth-small", routing_key="asset")],
            ).SerializeToString(),
            headers={
                "Content-Type": "application/x-protobuf",
                "Nats-Msg-Id": "valid",
                "Jammy-Received-At-Ms": "0",
            },
            ack=AsyncMock(),
        )
        invalid = Mock(
            data=b"invalid", headers=valid.headers, ack=AsyncMock(), term=AsyncMock()
        )

        async def pending():
            stop.set()
            return 0

        source.pending = pending
        source.prepare = AsyncMock()
        source.fetch = AsyncMock(return_value=[invalid, valid])
        consumer = Processor(SimpleNamespace(drop_ratio=0.5), None, Mock())
        consumer.database_ready.set()
        consumer.client = client

        await consumer.consume(stop, source)

        assert claim.await_count == 1
        assert claim.await_args.args[1] == ("processor", "valid")
        upsert.assert_awaited_once()
        valid.ack.assert_awaited_once()
        invalid.term.assert_awaited_once()
        invalid.ack.assert_not_awaited()
        assert [call.args for call in consumer.batches.add.call_args_list] == [
            (1, {"result": "poison"}),
            (1, {"result": "ok"}),
        ]

    asyncio.run(run())


def test_readiness_checks_database_and_stream_metadata(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from processor.main import Processor

    async def run():
        service = Processor(
            SimpleNamespace(drop_ratio=0.5, streams=("raw",)), object(), Mock()
        )
        broker = SimpleNamespace(stream_info=AsyncMock())
        service.client = SimpleNamespace(is_connected=True, jetstream=lambda: broker)
        service.database_ready.set()
        service.prepared.add("raw")
        database = AsyncMock(side_effect=[True, False])
        monkeypatch.setattr("processor.main.db.ping", database)
        assert await service.is_ready()
        assert not await service.is_ready()
        service.client.is_connected = False
        assert not await service.is_ready()
        assert [call.args for call in database.await_args_list] == [(service.pool,)] * 2
        assert [call.args for call in broker.stream_info.await_args_list] == [
            ("raw",)
        ] * 2

    asyncio.run(run())


def test_processor_config_rejects_invalid_limits(monkeypatch):
    import pytest
    from jammy_common.config import ConfigError
    from processor.main import ProcessorConfig

    monkeypatch.setenv("SITE_ID", "test")
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
    monkeypatch.setenv("PIPELINE_MODE", "direct-db")
    assert ProcessorConfig.from_env().max_deliver == 10
    for key, value in (
        ("CONSUMER_NAME", ""),
        ("ACK_WAIT_SECONDS", "0"),
        ("MAX_ACK_PENDING", "0"),
        ("PROCESSOR_MAX_DELIVER", "0"),
        ("FETCH_BATCH", "0"),
        ("STATE_FLUSH_SECONDS", "0"),
        ("ANOMALY_DROP_RATIO", "0"),
        ("ANOMALY_DROP_RATIO", "1.1"),
        ("GATE_MAX_PAST", "100000000000000h"),
        ("ACK_WAIT_SECONDS", "9223372037"),
        ("STATE_FLUSH_SECONDS", "9223372037"),
        ("MAX_ACK_PENDING", "2147483648"),
        ("PROCESSOR_MAX_DELIVER", "2147483648"),
        ("FETCH_BATCH", "2147483648"),
    ):
        with monkeypatch.context() as patch:
            patch.setenv(key, value)
            with pytest.raises(ConfigError, match=key):
                ProcessorConfig.from_env()


def test_processor_config_validates_database_and_nats_url(monkeypatch):
    import pytest
    from jammy_common.config import ConfigError
    from processor.main import ProcessorConfig, main

    monkeypatch.setenv("SITE_ID", "test")
    monkeypatch.setenv("PIPELINE_MODE", "direct-db")
    monkeypatch.setenv("DATABASE_URL", "://bad")
    with pytest.raises(ConfigError, match="^invalid DATABASE_URL$"):
        ProcessorConfig.from_env()
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == "invalid DATABASE_URL"

    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
    assert ProcessorConfig.from_env().streams == ()

    monkeypatch.setenv("PIPELINE_MODE", "jetstream")
    monkeypatch.setenv("NATS_STREAMS", "raw-a1")
    monkeypatch.setenv("NATS_URL", "http://localhost:4222")
    with pytest.raises(ConfigError, match="^invalid NATS_URL$"):
        ProcessorConfig.from_env()
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == "invalid NATS_URL"

    monkeypatch.setenv("NATS_URL", "tls://localhost:4222")
    assert ProcessorConfig.from_env().streams == ("raw-a1",)


def test_processor_terminates_undecodable_batch_without_database():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from processor.source_jetstream import JetStreamSource

    async def run():
        pool = Mock()
        record_poison = Mock()
        message = SimpleNamespace(
            data=b"\xff",
            headers={"Content-Type": "application/x-protobuf"},
            term=AsyncMock(),
            ack=AsyncMock(),
        )
        source = JetStreamSource(
            pool,
            None,
            consumer="processor",
            stream="raw-test",
            ack_wait=30,
            max_pending=64,
            batch_size=64,
            detector=None,
        )
        assert await source.commit([message], record_poison) == ([], ["poison"], None)
        message.term.assert_awaited_once()
        message.ack.assert_not_awaited()
        record_poison.assert_called_once()
        pool.connection.assert_not_called()

    asyncio.run(run())


def test_batch_span_extracts_traceparent_from_nats_headers(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from opentelemetry.trace import SpanKind, StatusCode
    from processor.main import Processor

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr("processor.main.trace.get_tracer", provider.get_tracer)

    async def run():
        for header_name in ("traceparent", "Traceparent"):
            stop = asyncio.Event()
            msg = SimpleNamespace(
                headers={
                    header_name: "00-00000000000000000000000000000042-0000000000000023-01",
                    "Nats-Msg-Id": "b-1",
                },
                ack=AsyncMock(),
            )
            source = SimpleNamespace(
                stream="raw-a1",
                prepare=AsyncMock(),
                fetch=AsyncMock(return_value=[msg]),
                commit=AsyncMock(return_value=([], ["ok"], 0.1)),
                pending=AsyncMock(side_effect=lambda s=stop: s.set() or 0),
            )
            consumer = Processor(SimpleNamespace(drop_ratio=0.5), None, Mock())
            consumer.database_ready.set()
            consumer.client = SimpleNamespace(is_connected=True)
            await consumer.consume(stop, source)

        stop = asyncio.Event()
        multi = [
            SimpleNamespace(
                headers={
                    "traceparent": "00-00000000000000000000000000000011-0000000000000012-01"
                },
                ack=AsyncMock(),
            ),
            SimpleNamespace(
                headers={
                    "Traceparent": "00-00000000000000000000000000000021-0000000000000022-01"
                },
                ack=AsyncMock(),
            ),
        ]
        source = SimpleNamespace(
            stream="raw-a1",
            prepare=AsyncMock(),
            fetch=AsyncMock(return_value=multi),
            commit=AsyncMock(side_effect=TimeoutError("db down")),
            pending=AsyncMock(side_effect=lambda: stop.set() or 0),
        )
        consumer = Processor(SimpleNamespace(drop_ratio=0.5), None, Mock())
        consumer.database_ready.set()
        consumer.client = SimpleNamespace(is_connected=True)
        monkeypatch.setattr(
            "processor.main.asyncio.sleep", AsyncMock(side_effect=lambda _: stop.set())
        )
        await consumer.consume(stop, source)

    asyncio.run(run())
    spans = exporter.get_finished_spans()
    assert len(spans) == 3
    for span in spans[:2]:
        assert span.kind == SpanKind.CONSUMER
        assert span.parent.span_id == 0x23
        assert span.context.trace_id == 0x42
        assert span.attributes["batch_id"] == "b-1"
    assert spans[2].status.status_code == StatusCode.ERROR
    assert [link.context.trace_id for link in spans[2].links] == [0x11, 0x21]
    provider.shutdown()


def test_batch_id_mismatch_termed_as_poison_and_db_error_naks_with_backoff():
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from processor.source_jetstream import JetStreamSource

    async def run():
        pool = Mock()
        record_poison = Mock()
        mismatch = SimpleNamespace(
            data=EventBatch(
                batch_id="expected", events=[Event(profile="synth-small", routing_key="a")]
            ).SerializeToString(),
            headers={
                "Content-Type": "application/x-protobuf",
                "Nats-Msg-Id": "different",
                "Jammy-Received-At-Ms": "0",
            },
            term=AsyncMock(),
            ack=AsyncMock(),
            nak=AsyncMock(),
        )
        source = JetStreamSource(
            pool,
            None,
            consumer="processor",
            stream="raw-test",
            ack_wait=30,
            max_pending=64,
            max_deliver=10,
            batch_size=64,
            detector=SimpleNamespace(write=AsyncMock()),
        )
        assert await source.commit([mismatch], record_poison) == ([], ["poison"], None)
        mismatch.term.assert_awaited_once()
        mismatch.ack.assert_not_awaited()
        mismatch.nak.assert_not_awaited()
        record_poison.assert_called_once()
        pool.connection.assert_not_called()

        @asynccontextmanager
        async def failing_connection(**_kwargs):
            raise psycopg.OperationalError("connection lost")
            yield  # pragma: no cover

        valid = SimpleNamespace(
            data=EventBatch(
                batch_id="valid", events=[Event(profile="synth-small", routing_key="a")]
            ).SerializeToString(),
            headers={
                "Content-Type": "application/x-protobuf",
                "Nats-Msg-Id": "valid",
                "Jammy-Received-At-Ms": "0",
            },
            term=AsyncMock(),
            ack=AsyncMock(),
            nak=AsyncMock(),
        )
        failing_source = JetStreamSource(
            SimpleNamespace(connection=failing_connection),
            None,
            consumer="processor",
            stream="raw-test",
            ack_wait=30,
            max_pending=64,
            max_deliver=7,
            batch_size=64,
            detector=SimpleNamespace(write=AsyncMock()),
        )
        try:
            await failing_source.commit([valid], record_poison)
        except psycopg.OperationalError:
            pass
        else:
            raise AssertionError("expected OperationalError")
        valid.nak.assert_awaited_once_with(delay=1)
        valid.term.assert_not_awaited()
        valid.ack.assert_not_awaited()

        js = SimpleNamespace(
            add_consumer=AsyncMock(), pull_subscribe_bind=AsyncMock(return_value="sub")
        )
        failing_source.client = SimpleNamespace(jetstream=lambda: js)
        await failing_source.prepare()
        assert js.add_consumer.await_args.kwargs["config"].max_deliver == 7

    asyncio.run(run())
