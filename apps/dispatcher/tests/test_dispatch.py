import asyncio
import json
import os
import socket
import sys
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import psycopg
import pytest
from botocore.exceptions import EndpointConnectionError
from dispatcher import main
from dispatcher.commands import Dispatcher
from dispatcher.leader import Leader
from dispatcher.lease import Lease
from dispatcher.main import serve
from dispatcher.sqs_drain import SQSDrain
from jammy_common.admin import AdminServer
from jammy_common.config import CommonConfig, ConfigError
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="module")
def postgres():
    container = DockerContainer("postgres:17.12-trixie").with_env(
        "POSTGRES_PASSWORD", "test"
    )
    with container.with_exposed_ports(5432):
        host, port = container.get_container_host_ip(), container.get_exposed_port(5432)
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
            root = Path(__file__).resolve().parents[3]
            for migration in sorted((root / "contracts/db/migrations").glob("*.up.sql")):
                connection.execute(migration.read_text())
        yield uri


@pytest.fixture
def dsn(postgres):
    with psycopg.connect(postgres) as conn:
        conn.execute(
            "TRUNCATE jammy.compliance, jammy.commands, jammy.instructions, "
            "jammy.assets, jammy.latest_state, jammy.site_lease_state"
        )
        conn.execute(
            "INSERT INTO jammy.assets VALUES ('solar-1','tenant-a','solar',100.000)"
        )
        conn.execute(
            "INSERT INTO jammy.latest_state VALUES ('solar-1','solar',40,now(),now())"
        )
    return postgres


async def insert_instruction(pool):
    instruction_id = str(uuid4())
    now = datetime.now(UTC)
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO jammy.instructions
                (instruction_id,body,body_sha256,target_reduction_kw,valid_from,valid_until,status,source)
            VALUES (%s,%s,%s,25.000,%s,%s,'accepted','https')
            """,
            (
                instruction_id,
                b"{}",
                "0" * 64,
                now - timedelta(minutes=5),
                now + timedelta(minutes=10),
            ),
        )
    return instruction_id


@asynccontextmanager
async def plant_gateway(handler):
    async def serve(reader, writer):
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            assert headers.startswith(b"POST /v1/commands HTTP/1.1\r\n")
            size = next(
                int(line.split(b":", 1)[1])
                for line in headers.lower().split(b"\r\n")
                if line.startswith(b"content-length:")
            )
            request = json.loads(await reader.readexactly(size))
            status = await handler(request)
            writer.write(
                f"HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    async with server:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"


async def statuses(pool, instruction_id):
    async with pool.connection() as conn:
        rows = await conn.execute(
            "SELECT command_id, status FROM jammy.commands WHERE instruction_id=%s",
            (instruction_id,),
        )
        commands = await rows.fetchall()
        row = await conn.execute(
            "SELECT status FROM jammy.instructions WHERE instruction_id=%s",
            (instruction_id,),
        )
        return commands, (await row.fetchone())[0]


@asynccontextmanager
async def dispatcher_process(dsn, gateway, port, **settings):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "dispatcher.main",
        env=os.environ | {
            "DATABASE_URL": dsn,
            "SITE_ID": "site-a",
            "INSTANCE_ID": "dispatcher-a1",
            "LEASE_MODE": "disabled",
            "PLANT_GATEWAY_URL": gateway,
            "ADMIN_PORT": str(port),
            "DB_POOL_MIN": "1",
            "DB_POOL_MAX": "2",
            "SQS_QUEUE_URL": "",
        } | settings,
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()


def test_readyz_does_not_wait_for_leader_transaction(dsn):
    async def exercise(port):
        async with (
            AsyncConnectionPool(dsn, open=False) as pool,
            httpx.AsyncClient() as client,
        ):
            await insert_instruction(pool)
            async with pool.connection() as blocker:
                await blocker.execute("LOCK TABLE jammy.assets IN ACCESS EXCLUSIVE MODE")
                async with dispatcher_process(dsn, "http://127.0.0.1:1", port):
                    async with await psycopg.AsyncConnection.connect(
                        dsn, autocommit=True
                    ) as observer:
                        async with asyncio.timeout(10):
                            while True:
                                blocked = await observer.execute(
                                    """
                                    SELECT EXISTS (
                                        SELECT 1 FROM pg_stat_activity
                                        WHERE wait_event_type = 'Lock'
                                          AND query LIKE '%LEFT JOIN jammy.latest_state%'
                                    )
                                    """
                                )
                                if (await blocked.fetchone())[0]:
                                    break
                                await asyncio.sleep(0.05)
                    async with asyncio.timeout(2):
                        response = await client.get(f"http://127.0.0.1:{port}/readyz")
                    assert response.status_code == 200

    asyncio.run(exercise(free_port()))


@pytest.mark.parametrize("stalled", [{"lease"}, {"sqs"}, {"lease", "sqs"}])
def test_readyz_bounds_stalled_aws_checks(stalled):
    class Table:
        def get_item(self, **kwargs):
            if "lease" in stalled:
                time.sleep(5)
            return {}

    class Queue:
        def get_queue_attributes(self, **kwargs):
            if "sqs" in stalled:
                time.sleep(5)
            return {}

    async def exercise():
        lease = Lease(Table(), holder="site-a", auto_acquire=False)
        sqs = SQSDrain(Queue(), "http://127.0.0.1:1/queue", None)
        admin = AdminServer(0, {"lease": lease.ready, "sqs": sqs.ready})
        await admin.start()
        try:
            async with httpx.AsyncClient() as client:
                async with asyncio.timeout(2):
                    response = await client.get(f"http://127.0.0.1:{admin.port}/readyz")
                assert response.status_code == 503
        finally:
            await admin.close()

    asyncio.run(exercise())


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.parametrize(
    "name",
    [
        "INSTANCE_ID",
        "COMMAND_TIMEOUT_SECONDS",
        "DISPATCH_POLL_SECONDS",
        "COMPLIANCE_TOLERANCE",
    ],
)
def test_dispatcher_rejects_invalid_env(monkeypatch, name):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("PLANT_GATEWAY_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("LEASE_MODE", "disabled")
    monkeypatch.setenv(name, "" if name == "INSTANCE_ID" else "0")
    monkeypatch.delenv("DATABASE_URL", raising=False)

    async def exercise():
        with pytest.raises(ConfigError, match=name):
            await serve(asyncio.Event(), CommonConfig.from_env())

    asyncio.run(exercise())


@pytest.mark.parametrize("dependency", ["postgres", "sqs"])
def test_dispatch_cycle_retries_after_one_second(dsn, monkeypatch, dependency):
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("DATABASE_URL", dsn)
    monkeypatch.setenv("ADMIN_PORT", str(free_port()))
    monkeypatch.setenv("PLANT_GATEWAY_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("LEASE_MODE", "disabled")
    monkeypatch.setenv("DISPATCH_POLL_SECONDS", "60")

    async def exercise():
        stop = asyncio.Event()
        attempts = 0
        delays = []
        loop = asyncio.get_running_loop()

        def record_attempt():
            nonlocal attempts
            attempts += 1
            if attempts == 3:
                stop.set()

        async def wait_for_cycle(stop_waiter, *, timeout):
            # Verify the scheduled backoff without counting DB or scheduler latency.
            delays.append(timeout)
            stop_waiter.close()
            await asyncio.sleep(0)

        monkeypatch.setattr(main, "asyncio", SimpleNamespace(
            Event=asyncio.Event,
            TaskGroup=asyncio.TaskGroup,
            timeout=asyncio.timeout,
            wait_for=wait_for_cycle,
        ))

        async def unavailable(self):
            record_attempt()
            raise psycopg.OperationalError("database unavailable")

        class Queue:
            def receive_message(self, **kwargs):
                loop.call_soon_threadsafe(record_attempt)
                raise EndpointConnectionError(endpoint_url="http://127.0.0.1:1")

        if dependency == "postgres":
            monkeypatch.setattr(Leader, "try_acquire", unavailable)
        else:
            monkeypatch.setenv("SQS_QUEUE_URL", "http://127.0.0.1:1/queue")
            monkeypatch.setattr(main.boto3, "client", lambda *args, **kwargs: Queue())
        async with asyncio.timeout(10):
            await serve(stop, CommonConfig.from_env())
        assert attempts == 3
        assert delays == [1, 1, 1]

    asyncio.run(exercise())


def test_dispatch_sends_and_records_ack(dsn):
    async def exercise():
        async with AsyncConnectionPool(dsn, open=False) as pool:
            instruction_id = await insert_instruction(pool)
            leader = Leader(pool, holder="a")
            generation = await leader.try_acquire()
            seen = []

            async def handle(command):
                seen.append(command)
                return 200

            try:
                async with plant_gateway(handle) as url, httpx.AsyncClient() as client:
                    dispatcher = Dispatcher(client, url, leader, Lease.disabled())
                    assert await dispatcher.dispatch_one()
                commands, status = await statuses(pool, instruction_id)
                assert [(str(key), result) for key, result in commands] == [
                    (seen[0]["command_id"], "acked"),
                ]
                assert status == "dispatched"
                assert seen[0] | {"valid_until": None} == {
                    "command_id": seen[0]["command_id"],
                    "instruction_id": instruction_id,
                    "asset_id": "solar-1",
                    "reduction_kw": "25.000",
                    "generation": generation,
                    "epoch": 0,
                    "valid_until": None,
                }
            finally:
                await leader.release()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("response", "expected"), [(None, "timeout"), (409, "stale_epoch"), (500, "error")]
)
def test_dispatch_terminal_statuses(dsn, response, expected):
    async def exercise():
        async with AsyncConnectionPool(dsn, open=False) as pool:
            instruction_id = await insert_instruction(pool)
            leader = Leader(pool, holder="a")
            await leader.try_acquire()
            seen = []

            async def handle(command):
                seen.append(command)
                if response is None:
                    await asyncio.sleep(11)
                return response or 200

            try:
                async with plant_gateway(handle) as url, httpx.AsyncClient() as client:
                    dispatcher = Dispatcher(
                        client, url, leader, Lease.disabled(), command_timeout=0.5
                    )
                    assert await dispatcher.dispatch_one()
                    assert not await dispatcher.dispatch_one()
                commands, status = await statuses(pool, instruction_id)
                assert [result for _, result in commands] == [expected]
                assert status == "partially_dispatched"
                assert len(seen) == 1
            finally:
                await leader.release()

    asyncio.run(exercise())


def test_no_duplicate_commands_on_leader_switch(dsn):
    async def exercise():
        async with AsyncConnectionPool(dsn, open=False) as pool:
            instruction_id = await insert_instruction(pool)
            first, second = Leader(pool, holder="a"), Leader(pool, holder="b")
            await first.try_acquire()
            seen = []

            async def handle(command):
                seen.append(command)
                if len(seen) == 1:
                    await first.release()
                    await second.try_acquire()
                return 200

            try:
                async with plant_gateway(handle) as url, httpx.AsyncClient() as client:
                    assert await Dispatcher(
                        client, url, first, Lease.disabled()
                    ).dispatch_one()
                    assert await Dispatcher(
                        client, url, second, Lease.disabled()
                    ).dispatch_one()
                assert len(seen) == 2
                assert seen[0]["command_id"] == seen[1]["command_id"]
                assert seen[0]["generation"] + 1 == seen[1]["generation"]
                commands, status = await statuses(pool, instruction_id)
                assert len(commands) == 1
                assert commands[0][1] == "acked"
                assert status == "dispatched"
            finally:
                await first.release()
                await second.release()

    asyncio.run(exercise())


def test_lease_lost_stops_sending(dsn):
    async def exercise():
        async with AsyncConnectionPool(dsn, open=False) as pool:
            instruction_id = await insert_instruction(pool)
            leader = Leader(pool, holder="a")
            await leader.try_acquire()

            async def unexpected(request):
                pytest.fail("An inactive lease must prevent command sends")

            try:
                async with (
                    plant_gateway(unexpected) as url,
                    httpx.AsyncClient() as client,
                ):
                    assert not await Dispatcher(
                        client, url, leader, Lease(object(), holder="site-a", auto_acquire=False)
                    ).dispatch_one()
                assert await statuses(pool, instruction_id) == ([], "accepted")
            finally:
                await leader.release()

    asyncio.run(exercise())


def test_expire_preserves_terminal_statuses(dsn):
    async def exercise():
        async with AsyncConnectionPool(dsn, open=False) as pool:
            expected = {}
            for status in (
                "accepted", "dispatching", "dispatched", "partially_dispatched", "expired"
            ):
                instruction_id = await insert_instruction(pool)
                async with pool.connection() as conn:
                    await conn.execute(
                        """UPDATE jammy.instructions
                           SET status = %s, valid_until = now() - interval '1 minute'
                           WHERE instruction_id = %s""",
                        (status, instruction_id),
                    )
                expected[instruction_id] = (
                    "expired" if status in {"accepted", "dispatching"} else status
                )
            expected[await insert_instruction(pool)] = "accepted"
            leader = Leader(pool)
            await leader.try_acquire()
            try:
                await Dispatcher(None, "", leader, Lease.disabled()).expire()
            finally:
                await leader.release()
            for instruction_id, status in expected.items():
                assert (await statuses(pool, instruction_id))[1] == status

    asyncio.run(exercise())


def test_allocation_ignores_stale_state(dsn):
    async def exercise():
        async with AsyncConnectionPool(dsn, open=False) as pool:
            instruction_id = await insert_instruction(pool)
            async with pool.connection() as conn:
                await conn.execute(
                    "UPDATE jammy.latest_state SET event_time = now() - interval '121 seconds'"
                )
            leader = Leader(pool, holder="a")
            await leader.try_acquire()

            async def unexpected(request):
                pytest.fail("Stale measurements must contribute zero")

            try:
                async with (
                    plant_gateway(unexpected) as url,
                    httpx.AsyncClient() as client,
                ):
                    assert await Dispatcher(
                        client, url, leader, Lease.disabled()
                    ).dispatch_one()
                assert await statuses(pool, instruction_id) == (
                    [],
                    "partially_dispatched",
                )
            finally:
                await leader.release()

    asyncio.run(exercise())
