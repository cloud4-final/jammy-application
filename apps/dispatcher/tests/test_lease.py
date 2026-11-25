import asyncio
import time
from threading import Event
from uuid import uuid4

import boto3
import httpx
import pytest
import test_dispatch
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionError, HTTPClientError
from dispatcher.lease import Lease, LeaseState
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer

postgres = test_dispatch.postgres
dsn = test_dispatch.dsn


@pytest.fixture(scope="module")
def dynamodb():
    with DockerContainer("amazon/dynamodb-local:3.3.2").with_exposed_ports(
        8000
    ) as container:
        host, port = container.get_container_host_ip(), container.get_exposed_port(8000)
        dynamodb = boto3.resource(
            "dynamodb",
            endpoint_url=f"http://{host}:{port}",
            region_name="ap-northeast-2",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(
                connect_timeout=2, read_timeout=30, retries={"total_max_attempts": 1}
            ),
        )
        deadline = time.monotonic() + 120
        while True:
            try:
                dynamodb.meta.client.list_tables()
                break
            except ConnectionError, HTTPClientError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        yield dynamodb


@pytest.fixture
def table(dynamodb):
    return dynamodb.create_table(
        TableName=f"lease-{uuid4()}",
        KeySchema=[{"AttributeName": "lease_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "lease_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


def test_lease_disabled_is_active_epoch_zero(monkeypatch):
    monkeypatch.setenv("LEASE_MODE", "disabled")
    assert asyncio.run(Lease.from_env().tick()) == LeaseState(True, 0)


@pytest.mark.parametrize("legacy", [False, True])
def test_lease_env_controls_aws_item_and_holder(monkeypatch, legacy):
    key = {"lease_name": "active-site"} if legacy else {"lease_id": "db-primary"}
    holder = "shared-holder" if legacy else "site-a"
    for name in ("LEASE_KEY_NAME", "LEASE_KEY_VALUE", "LEASE_HOLDER"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(f"{name}_FILE", raising=False)
    if legacy:
        name, value = next(iter(key.items()))
        monkeypatch.setenv("LEASE_KEY_NAME", name)
        monkeypatch.setenv("LEASE_KEY_VALUE", value)
        monkeypatch.setenv("LEASE_HOLDER", holder)
    monkeypatch.setenv("SITE_ID", "site-a")
    monkeypatch.setenv("LEASE_MODE", "dynamodb")
    monkeypatch.setenv("LEASE_AUTO_ACQUIRE", "true")
    monkeypatch.setenv("LEASE_TABLE", "aws-owned")

    class Table:
        def Table(self, name):
            assert name == "aws-owned"
            return self

        def get_item(self, **kwargs):
            assert kwargs == {"Key": key, "ConsistentRead": True}
            return {}

        def update_item(self, **kwargs):
            assert kwargs["Key"] == key
            assert kwargs["ExpressionAttributeNames"] == {"#key": next(iter(key))}
            assert kwargs["ExpressionAttributeValues"][":holder"] == holder
            return {"Attributes": {"epoch": 1}}

    monkeypatch.setattr(boto3, "resource", lambda *args, **kwargs: Table())
    assert asyncio.run(Lease.from_env().tick()) == LeaseState(True, 1)


@pytest.mark.parametrize(("renew_seconds", "ready"), [(5, True), (0, False)])
def test_readiness_uses_fresh_observation_during_tick(renew_seconds, ready):
    async def exercise():
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        finished = Event()

        class Table:
            stall = False

            def get_item(self, **kwargs):
                if self.stall:
                    loop.call_soon_threadsafe(started.set)
                    finished.wait(5)
                return {}

        table = Table()
        lease = Lease(
            table, holder="site-a", auto_acquire=False, renew_seconds=renew_seconds
        )
        assert await lease.tick() == LeaseState(False, 0)
        table.stall = True
        tick = asyncio.create_task(lease.tick())
        try:
            async with asyncio.timeout(1):
                await started.wait()
                assert await lease.ready() is ready
        finally:
            finished.set()
            await tick

    asyncio.run(exercise())


def test_lease_renew_and_lose(table):
    async def exercise():
        lease = Lease(table, holder="site-a", auto_acquire=True, renew_seconds=0)
        assert await lease.tick() == LeaseState(True, 1)
        original = table.get_item(Key={"lease_id": "db-primary"})["Item"]
        await asyncio.sleep(1.05)
        assert await lease.tick() == LeaseState(True, 1)
        renewed = table.get_item(Key={"lease_id": "db-primary"})["Item"]
        assert renewed["expires_at"] > original["expires_at"]
        table.put_item(Item=renewed | {"holder": "site-b", "epoch": 2})
        assert not (await lease.tick()).active

    asyncio.run(exercise())


def test_lease_no_auto_acquire_on_site_b(table):
    async def exercise():
        lease = Lease(table, holder="site-b", auto_acquire=False)
        first = {"lease_id": "db-primary", "holder": "site-a", "epoch": 4}
        table.put_item(Item=first | {"expires_at": int(time.time()) + 30})
        with pytest.raises(ClientError, match="ConditionalCheckFailed"):
            await lease.takeover(confirm=True)
        table.put_item(Item=first | {"expires_at": int(time.time()) - 1})
        assert not (await lease.tick()).active
        assert await lease.takeover(confirm=True) == 5
        assert await lease.tick() == LeaseState(True, 5)

    asyncio.run(exercise())


def test_same_site_leader_switch_keeps_lease(dsn, table):
    async def read_lease():
        reply = await asyncio.to_thread(
            table.get_item, Key={"lease_id": "db-primary"}, ConsistentRead=True
        )
        return reply.get("Item")

    async def exercise():
        commands = []
        settings = {
            "LEASE_MODE": "dynamodb",
            "LEASE_TABLE": table.name,
            "LEASE_TTL_SECONDS": "60",
            "LEASE_RENEW_SECONDS": "20",
            "LEASE_FENCE_SECONDS": "45",
            "DYNAMODB_ENDPOINT": table.meta.client.meta.endpoint_url,
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_DEFAULT_REGION": "ap-northeast-2",
        }

        async def acknowledge(command):
            commands.append(command)
            return 200

        async with (
            AsyncConnectionPool(dsn, open=False) as pool,
            test_dispatch.plant_gateway(acknowledge) as gateway,
            httpx.AsyncClient() as client,
        ):
            first_id = await test_dispatch.insert_instruction(pool)
            async with test_dispatch.dispatcher_process(
                dsn, gateway, test_dispatch.free_port(),
                LEASE_AUTO_ACQUIRE="true",
                **settings,
            ) as first:
                async with asyncio.timeout(15):
                    while (await test_dispatch.statuses(pool, first_id))[1] != "dispatched":
                        await asyncio.sleep(0.05)
                    original = await read_lease()
                    assert original["holder"] == "site-a"
                    second_port = test_dispatch.free_port()
                    while int(time.time()) + 60 <= original["expires_at"]:
                        await asyncio.sleep(0.05)

                async with test_dispatch.dispatcher_process(
                    dsn, gateway, second_port,
                    INSTANCE_ID="dispatcher-a2",
                    LEASE_AUTO_ACQUIRE="false",
                    **settings,
                ):
                    async with asyncio.timeout(10):
                        while True:
                            try:
                                response = await client.get(
                                    f"http://127.0.0.1:{second_port}/readyz"
                                )
                                if response.status_code == 200:
                                    break
                            except httpx.ConnectError:
                                pass
                            await asyncio.sleep(0.05)
                    assert await read_lease() == original

                    async with asyncio.timeout(20):
                        first.kill()
                        await first.wait()
                        second_id = await test_dispatch.insert_instruction(pool)
                        while (await test_dispatch.statuses(pool, second_id))[1] != "dispatched":
                            await asyncio.sleep(0.05)

                    renewed = await read_lease()
                    assert renewed["holder"] == "site-a"
                    assert renewed["epoch"] == original["epoch"]
                    assert renewed["expires_at"] > original["expires_at"]
                    before, after = commands
                    assert before["instruction_id"] == first_id
                    assert after["instruction_id"] == second_id
                    assert after["generation"] == before["generation"] + 1
                    assert after["epoch"] == before["epoch"] == original["epoch"]

    asyncio.run(exercise())
