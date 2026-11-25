import asyncio
import json
import time
from contextlib import asynccontextmanager

import boto3
import pytest
import test_dispatch
from botocore.config import Config
from botocore.exceptions import ConnectionError, HTTPClientError
from dispatcher.lease import LeaseState
from dispatcher.sqs_drain import SQSDrain
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer

postgres = test_dispatch.postgres


@pytest.fixture(scope="module", params=[False, True], ids=["standard", "fifo"])
def queue(request):
    container = DockerContainer("softwaremill/elasticmq-native:1.7.1")
    with container.with_exposed_ports(9324):
        host, port = container.get_container_host_ip(), container.get_exposed_port(9324)
        endpoint = f"http://{host}:{port}"
        client = boto3.client(
            "sqs",
            endpoint_url=endpoint,
            region_name="ap-northeast-2",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(
                connect_timeout=2, read_timeout=2, retries={"total_max_attempts": 1}
            ),
        )
        deadline = time.monotonic() + 120
        while True:
            try:
                client.list_queues()
                break
            except ConnectionError, HTTPClientError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        if request.param:
            url = client.create_queue(
                QueueName="instructions.fifo", Attributes={"FifoQueue": "true"}
            )["QueueUrl"]
        else:
            url = client.create_queue(QueueName="instructions")["QueueUrl"]
        yield client, url


def test_sqs_drain_inserts_idempotently_and_deletes(postgres, queue):
    client, url = queue
    is_fifo = url.endswith(".fifo")
    payload = {
        "instruction_id": "queued-fifo" if is_fifo else "queued-standard",
        "valid_from": "2026-11-23T01:00:00Z",
        "valid_until": "2026-11-23T02:00:00Z",
        "target_reduction_kw": "25.000",
    }
    body = json.dumps(payload)
    conflicting = json.dumps(payload | {"target_reduction_kw": "1.000"})
    for index, message in enumerate((body, body, conflicting, "{}")):
        fifo = (
            {"MessageGroupId": "instruction", "MessageDeduplicationId": str(index)}
            if is_fifo
            else {}
        )
        client.send_message(QueueUrl=url, MessageBody=message, **fifo)

    class Counter:
        def __init__(self):
            self.results = []

        def add(self, value, attributes):
            assert value == 1
            self.results.append(attributes["result"])

    async def exercise():
        async with AsyncConnectionPool(postgres, open=False) as pool:
            counter = Counter()
            drain = SQSDrain(client, url, counter)
            async with pool.connection() as conn:
                await conn.set_autocommit(True)
                @asynccontextmanager
                async def transaction():
                    async with conn.transaction():
                        yield conn

                async def active():
                    return LeaseState(True, 0)

                await drain.drain_once(transaction, active)
                rows = await conn.execute(
                    "SELECT body,source,status FROM jammy.instructions WHERE instruction_id=%s",
                    (payload["instruction_id"],),
                )
                assert await rows.fetchall() == [(body.encode(), "sqs", "expired")]
            assert sorted(counter.results) == [
                "conflict",
                "duplicate",
                "inserted",
                "invalid",
            ]

    asyncio.run(exercise())
    attributes = client.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])[
        "Attributes"
    ]
    assert int(attributes["ApproximateNumberOfMessages"]) == 0
    assert int(attributes["ApproximateNumberOfMessagesNotVisible"]) == 2
